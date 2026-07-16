package controller

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/operator/reconcile"
	"github.com/monedula-dev/monedula-gitops/internal/rbac"
)

// buildClusterRoleBindingView aggregates the desired MDS role-binding scope
// across EVERY KafkaRoleBinding referencing the same cluster as the resource
// being reconciled (spec §40), so the reconcile core computes PRUNE candidates
// against the cluster-wide union instead of the single resource's own desired
// set — the same anti-flap fix as buildClusterACLView for ACLs.
//
// Cluster identity filter: a resource belongs to the view when its
// spec.clusterRef.name equals clusterRefName AND it resolves to the same
// KafkaCluster object. Resolution mirrors the controller's own lookup:
//
//   - with a --cluster-namespace override (namespaceLocal=false) every
//     clusterRef resolves into that one namespace, so a matching name is
//     sufficient regardless of the resource's namespace;
//   - without an override (namespaceLocal=true) clusterRef is namespace-local,
//     so only resources in clusterNS (the reconciled resource's own namespace)
//     can resolve to the same KafkaCluster.
//
// Resources being deleted (non-zero DeletionTimestamp) are skipped.
//
// KafkaRoleBindings that fail to compile (missing cluster IDs, invalid scope)
// are SKIPPED rather than aborting the view: they will surface their own
// ValidationFailed condition at their own reconcile. Accumulating only the
// compilable bindings gives the broadest valid prune scope.
//
// When cluster has accessBackends including "rbac", KafkaTopics are also
// listed and their topic-access-derived bindings (via rbac.CompileTopicAccess)
// are aggregated. Compile errors and warnings on topics are skipped here —
// they surface on the topic's own reconcile.
//
// A List failure (either list) is returned as-is — the caller treats it as
// transient (requeue). The list intentionally does NOT filter by field index on
// the server side so the bare envtest client (no manager cache / no registered
// index) also works. The clusterRef check is performed in code after listing,
// mirroring buildClusterACLView.
//
// Deliberate fail-safe: tenancy-denied CRs still contribute. Like
// buildClusterACLView, this view does not exclude KafkaRoleBindings (or
// topic-access-derived bindings) whose own reconcile is terminally rejected by
// tenancy (TenancyDenied) — that check runs later, per-CR. Filtering denied
// CRs out here could over-delete when tenancy flips transiently: a denied
// CR's bindings would stop being protected and a co-owner's prune could
// remove role bindings that are still live and still desired once tenancy
// flips back. Retention is the safe direction.
func buildClusterRoleBindingView(ctx context.Context, c client.Client,
	clusterRefName, clusterNS string, namespaceLocal bool,
	mds *v1alpha1.MDSConfig, cluster *v1alpha1.KafkaCluster) (*reconcile.ClusterRoleBindingView, error) {

	var listOpts []client.ListOption
	if namespaceLocal {
		listOpts = append(listOpts, client.InNamespace(clusterNS))
	}

	var rbList v1alpha1.KafkaRoleBindingList
	if err := c.List(ctx, &rbList, listOpts...); err != nil {
		return nil, fmt.Errorf("listing KafkaRoleBindings for cluster role-binding view: %w", err)
	}

	var allBindings []rbac.RoleBinding
	for i := range rbList.Items {
		rb := &rbList.Items[i]
		if rb.Spec.ClusterRef.Name != clusterRefName {
			continue // different cluster
		}
		if rb.DeletionTimestamp != nil && !rb.DeletionTimestamp.IsZero() {
			continue // being deleted; its bindings are the finalizer's responsibility
		}
		if mds == nil {
			continue // no MDS config: nothing to compile
		}
		compiled, err := rbac.Compile(rb, mds)
		if err != nil {
			// Skip: compile error (missing cluster IDs) will surface in that
			// resource's own reconcile as a ValidationFailed condition.
			continue
		}
		rbac.StampPrune(compiled, rb.Spec.Prune)
		allBindings = append(allBindings, compiled...)
	}

	// Aggregate topic-access-derived bindings when the cluster uses RBAC.
	if mds != nil && v1alpha1.HasAccessBackend(cluster, "rbac") {
		var topicList v1alpha1.KafkaTopicList
		if err := c.List(ctx, &topicList, listOpts...); err != nil {
			return nil, fmt.Errorf("listing KafkaTopics for cluster role-binding view: %w", err)
		}
		for i := range topicList.Items {
			tp := &topicList.Items[i]
			if tp.Spec.ClusterRef.Name != clusterRefName {
				continue // different cluster
			}
			if tp.DeletionTimestamp != nil && !tp.DeletionTimestamp.IsZero() {
				continue // being deleted
			}
			compiled, _, err := rbac.CompileTopicAccess(tp, mds)
			if err != nil {
				// Skip: compile error will surface in the topic's own reconcile.
				continue
			}
			rbac.StampPrune(compiled, tp.Spec.Prune)
			allBindings = append(allBindings, compiled...)
		}
	}

	desired, _ := rbac.BuildDesiredSet(allBindings) // ignore collision errors — surface on offending resource's own reconcile
	return &reconcile.ClusterRoleBindingView{
		DesiredBindings: desired,
		DesiredScope:    rbac.BuildScope(desired),
	}, nil
}

// subtractProtectedRoleBindings returns the entries of toDelete whose identity
// (rbac.RoleBinding.FullKey — principal, role, scope, resource) is NOT present
// in protect. It is the role-binding analogue of subtractProtectedACLs: on the
// finalizer Delete path toDelete is the deleting CR's own compiled binding set
// and protect is the cluster-wide desired union across the REMAINING live CRs
// (buildClusterRoleBindingView skips resources with a non-zero
// DeletionTimestamp, so neither the deleting CR nor any other CR mid-deletion
// contributes to protect — a co-owner that is itself going away must not keep
// a tuple alive). A binding still desired by a live KafkaRoleBinding or a live
// topic's access block is kept in MDS; only the remainder is removed.
//
// Pure function; order of the surviving entries is preserved.
func subtractProtectedRoleBindings(toDelete, protect []rbac.RoleBinding) []rbac.RoleBinding {
	if len(toDelete) == 0 || len(protect) == 0 {
		return toDelete
	}
	protected := make(map[string]struct{}, len(protect))
	for _, b := range protect {
		protected[b.FullKey()] = struct{}{}
	}
	var out []rbac.RoleBinding
	for _, b := range toDelete {
		if _, ok := protected[b.FullKey()]; !ok {
			out = append(out, b)
		}
	}
	return out
}

// retainedRoleBindings returns the entries of protect whose identity is ALSO
// present in toDelete — the bindings actually retained in MDS by the
// co-ownership shield (the complement of subtractProtectedRoleBindings,
// restricted to the intersection). It is the role-binding analogue of
// retainedACLs: it deliberately returns the PROTECT-side entries so the
// SharedRoleBindingsRetained event names the SURVIVING co-owner, not the
// deleting CR itself — toDelete's own Source* (rbac.Compile stamps it with the
// deleting CR's own identity) would name the wrong resource.
//
// Pure function; order of the retained entries (as found in protect) is
// preserved.
func retainedRoleBindings(toDelete, protect []rbac.RoleBinding) []rbac.RoleBinding {
	if len(toDelete) == 0 || len(protect) == 0 {
		return nil
	}
	wanted := make(map[string]struct{}, len(toDelete))
	for _, b := range toDelete {
		wanted[b.FullKey()] = struct{}{}
	}
	var out []rbac.RoleBinding
	for _, b := range protect {
		if _, ok := wanted[b.FullKey()]; ok {
			out = append(out, b)
		}
	}
	return out
}
