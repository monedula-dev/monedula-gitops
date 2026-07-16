package controller

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/monedula-dev/monedula-gitops/internal/access"
	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/operator/reconcile"
)

// buildClusterACLView aggregates the desired ACL set + managed scope across
// EVERY KafkaTopic and KafkaAccessPolicy referencing the same cluster as the
// resource being reconciled (spec §20.1), so the reconcile core computes PRUNE
// candidates against the cluster-wide union instead of the single resource's
// own desired set — the fix for the §10.4 overlapping-owners flap.
//
// Cluster identity filter: a resource belongs to the view when its
// spec.clusterRef.name equals clusterRefName AND it resolves to the same
// KafkaCluster object. Resolution mirrors the controllers' own lookup:
//
//   - with a --cluster-namespace override (namespaceLocal=false) every
//     clusterRef resolves into that one namespace, so a matching name is
//     sufficient regardless of the resource's namespace;
//   - without an override (namespaceLocal=true) clusterRef is namespace-local,
//     so only resources in clusterNamespace (the reconciled resource's own
//     namespace) can resolve to the same KafkaCluster.
//
// Known approximation: two KafkaCluster objects in different namespaces may
// point at the same physical brokers; they are still treated as DIFFERENT
// clusters here (aggregation follows the KafkaCluster object, not bootstrap
// servers), matching how every other part of the operator scopes state.
//
// Resources being deleted (non-zero DeletionTimestamp) are skipped: their ACLs
// are the finalizers' responsibility, and keeping them in the union would let
// a half-deleted resource indefinitely protect (or veto pruning of) tuples it
// is about to release.
//
// Reads go through the same cache as the controllers' Get, so the view is
// consistent with the object being reconciled. List items are deep-copied
// before defaulting mutates them, keeping the shared cache pristine. A List
// failure is returned as-is — the callers treat it as transient (requeue).
//
// Deliberate fail-safe: tenancy-denied CRs still contribute. This view does
// NOT filter out KafkaTopics/KafkaAccessPolicies whose own reconcile is
// terminally rejected by tenancy (Phase Error, TenancyDenied) — the tenancy
// check runs later, per-CR, deeper in the reconcile core, and only against
// the single resource being reconciled. Filtering denied CRs out of this
// cluster-wide view could over-delete: if tenancy flips transiently (e.g. a
// KafkaCluster's tenancy config is mid-edit, or a namespace briefly drops off
// an allow-list), a denied CR's ACLs would stop being protected and a
// co-owner's prune could delete tuples that are still live on the broker and
// still desired once tenancy flips back. Retention — letting a denied CR keep
// protecting its tuples from prune — is the safe direction; the denied CR
// itself never mutates live state (its own reconcile is terminal), so this
// only ever prevents deletion, never causes it.
func buildClusterACLView(ctx context.Context, c client.Client, clusterRefName,
	clusterNamespace string, namespaceLocal bool, clusterDefaults *v1alpha1.ClusterDefaults,
	cluster *v1alpha1.KafkaCluster) (*reconcile.ClusterACLView, error) {

	var listOpts []client.ListOption
	if namespaceLocal {
		listOpts = append(listOpts, client.InNamespace(clusterNamespace))
	}

	var topicList v1alpha1.KafkaTopicList
	if err := c.List(ctx, &topicList, listOpts...); err != nil {
		return nil, fmt.Errorf("listing KafkaTopics for cluster ACL view: %w", err)
	}
	var policyList v1alpha1.KafkaAccessPolicyList
	if err := c.List(ctx, &policyList, listOpts...); err != nil {
		return nil, fmt.Errorf("listing KafkaAccessPolicies for cluster ACL view: %w", err)
	}

	var topics []*v1alpha1.KafkaTopic
	for i := range topicList.Items {
		tp := &topicList.Items[i]
		if tp.Spec.ClusterRef.Name != clusterRefName || !tp.DeletionTimestamp.IsZero() {
			continue
		}
		topics = append(topics, tp.DeepCopy())
	}
	var policies []*v1alpha1.KafkaAccessPolicy
	for i := range policyList.Items {
		pol := &policyList.Items[i]
		if pol.Spec.ClusterRef.Name != clusterRefName || !pol.DeletionTimestamp.IsZero() {
			continue
		}
		policies = append(policies, pol.DeepCopy())
	}

	return reconcile.BuildClusterACLView(topics, policies, clusterDefaults, cluster), nil
}

// shieldACLDeletion is the finalizer-path co-ownership shield shared by the
// KafkaTopic and KafkaAccessPolicy Delete paths (spec §10.4's delete-path
// analogue): it builds the cluster-wide desired ACL union across the REMAINING
// live CRs (buildClusterACLView skips resources with a non-zero
// DeletionTimestamp, which excludes the deleting CR — and any other CR mid-
// deletion — by construction) and subtracts any tuple another live KafkaTopic /
// KafkaAccessPolicy still desires from toDelete, so deleting one co-owner
// cannot revoke access a surviving co-owner depends on. When tuples are
// retained, event (never nil) is invoked once with the SharedACLsRetained
// reason and a co-owner summary.
//
// If the view cannot be built (a List failure), the returned error makes the
// deletion attempt FAIL so it is retried with the finalizer retained — never
// fall back to deleting the full set on error, since that could over-delete a
// co-owned tuple. An empty toDelete short-circuits without listing.
func shieldACLDeletion(ctx context.Context, c client.Client, clusterRefName,
	clusterNamespace string, namespaceLocal bool, cluster *v1alpha1.KafkaCluster,
	toDelete []access.ACL, event func(reason, msg string)) ([]access.ACL, error) {

	if len(toDelete) == 0 {
		return toDelete, nil
	}
	view, verr := buildClusterACLView(ctx, c, clusterRefName, clusterNamespace,
		namespaceLocal, cluster.Spec.Defaults, cluster)
	if verr != nil {
		return nil, fmt.Errorf("building cluster ACL view for deletion (retry; not deleting ACLs blindly): %w", verr)
	}
	remaining := subtractProtectedACLs(toDelete, view.DesiredACLs)
	if shared := len(toDelete) - len(remaining); shared > 0 {
		retained := retainedACLs(toDelete, view.DesiredACLs)
		event("SharedACLsRetained",
			fmt.Sprintf("%d ACL(s) still desired by other live resources were retained on the cluster (%s)",
				shared, aclCoOwnerSummary(retained, coOwnerNamesLimit)))
	}
	return remaining, nil
}

// subtractProtectedACLs returns the entries of toDelete whose identity
// (access.ACL.FullKey — the canonical 7-field tuple) is NOT present in protect.
// It is the co-ownership shield for the finalizer Delete path: toDelete is the
// deleting CR's own compiled desired set, protect is the cluster-wide desired
// union across the REMAINING live CRs (buildClusterACLView skips resources with
// a non-zero DeletionTimestamp, so the deleting CR — and any other CR mid-
// deletion — never contributes to protect). A tuple still desired by another
// live CR is kept on the broker; only the remainder is deleted, so removing one
// co-owner cannot revoke access a surviving co-owner depends on (the delete-
// path analogue of the §10.4 prune-path aggregation).
//
// Pure function; order of the surviving entries is preserved.
func subtractProtectedACLs(toDelete, protect []access.ACL) []access.ACL {
	if len(toDelete) == 0 || len(protect) == 0 {
		return toDelete
	}
	protected := make(map[string]struct{}, len(protect))
	for _, a := range protect {
		protected[a.FullKey()] = struct{}{}
	}
	var out []access.ACL
	for _, a := range toDelete {
		if _, ok := protected[a.FullKey()]; !ok {
			out = append(out, a)
		}
	}
	return out
}

// retainedACLs returns the entries of protect whose identity is ALSO present
// in toDelete — the tuples actually retained on the broker by the
// co-ownership shield (the complement of subtractProtectedACLs, restricted to
// the intersection). It deliberately returns the PROTECT-side entries, not the
// toDelete-side ones: toDelete is the deleting CR's own compile and is never
// Source-stamped (it doesn't need to be — deletion doesn't consult it), while
// protect is the cluster-wide view built by BuildClusterACLView, which DOES
// stamp Source* per contributor. Only protect's copies carry the co-owner
// attribution the SharedACLsRetained event names.
//
// Pure function; order of the retained entries (as found in protect) is
// preserved.
func retainedACLs(toDelete, protect []access.ACL) []access.ACL {
	if len(toDelete) == 0 || len(protect) == 0 {
		return nil
	}
	wanted := make(map[string]struct{}, len(toDelete))
	for _, a := range toDelete {
		wanted[a.FullKey()] = struct{}{}
	}
	var out []access.ACL
	for _, a := range protect {
		if _, ok := wanted[a.FullKey()]; ok {
			out = append(out, a)
		}
	}
	return out
}
