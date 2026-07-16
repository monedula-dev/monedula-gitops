// KafkaRoleBinding shape + identity + immutability admission webhook (spec §40).
// This validating webhook mirrors the KafkaQuota validator: it enforces, at
// admission time (before the object is persisted), invariants the reconciler can
// only surface terminally after the fact:
//
//   - Shape (§40): the per-resource field rules (valid principal form, non-empty
//     role, known scope.type, resource well-formedness). Reuses
//     validation.ValidateRoleBindingShape so the CLI lint and the webhook agree.
//   - Identity uniqueness (§40): at most one live KafkaRoleBinding CR may resolve
//     to a given set of (cluster, principal, role, scope, resource) MDS binding
//     identities. Without this, two CRs claiming the same MDS binding flap
//     last-writer-wins. Identity is checked via rbac.Compile + RoleBinding.FullKey.
//   - Immutable fields (update only): clusterRef.name, principal, role, and
//     scope.type may not change (changing any of these orphans the previous MDS
//     bindings). spec.resources changes ARE allowed.
//
// The validator reads only from the manager's cache (a client.Reader), so it
// never blocks admission on a live apiserver round-trip beyond the cache.
//
// # TOCTOU caveat
//
// Like the KafkaTopic, KafkaQuota, and KafkaAccessPolicy validators, the
// uniqueness check reads from the manager CACHE, not the live apiserver. Two
// near-simultaneous Creates can therefore both pass the uniqueness check (each
// sees a stale cache that does not yet contain the other's object). Admission
// shrinks the duplicate-identity window significantly but is NOT a linearizable
// guarantee. The reconciler's behaviour remains the backstop for the rare
// duplicate that slips through during a cache-lag window.
package webhook

import (
	"context"
	"errors"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/rbac"
	"github.com/monedula-dev/monedula-gitops/internal/validation"
)

// +kubebuilder:webhook:path=/validate-gitops-monedula-dev-v1alpha1-kafkarolebinding,mutating=false,failurePolicy=fail,sideEffects=None,groups=gitops.monedula.dev,resources=kafkarolebindings,verbs=create;update,versions=v1alpha1,name=vkafkarolebinding.gitops.monedula.dev,admissionReviewVersions=v1

// KafkaRoleBindingValidator is the admission.Validator[*v1alpha1.KafkaRoleBinding]
// for v1alpha1.KafkaRoleBinding. It reads from the manager cache (Reader) and
// resolves the referenced cluster per the operator's cluster-resolution convention
// (ClusterNamespace override, else the role binding's own namespace) — sharing the
// same convention as the KafkaTopic, KafkaQuota, and KafkaAccessPolicy validators
// so admission and reconcile agree on the identity scope.
type KafkaRoleBindingValidator struct {
	// Reader is the manager's cached client. It must have the
	// RoleBindingClusterRefNameIndex field index registered (RegisterIndexes does
	// this; the index is declared in setup.go).
	Reader client.Reader
	// ClusterNamespace is where KafkaCluster CRs are resolved from. Empty means
	// each role binding's own namespace (clusterRef is namespace-local). This MUST
	// match the value passed to the reconcilers so admission and reconcile agree on
	// the identity scope.
	ClusterNamespace string
}

// Compile-time assertion that the validator satisfies the generic Validator
// interface for *v1alpha1.KafkaRoleBinding.
var _ admission.Validator[*v1alpha1.KafkaRoleBinding] = &KafkaRoleBindingValidator{}

// clusterNamespaceFor returns the namespace the role binding's clusterRef resolves
// in, applying the ClusterNamespace override exactly as the reconciler does.
func (v *KafkaRoleBindingValidator) clusterNamespaceFor(rb *v1alpha1.KafkaRoleBinding) string {
	if v.ClusterNamespace != "" {
		return v.ClusterNamespace
	}
	return rb.Namespace
}

// ValidateCreate enforces shape and identity uniqueness on create.
func (v *KafkaRoleBindingValidator) ValidateCreate(ctx context.Context, rb *v1alpha1.KafkaRoleBinding) (admission.Warnings, error) {
	if err := checkRoleBindingShape(rb); err != nil {
		return nil, err
	}
	return nil, v.checkIdentityUnique(ctx, rb)
}

// ValidateUpdate enforces shape, field immutability, and identity uniqueness on
// update. Shape is checked first; immutability is checked next (before the
// uniqueness scan) so a key-field change gets the clearest rejection message.
func (v *KafkaRoleBindingValidator) ValidateUpdate(ctx context.Context, oldRB, newRB *v1alpha1.KafkaRoleBinding) (admission.Warnings, error) {
	if err := checkRoleBindingShape(newRB); err != nil {
		return nil, err
	}

	// Immutability: reject changes to fields that orphan the previous MDS
	// bindings (a delete+create of a different MDS role binding set). Resources
	// changes are ALLOWED — only the identity-defining fields are immutable.
	if oldRB.Spec.ClusterRef.Name != newRB.Spec.ClusterRef.Name {
		return nil, fmt.Errorf(
			"KafkaRoleBinding %s/%s: spec.clusterRef.name is immutable: cannot change from %q to %q (changing it orphans the previous MDS bindings)",
			newRB.Namespace, newRB.Name, oldRB.Spec.ClusterRef.Name, newRB.Spec.ClusterRef.Name)
	}
	if oldRB.Spec.Principal != newRB.Spec.Principal {
		return nil, fmt.Errorf(
			"KafkaRoleBinding %s/%s: spec.principal is immutable: cannot change from %q to %q (changing it orphans the previous MDS bindings)",
			newRB.Namespace, newRB.Name, oldRB.Spec.Principal, newRB.Spec.Principal)
	}
	if oldRB.Spec.Role != newRB.Spec.Role {
		return nil, fmt.Errorf(
			"KafkaRoleBinding %s/%s: spec.role is immutable: cannot change from %q to %q (changing it orphans the previous MDS bindings)",
			newRB.Namespace, newRB.Name, oldRB.Spec.Role, newRB.Spec.Role)
	}
	if oldRB.Spec.Scope.Type != newRB.Spec.Scope.Type {
		return nil, fmt.Errorf(
			"KafkaRoleBinding %s/%s: spec.scope.type is immutable: cannot change from %q to %q (changing it orphans the previous MDS bindings)",
			newRB.Namespace, newRB.Name, oldRB.Spec.Scope.Type, newRB.Spec.Scope.Type)
	}

	return nil, v.checkIdentityUnique(ctx, newRB)
}

// ValidateDelete always allows: removing a CR can never violate the identity or
// shape invariants.
func (v *KafkaRoleBindingValidator) ValidateDelete(_ context.Context, _ *v1alpha1.KafkaRoleBinding) (admission.Warnings, error) {
	return nil, nil
}

// checkRoleBindingShape runs the standalone shape checks (reused from internal/
// validation) and aggregates any failures into a single rejection error.
//
// Objects received at admission may have an empty TypeMeta (apiVersion/kind are
// stripped by the apiserver machinery in some paths). ValidateRoleBindingShape
// checks apiVersion, so we fill it on a shallow copy before calling — mirroring
// how the KafkaAccessPolicy webhook handles this.
func checkRoleBindingShape(rb *v1alpha1.KafkaRoleBinding) error {
	check := *rb
	if check.APIVersion == "" {
		check.APIVersion = v1alpha1.APIVersion
	}
	shapeErrs := validation.ValidateRoleBindingShape(&check)
	if len(shapeErrs) == 0 {
		return nil
	}
	msgs := make([]string, 0, len(shapeErrs))
	for _, e := range shapeErrs {
		msgs = append(msgs, e.Error())
	}
	return errors.New(strings.Join(msgs, "; "))
}

// checkIdentityUnique rejects when ANOTHER live CR's compiled MDS bindings
// overlap (by FullKey) with the incoming role binding's compiled MDS bindings AND
// resolve to the same effective cluster namespace.
//
// NOTE — TOCTOU: this check reads from the manager CACHE. Two near-simultaneous
// Creates can both pass (cache lag). Admission shrinks the duplicate-identity
// window but is not a linearizable guarantee; the reconciler remains the
// backstop for duplicates that slip through.
//
// # Cluster-not-found / MDS-not-configured → allow
//
// If the referenced KafkaCluster does not exist, or it has no MDS configuration
// (spec.authorization.mds == nil), the incoming role binding may be arriving
// before its cluster CR (a common GitOps push pattern). We allow and defer to
// the reconciler, which will surface the missing cluster/MDS as a condition.
// This mirrors the KafkaQuota and KafkaAccessPolicy webhooks' cluster-not-found
// → allow behaviour.
//
// # Identity scope (with vs without --cluster-namespace)
//
// MDS role binding identity is (cluster, principal, role, scope, resource). The
// CONTESTED cluster is resolved per the operator convention (identical to the
// other validators):
//
//   - When ClusterNamespace is UNSET, clusterRef is namespace-local.
//   - When ClusterNamespace is SET, all namespaces share the one KafkaCluster
//     object in that namespace.
//
// A candidate with the same namespace/name is the object itself (self-update)
// and is skipped. A candidate with a non-zero DeletionTimestamp is STILL
// considered to occupy the identity: its finalizer may still be running
// cluster-side cleanup, so re-claiming the identity before it is fully gone is
// the user's race to lose. We reject in that case rather than allow an early
// duplicate (mirrors the KafkaTopic/KafkaQuota/KafkaUser validators).
func (v *KafkaRoleBindingValidator) checkIdentityUnique(ctx context.Context, rb *v1alpha1.KafkaRoleBinding) error {
	wantClusterRef := rb.Spec.ClusterRef.Name
	wantClusterNS := v.clusterNamespaceFor(rb)

	// Resolve the referenced KafkaCluster to get its MDS configuration.
	// Cluster-not-found → allow; defer to reconcile.
	var cluster v1alpha1.KafkaCluster
	if err := v.Reader.Get(ctx, types.NamespacedName{Namespace: wantClusterNS, Name: wantClusterRef}, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return nil // cluster not yet present: allow; reconcile surfaces it
		}
		// Unresolvable cluster (read error): allow; defer to reconcile.
		return nil
	}

	// MDS-not-configured → allow; defer to reconcile.
	if cluster.Spec.Authorization == nil || cluster.Spec.Authorization.MDS == nil {
		return nil
	}
	mds := cluster.Spec.Authorization.MDS

	// Compile the incoming role binding's MDS bindings.
	incomingBindings, err := rbac.Compile(rb, mds)
	if err != nil {
		// Compile error (e.g. missing cluster IDs for scope type): allow; defer to reconcile.
		return nil
	}

	// Build a set of FullKeys for the incoming bindings.
	incomingKeys := make(map[string]struct{}, len(incomingBindings))
	for _, b := range incomingBindings {
		incomingKeys[b.FullKey()] = struct{}{}
	}

	// List all KafkaRoleBindings referencing the same cluster ref name.
	var list v1alpha1.KafkaRoleBindingList
	if err := v.Reader.List(ctx, &list,
		client.MatchingFields{RoleBindingClusterRefNameIndex: wantClusterRef},
	); err != nil {
		return fmt.Errorf("listing KafkaRoleBindings for identity check: %w", err)
	}

	for i := range list.Items {
		other := &list.Items[i]

		// Skip the object itself by namespace+name. On create the incoming object
		// has no UID yet; on update it is the same object being modified. Either
		// way the namespace+name match is correct and more robust than UID matching.
		if other.Namespace == rb.Namespace && other.Name == rb.Name {
			continue
		}

		// Same clusterRef.name is guaranteed by the field-index filter; require
		// the same EFFECTIVE cluster namespace so the scope matches the operator
		// convention (namespace-local without the override; cluster-wide with it).
		if v.clusterNamespaceFor(other) != wantClusterNS {
			continue
		}

		// Compile the other role binding against the same MDS config.
		otherBindings, err := rbac.Compile(other, mds)
		if err != nil {
			continue // compile error: skip this candidate (allow; defer to reconcile)
		}

		// Check for any FullKey overlap between the incoming and other bindings.
		for _, ob := range otherBindings {
			if _, ok := incomingKeys[ob.FullKey()]; ok {
				return fmt.Errorf(
					"KafkaRoleBinding %s/%s conflicts with %s/%s: both resolve to MDS binding identity %q on cluster %q (namespace %q); role binding identity (cluster, principal, role, scope, resource) must be unique (spec §40)",
					rb.Namespace, rb.Name, other.Namespace, other.Name,
					ob.FullKey(), wantClusterRef, wantClusterNS)
			}
		}
	}
	return nil
}
