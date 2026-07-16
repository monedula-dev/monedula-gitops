// KafkaUser identity + shape admission webhook (spec v0.35 §T2, mirroring the
// KafkaQuota and KafkaTopic validators). This validating webhook enforces, at
// admission time (before the object is persisted), invariants the reconciler
// can only surface terminally after the fact:
//
//   - Shape (v0.35 §T2): password one-of (valueFrom XOR generate), rejecting
//     inline/configMapKeyRef password sources, mechanism/iterations bounds,
//     and username charset. Reuses validation.ValidateUserShape so the CLI
//     lint and the webhook agree.
//   - Identity uniqueness: at most one live KafkaUser CR may resolve to a
//     given (cluster, username) identity. Without this, two CRs claiming the
//     same principal flap last-writer-wins on the underlying credential.
//   - Username immutability: the resolved username of an existing CR may not
//     change on update (a rename is a delete + create of a different Kafka
//     principal). The CEL rule on KafkaUserSpec already enforces "immutable
//     once set" but cannot compare against metadata.name from spec scope, so
//     an unset -> set transition to a value OTHER than metadata.name slips
//     past CEL; this webhook adds the richer resolved-name comparison
//     (mirroring the KafkaTopic validator's ResolvedTopicName comparison) so
//     that case is also rejected, with an old -> new message CEL cannot
//     produce.
//   - ClusterRef immutability: spec.clusterRef.name of an existing CR may not
//     change on update (repointing a user orphans the credential on the
//     previous cluster). Also enforced always-on by the CEL rule on
//     KafkaUserSpec; the webhook check keeps webhook-on messages consistent.
//
// Unlike the KafkaTopic validator, there is no tenancy check here: tenancy is
// topic-webhook-only by design (v0.35 scope).
//
// The validator reads only from the manager's cache (a client.Reader), so it
// never blocks admission on a live apiserver round-trip beyond the cache.
//
// # TOCTOU caveat
//
// Like the KafkaQuota/KafkaTopic validators, the uniqueness check reads from
// the manager CACHE, not the live apiserver. Two near-simultaneous Creates can
// therefore both pass the uniqueness check (each sees a stale cache that does
// not yet contain the other's object). Admission shrinks the
// duplicate-identity window significantly but is NOT a linearizable
// guarantee. The reconciler's behaviour remains the backstop for the rare
// duplicate that slips through during a cache-lag window.
package webhook

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/validation"
)

// +kubebuilder:webhook:path=/validate-gitops-monedula-dev-v1alpha1-kafkauser,mutating=false,failurePolicy=fail,sideEffects=None,groups=gitops.monedula.dev,resources=kafkausers,verbs=create;update,versions=v1alpha1,name=vkafkauser.gitops.monedula.dev,admissionReviewVersions=v1

// KafkaUserValidator is the admission.Validator[*v1alpha1.KafkaUser] for
// v1alpha1.KafkaUser. It reads from the manager cache (Reader) and resolves
// the referenced cluster per the operator's cluster-resolution convention
// (ClusterNamespace override, else the user's own namespace) — sharing the
// same convention as the KafkaTopic/KafkaQuota validators and reconcilers so
// admission and reconcile agree on the identity scope.
type KafkaUserValidator struct {
	// Reader is the manager's cached client. It must have the
	// UserClusterRefNameIndex field index registered (RegisterIndexes does
	// this).
	Reader client.Reader
	// ClusterNamespace is where KafkaCluster CRs are resolved from. Empty
	// means each user's own namespace (clusterRef is namespace-local). This
	// MUST match the value passed to the reconcilers so admission and
	// reconcile agree on the identity scope.
	ClusterNamespace string
}

// Compile-time assertion that the validator satisfies the generic Validator
// interface for *v1alpha1.KafkaUser.
var _ admission.Validator[*v1alpha1.KafkaUser] = &KafkaUserValidator{}

// ResolvedUsername returns the effective Kafka principal name: spec.username
// when set, else metadata.name (the defaulting-package default). Defensive
// against a nil-ish object. Exported because the controller-side
// duplicate-identity gate (the webhook-off backstop) must resolve identity
// EXACTLY as this validator does.
func ResolvedUsername(u *v1alpha1.KafkaUser) string {
	if u.Spec.Username != "" {
		return u.Spec.Username
	}
	return u.Name
}

// clusterNamespaceFor returns the namespace the user's clusterRef resolves
// in, applying the ClusterNamespace override exactly as the reconciler does.
func (v *KafkaUserValidator) clusterNamespaceFor(u *v1alpha1.KafkaUser) string {
	if v.ClusterNamespace != "" {
		return v.ClusterNamespace
	}
	return u.Namespace
}

// ValidateCreate enforces shape and identity uniqueness on create.
func (v *KafkaUserValidator) ValidateCreate(ctx context.Context, u *v1alpha1.KafkaUser) (admission.Warnings, error) {
	if err := checkUserShape(u); err != nil {
		return nil, err
	}
	return nil, v.checkIdentityUnique(ctx, u)
}

// ValidateUpdate enforces shape, clusterRef immutability, username
// immutability, and identity uniqueness on update. Shape is checked first;
// immutability checks are next, before the (now-stale-identity) uniqueness
// scan, so an identity change gets the clearest rejection.
func (v *KafkaUserValidator) ValidateUpdate(ctx context.Context, oldUser, newUser *v1alpha1.KafkaUser) (admission.Warnings, error) {
	if err := checkUserShape(newUser); err != nil {
		return nil, err
	}

	// clusterRef immutability: repointing a user to a different cluster
	// orphans the credential on the previous cluster. Mirrors the CEL rule on
	// KafkaUserSpec so webhook-on and webhook-off installs agree.
	if oldUser.Spec.ClusterRef.Name != newUser.Spec.ClusterRef.Name {
		return nil, fmt.Errorf(
			"KafkaUser %s/%s: spec.clusterRef.name is immutable: cannot change from %q to %q (repointing a user orphans the credential on the previous cluster)",
			newUser.Namespace, newUser.Name, oldUser.Spec.ClusterRef.Name, newUser.Spec.ClusterRef.Name)
	}

	// Immutability: compare RESOLVED usernames so adding an explicit username
	// equal to metadata.name (empty -> explicit-same) is NOT treated as a
	// change. The CEL rule on KafkaUserSpec already rejects any change to a
	// non-empty username, but cannot compare against metadata.name from spec
	// scope, so an unset -> set transition to a DIFFERENT value than
	// metadata.name slips past CEL. This resolved-name comparison catches
	// that case too, with a clearer old -> new message.
	oldName := ResolvedUsername(oldUser)
	newName := ResolvedUsername(newUser)
	if oldName != newName {
		return nil, fmt.Errorf(
			"KafkaUser %s/%s: spec.username is immutable: resolved username cannot change from %q to %q (a rename is a delete + create of a different Kafka principal)",
			newUser.Namespace, newUser.Name, oldName, newName)
	}

	return nil, v.checkIdentityUnique(ctx, newUser)
}

// ValidateDelete always allows: removing a CR can never violate the identity
// or shape invariants. There is no tenancy check to skip here (tenancy is
// topic-webhook-only by design).
func (v *KafkaUserValidator) ValidateDelete(_ context.Context, _ *v1alpha1.KafkaUser) (admission.Warnings, error) {
	return nil, nil
}

// checkUserShape runs the standalone shape checks (reused from internal/
// validation) and aggregates any failures into a single rejection error.
//
// Objects received at admission may have an empty TypeMeta (apiVersion/kind
// are stripped by the apiserver machinery in some paths). ValidateUserShape
// checks apiVersion, so we fill it on a shallow copy before calling —
// mirroring how the KafkaAccessPolicy/KafkaRoleBinding webhooks handle this.
func checkUserShape(u *v1alpha1.KafkaUser) error {
	check := *u
	if check.APIVersion == "" {
		check.APIVersion = v1alpha1.APIVersion
	}
	shapeErrs := validation.ValidateUserShape(&check)
	if len(shapeErrs) == 0 {
		return nil
	}
	msgs := make([]string, 0, len(shapeErrs))
	for _, e := range shapeErrs {
		msgs = append(msgs, e.Error())
	}
	return errors.New(strings.Join(msgs, "; "))
}

// checkIdentityUnique rejects when ANOTHER live CR resolves to the same
// identity AND the same RESOLVED cluster as u.
//
// NOTE — TOCTOU: this check reads from the manager CACHE. Two
// near-simultaneous Creates can both pass (cache lag). Admission shrinks the
// duplicate-identity window but is not a linearizable guarantee; the
// reconciler remains the backstop for duplicates that slip through.
//
// # Identity scope (with vs without --cluster-namespace)
//
// Kafka user identity is (cluster, username). The CONTESTED cluster is a
// concrete KafkaCluster object, resolved per the operator convention
// (identical to the KafkaTopic/KafkaQuota validators):
//
//   - When ClusterNamespace is UNSET, clusterRef is namespace-local: a user
//     in namespace "a" referencing clusterRef "prod" points at KafkaCluster
//     prod-in-a, while a user in namespace "b" with the same clusterRef
//     points at a DIFFERENT object prod-in-b. So two users in different
//     namespaces sharing (clusterRef, username) do NOT collide — the
//     effective identity scope is (namespace, clusterRef, username).
//   - When ClusterNamespace is SET, all namespaces share the one KafkaCluster
//     object in that namespace. So (clusterRef, username) collisions are
//     cluster-wide across every namespace — scope is (clusterRef, username).
//
// We implement exactly that by requiring a candidate to match on
// clusterRef.name AND on the EFFECTIVE cluster namespace
// (clusterNamespaceFor): with the override every user yields the same
// cluster namespace (cluster-wide); without it the cluster namespace equals
// the user's own namespace (namespace-scoped).
//
// A candidate with the same UID is the object itself (self-update) and is
// skipped. A candidate with a non-zero DeletionTimestamp is STILL considered
// to occupy the identity: its finalizer may still be running cluster-side
// cleanup, so re-claiming the identity before it is fully gone is the user's
// race to lose. We reject in that case rather than allow an early duplicate
// (mirrors the KafkaTopic/KafkaQuota validators).
func (v *KafkaUserValidator) checkIdentityUnique(ctx context.Context, u *v1alpha1.KafkaUser) error {
	wantName := ResolvedUsername(u)
	wantClusterRef := u.Spec.ClusterRef.Name
	wantClusterNS := v.clusterNamespaceFor(u)

	var list v1alpha1.KafkaUserList
	if err := v.Reader.List(ctx, &list,
		client.MatchingFields{UserClusterRefNameIndex: wantClusterRef},
	); err != nil {
		return fmt.Errorf("listing KafkaUsers for identity check: %w", err)
	}

	for i := range list.Items {
		other := &list.Items[i]
		if other.UID == u.UID {
			continue // the object itself (self-update); not a conflict
		}
		if ResolvedUsername(other) != wantName {
			continue
		}
		// Same clusterRef.name is guaranteed by the field-index filter; require
		// the same EFFECTIVE cluster namespace so the scope matches the operator
		// convention (namespace-local without the override; cluster-wide with it).
		if v.clusterNamespaceFor(other) != wantClusterNS {
			continue
		}
		return fmt.Errorf(
			"KafkaUser %s/%s conflicts with %s/%s: both resolve to username %q on cluster %q (namespace %q); user identity (cluster, username) must be unique",
			u.Namespace, u.Name, other.Namespace, other.Name,
			wantName, wantClusterRef, wantClusterNS)
	}
	return nil
}
