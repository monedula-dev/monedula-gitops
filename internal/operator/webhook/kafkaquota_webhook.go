// KafkaQuota identity + shape admission webhook (spec §39.5). This validating
// webhook mirrors the KafkaTopic validator: it enforces, at admission time
// (before the object is persisted), invariants the reconciler can only surface
// terminally after the fact:
//
//   - Shape (§39.5): the per-resource entity/limits rules (at least one entity
//     component, mutually-exclusive user/userDefault and clientId/clientIdDefault,
//     "User:" form, at least one non-negative limit). Reuses
//     validation.ValidateQuotaShape so the CLI lint and the webhook agree.
//   - Identity uniqueness (§39.5): at most one live KafkaQuota CR may resolve to a
//     given (cluster, entity) identity. Without this, two CRs claiming the same
//     entity flap last-writer-wins.
//   - Entity immutability: the resolved entity Key of an existing CR may not
//     change on update (changing it orphans the previous entity's quota — a
//     delete + create of a different Kafka quota entity).
//   - ClusterRef immutability: spec.clusterRef.name of an existing CR may not
//     change on update (repointing a quota orphans the quota on the previous
//     cluster).
//
// The validator reads only from the manager's cache (a client.Reader), so it
// never blocks admission on a live apiserver round-trip beyond the cache.
//
// # TOCTOU caveat
//
// Like the KafkaTopic validator, the uniqueness check reads from the manager
// CACHE, not the live apiserver. Two near-simultaneous Creates can therefore
// both pass the uniqueness check (each sees a stale cache that does not yet
// contain the other's object). Admission shrinks the duplicate-identity window
// significantly but is NOT a linearizable guarantee. The reconciler's behaviour
// remains the backstop for the rare duplicate that slips through during a
// cache-lag window.
package webhook

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/operator/index"
	"github.com/monedula-dev/monedula-gitops/internal/quota"
	"github.com/monedula-dev/monedula-gitops/internal/validation"
)

// QuotaClusterRefNameIndex is the manager cache field-index key on a
// KafkaQuota's spec.clusterRef.name. It mirrors ClusterRefNameIndex for topics:
// the validator lists candidate duplicates filtered by this index so it scans
// only quotas referencing the same cluster ref, not every quota. Re-exported
// from internal/operator/index, the neutral home of the shared field indexes.
const QuotaClusterRefNameIndex = index.QuotaClusterRefNameIndex

// +kubebuilder:webhook:path=/validate-gitops-monedula-dev-v1alpha1-kafkaquota,mutating=false,failurePolicy=fail,sideEffects=None,groups=gitops.monedula.dev,resources=kafkaquotas,verbs=create;update,versions=v1alpha1,name=vkafkaquota.gitops.monedula.dev,admissionReviewVersions=v1

// KafkaQuotaValidator is the admission.Validator[*v1alpha1.KafkaQuota] for
// v1alpha1.KafkaQuota. It reads from the manager cache (Reader) and resolves the
// referenced cluster per the operator's cluster-resolution convention
// (ClusterNamespace override, else the quota's own namespace) — sharing the same
// convention as the KafkaTopic validator and reconcilers so admission and
// reconcile agree on the identity scope.
type KafkaQuotaValidator struct {
	// Reader is the manager's cached client. It must have the
	// QuotaClusterRefNameIndex field index registered (RegisterIndexes does this).
	Reader client.Reader
	// ClusterNamespace is where KafkaCluster CRs are resolved from. Empty means
	// each quota's own namespace (clusterRef is namespace-local). This MUST match
	// the value passed to the reconcilers so admission and reconcile agree on the
	// identity scope.
	ClusterNamespace string
}

// Compile-time assertion that the validator satisfies the generic Validator
// interface for *v1alpha1.KafkaQuota.
var _ admission.Validator[*v1alpha1.KafkaQuota] = &KafkaQuotaValidator{}

// ResolvedEntityKey returns the resolved entity identity Key of a quota
// (§39.2). Exported because the controller-side duplicate-identity gate (the
// webhook-off backstop) must resolve identity EXACTLY as this validator does.
func ResolvedEntityKey(q *v1alpha1.KafkaQuota) string {
	return quota.Compile(q).Entity.Key()
}

// clusterNamespaceFor returns the namespace the quota's clusterRef resolves in,
// applying the ClusterNamespace override exactly as the reconciler does.
func (v *KafkaQuotaValidator) clusterNamespaceFor(q *v1alpha1.KafkaQuota) string {
	if v.ClusterNamespace != "" {
		return v.ClusterNamespace
	}
	return q.Namespace
}

// ValidateCreate enforces shape and identity uniqueness on create.
func (v *KafkaQuotaValidator) ValidateCreate(ctx context.Context, q *v1alpha1.KafkaQuota) (admission.Warnings, error) {
	if err := checkQuotaShape(q); err != nil {
		return nil, err
	}
	return nil, v.checkIdentityUnique(ctx, q)
}

// ValidateUpdate enforces shape, clusterRef immutability, entity immutability,
// and identity uniqueness on update. Shape is checked first; immutability
// checks are next, before the (now-stale-identity) uniqueness scan, so an
// identity change gets the clearest rejection.
func (v *KafkaQuotaValidator) ValidateUpdate(ctx context.Context, oldQuota, newQuota *v1alpha1.KafkaQuota) (admission.Warnings, error) {
	if err := checkQuotaShape(newQuota); err != nil {
		return nil, err
	}

	// clusterRef immutability: repointing a quota to a different cluster
	// orphans the quota applied on the previous one. Mirrors the CEL rule on
	// KafkaQuotaSpec so webhook-on and webhook-off installs agree.
	if oldQuota.Spec.ClusterRef.Name != newQuota.Spec.ClusterRef.Name {
		return nil, fmt.Errorf(
			"KafkaQuota %s/%s: spec.clusterRef.name is immutable: cannot change from %q to %q (repointing a quota orphans the previous cluster's quota)",
			newQuota.Namespace, newQuota.Name, oldQuota.Spec.ClusterRef.Name, newQuota.Spec.ClusterRef.Name)
	}

	// Immutability: compare RESOLVED entity keys so a no-op re-spelling of the
	// same entity is not treated as a change. A change orphans the previous
	// entity's quota (a delete + create of a different Kafka quota entity).
	oldKey := ResolvedEntityKey(oldQuota)
	newKey := ResolvedEntityKey(newQuota)
	if oldKey != newKey {
		return nil, fmt.Errorf(
			"KafkaQuota %s/%s: spec.entity is immutable: resolved entity cannot change from %q to %q (changing it orphans the previous entity's quota)",
			newQuota.Namespace, newQuota.Name, oldKey, newKey)
	}

	return nil, v.checkIdentityUnique(ctx, newQuota)
}

// ValidateDelete always allows: removing a CR can never violate the identity or
// shape invariants.
func (v *KafkaQuotaValidator) ValidateDelete(_ context.Context, _ *v1alpha1.KafkaQuota) (admission.Warnings, error) {
	return nil, nil
}

// checkQuotaShape runs the standalone shape checks (reused from internal/
// validation) and aggregates any failures into a single rejection error.
func checkQuotaShape(q *v1alpha1.KafkaQuota) error {
	shapeErrs := validation.ValidateQuotaShape(q)
	if len(shapeErrs) == 0 {
		return nil
	}
	msgs := make([]string, 0, len(shapeErrs))
	for _, e := range shapeErrs {
		msgs = append(msgs, e.Error())
	}
	return errors.New(strings.Join(msgs, "; "))
}

// checkIdentityUnique rejects when ANOTHER live CR resolves to the same identity
// AND the same RESOLVED cluster as q.
//
// NOTE — TOCTOU: this check reads from the manager CACHE. Two near-simultaneous
// Creates can both pass (cache lag). Admission shrinks the duplicate-identity
// window but is not a linearizable guarantee; the reconciler remains the
// backstop for duplicates that slip through.
//
// # Identity scope (with vs without --cluster-namespace)
//
// Kafka quota identity is (cluster, entity). The CONTESTED cluster is a concrete
// KafkaCluster object, resolved per the operator convention (identical to the
// KafkaTopic validator):
//
//   - When ClusterNamespace is UNSET, clusterRef is namespace-local: a quota in
//     namespace "a" referencing clusterRef "prod" points at KafkaCluster
//     prod-in-a, while a quota in namespace "b" with the same clusterRef points
//     at a DIFFERENT object prod-in-b. So two quotas in different namespaces
//     sharing (clusterRef, entity) do NOT collide — the effective identity scope
//     is (namespace, clusterRef, entity).
//   - When ClusterNamespace is SET, all namespaces share the one KafkaCluster
//     object in that namespace. So (clusterRef, entity) collisions are
//     cluster-wide across every namespace — scope is (clusterRef, entity).
//
// We implement exactly that by requiring a candidate to match on clusterRef.name
// AND on the EFFECTIVE cluster namespace (clusterNamespaceFor): with the
// override every quota yields the same cluster namespace (cluster-wide); without
// it the cluster namespace equals the quota's own namespace (namespace-scoped).
//
// A candidate with the same UID is the object itself (self-update) and is
// skipped. A candidate with a non-zero DeletionTimestamp is STILL considered to
// occupy the identity: its finalizer may still be running cluster-side cleanup,
// so re-claiming the identity before it is fully gone is the user's race to
// lose. We reject in that case rather than allow an early duplicate (mirrors the
// KafkaTopic validator).
func (v *KafkaQuotaValidator) checkIdentityUnique(ctx context.Context, q *v1alpha1.KafkaQuota) error {
	wantKey := ResolvedEntityKey(q)
	wantClusterRef := q.Spec.ClusterRef.Name
	wantClusterNS := v.clusterNamespaceFor(q)

	var list v1alpha1.KafkaQuotaList
	if err := v.Reader.List(ctx, &list,
		client.MatchingFields{QuotaClusterRefNameIndex: wantClusterRef},
	); err != nil {
		return fmt.Errorf("listing KafkaQuotas for identity check: %w", err)
	}

	for i := range list.Items {
		other := &list.Items[i]
		if other.UID == q.UID {
			continue // the object itself (self-update); not a conflict
		}
		if ResolvedEntityKey(other) != wantKey {
			continue
		}
		// Same clusterRef.name is guaranteed by the field-index filter; require
		// the same EFFECTIVE cluster namespace so the scope matches the operator
		// convention (namespace-local without the override; cluster-wide with it).
		if v.clusterNamespaceFor(other) != wantClusterNS {
			continue
		}
		return fmt.Errorf(
			"KafkaQuota %s/%s conflicts with %s/%s: both resolve to entity %q on cluster %q (namespace %q); quota identity (cluster, entity) must be unique (spec §39.5)",
			q.Namespace, q.Name, other.Namespace, other.Name,
			wantKey, wantClusterRef, wantClusterNS)
	}
	return nil
}
