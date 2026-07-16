// Package webhook implements the KafkaTopic identity admission webhook (spec
// §20.3, Part IX decision 10). It is a validating webhook that enforces, at
// admission time (before the object is persisted), three invariants the
// reconciler can only surface terminally after the fact:
//
//   - Identity uniqueness (§5.3): at most one live KafkaTopic CR may resolve to
//     a given (cluster, topicName) identity. The webhook rejects the duplicate
//     at admission; without it the reconciler's duplicate-identity gate catches
//     the duplicate eventually-consistently (older wins, newer goes terminal).
//   - topicName immutability: the resolved topic name of an existing CR may not
//     change on update (a rename is a delete+create of a different Kafka topic).
//   - clusterRef immutability: repointing a topic to a different cluster orphans
//     state on the previous cluster. Also enforced always-on by the CEL rule on
//     KafkaTopicSpec; the webhook check keeps webhook-on messages consistent.
//   - Tenancy (§20.2): the topic's namespace and name must satisfy the
//     referenced cluster's tenancy policy. The reconciler enforces this
//     terminally; the webhook rejects at admission for fast feedback. Tenancy
//     is never checked on an update where the object is being deleted
//     (DeletionTimestamp set), so tightening tenancy after a topic exists can
//     never make it undeletable.
//
// The validator reads only from the manager's cache (a client.Reader), so it
// never blocks admission on a live apiserver round-trip beyond the cache.
//
// # TOCTOU caveat
//
// All reads in the uniqueness and tenancy checks go through the manager CACHE,
// not the live apiserver. Two near-simultaneous Creates can therefore both pass
// the uniqueness check (each sees a stale cache that does not yet contain the
// other's object). Admission shrinks the duplicate-identity window significantly
// but is NOT a linearizable guarantee. The reconciler's duplicate-identity gate
// (older CR wins; the newer goes terminal with DuplicateIdentity and never
// touches the broker) remains the backstop for the rare duplicate that slips
// through during a cache-lag window.
package webhook

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/operator/index"
	"github.com/monedula-dev/monedula-gitops/internal/tenancy"
)

// ClusterRefNameIndex is the manager cache field-index key on a KafkaTopic's
// spec.clusterRef.name. The validator lists candidate duplicates filtered by
// this index so it scans only topics referencing the same cluster ref, not
// every topic. Registered unconditionally in manager.New (see RegisterIndexes).
// Re-exported from internal/operator/index, the neutral home of the shared
// field indexes.
const ClusterRefNameIndex = index.ClusterRefNameIndex

// +kubebuilder:webhook:path=/validate-gitops-monedula-dev-v1alpha1-kafkatopic,mutating=false,failurePolicy=fail,sideEffects=None,groups=gitops.monedula.dev,resources=kafkatopics,verbs=create;update,versions=v1alpha1,name=vkafkatopic.gitops.monedula.dev,admissionReviewVersions=v1

// KafkaTopicValidator is the admission.Validator[*v1alpha1.KafkaTopic] for
// v1alpha1.KafkaTopic. It reads from the manager cache (Reader) and resolves
// the referenced cluster per the operator's cluster-resolution convention
// (ClusterNamespace override, else the topic's own namespace) — mirroring
// KafkaTopicReconciler exactly.
type KafkaTopicValidator struct {
	// Reader is the manager's cached client. It must have the
	// ClusterRefNameIndex field index registered (RegisterIndexes does this).
	Reader client.Reader
	// ClusterNamespace is where KafkaCluster CRs are resolved from. Empty means
	// each topic's own namespace (clusterRef is namespace-local). This MUST match
	// the value passed to the reconcilers so admission and reconcile agree on the
	// identity scope.
	ClusterNamespace string
}

// Compile-time assertion that the validator satisfies the generic Validator
// interface for *v1alpha1.KafkaTopic.
var _ admission.Validator[*v1alpha1.KafkaTopic] = &KafkaTopicValidator{}

// ResolvedTopicName returns the effective Kafka topic name: spec.topicName when
// set, else metadata.name (the §4 default). It delegates to the API type's
// KafkaTopic.ResolvedTopicName — THE single resolution of topic identity — and
// stays exported because the controller-side duplicate-identity gate (the
// webhook-off backstop) resolves identity through it.
func ResolvedTopicName(t *v1alpha1.KafkaTopic) string {
	return t.ResolvedTopicName()
}

// clusterNamespaceFor returns the namespace the topic's clusterRef resolves in,
// applying the ClusterNamespace override exactly as the reconciler does.
func (v *KafkaTopicValidator) clusterNamespaceFor(t *v1alpha1.KafkaTopic) string {
	if v.ClusterNamespace != "" {
		return v.ClusterNamespace
	}
	return t.Namespace
}

// ValidateCreate enforces identity uniqueness and tenancy on create.
func (v *KafkaTopicValidator) ValidateCreate(ctx context.Context, topic *v1alpha1.KafkaTopic) (admission.Warnings, error) {
	if err := v.checkIdentityUnique(ctx, topic); err != nil {
		return nil, err
	}
	return nil, v.checkTenancy(ctx, topic)
}

// ValidateUpdate enforces topicName + clusterRef immutability, identity
// uniqueness, and tenancy on update. Immutability is checked first so a rename
// is rejected with the clearest message before the (now-stale-identity)
// uniqueness scan.
//
// # Deletion wedge (tenancy)
//
// The tenancy check is SKIPPED when newTopic has a non-zero DeletionTimestamp.
// Tenancy policy can be tightened after a topic already exists (a namespace
// removed from allowedNamespaces, a prefix rule added), and once that happens
// a still-live topic would fail its own tenancy check forever — including on
// the controller's finalizer-removal Update and on a user's allow-delete
// annotation patch, both of which are updates to an object mid-deletion. That
// would make the topic permanently undeletable until tenancy is relaxed again.
// Deletion must always be able to proceed once requested, so tenancy is not
// re-evaluated once DeletionTimestamp is set.
//
// Immutability and identity-uniqueness stay ACTIVE during deletion: they are
// cheap (no cluster-side dependency that can drift out from under an existing
// object) and cannot wedge a deletion — finalizer removal and the delete
// annotation patch don't touch spec.topicName or spec.clusterRef, and identity
// uniqueness only rejects a genuine duplicate, not a policy change.
func (v *KafkaTopicValidator) ValidateUpdate(ctx context.Context, oldTopic, newTopic *v1alpha1.KafkaTopic) (admission.Warnings, error) {
	// Immutability: compare RESOLVED names so adding an explicit topicName equal
	// to metadata.name (empty -> explicit-same) is NOT treated as a change.
	oldName := ResolvedTopicName(oldTopic)
	newName := ResolvedTopicName(newTopic)
	if oldName != newName {
		return nil, fmt.Errorf(
			"KafkaTopic %s/%s: spec.topicName is immutable: resolved topic name cannot change from %q to %q (a rename is a delete + create of a different Kafka topic)",
			newTopic.Namespace, newTopic.Name, oldName, newName)
	}

	// clusterRef immutability: repointing a topic to a different cluster orphans
	// the topic (and its ACLs/schema state) on the previous cluster. This mirrors
	// the KafkaRoleBinding webhook and the CEL rule on KafkaTopicSpec, so the
	// webhook-on and webhook-off installs agree.
	if oldTopic.Spec.ClusterRef.Name != newTopic.Spec.ClusterRef.Name {
		return nil, fmt.Errorf(
			"KafkaTopic %s/%s: spec.clusterRef.name is immutable: cannot change from %q to %q (repointing a topic orphans state on the previous cluster)",
			newTopic.Namespace, newTopic.Name, oldTopic.Spec.ClusterRef.Name, newTopic.Spec.ClusterRef.Name)
	}

	if err := v.checkIdentityUnique(ctx, newTopic); err != nil {
		return nil, err
	}

	if !newTopic.DeletionTimestamp.IsZero() {
		// Deletion in progress: never let a tenancy change wedge it. See the
		// "Deletion wedge (tenancy)" note on this method's doc comment.
		return nil, nil
	}
	return nil, v.checkTenancy(ctx, newTopic)
}

// ValidateDelete always allows: removing a CR can never violate the identity or
// tenancy invariants.
func (v *KafkaTopicValidator) ValidateDelete(_ context.Context, _ *v1alpha1.KafkaTopic) (admission.Warnings, error) {
	return nil, nil
}

// checkIdentityUnique rejects when ANOTHER live CR resolves to the same
// identity AND the same RESOLVED cluster as topic.
//
// NOTE — TOCTOU: this check reads from the manager CACHE. Two near-simultaneous
// Creates can both pass (cache lag). Admission shrinks the duplicate-identity
// window but is not a linearizable guarantee; the reconciler remains the
// backstop for duplicates that slip through.
//
// # Identity scope (with vs without --cluster-namespace)
//
// Kafka identity is (cluster, topicName). The CONTESTED cluster is a concrete
// KafkaCluster object, resolved per the operator convention:
//
//   - When ClusterNamespace is UNSET, clusterRef is namespace-local: a topic in
//     namespace "a" referencing clusterRef "prod" points at KafkaCluster
//     prod-in-a, while a topic in namespace "b" with the same clusterRef points
//     at a DIFFERENT object prod-in-b. So two topics in different namespaces
//     sharing (clusterRef, topicName) do NOT collide — the effective identity
//     scope is (namespace, clusterRef, topicName).
//   - When ClusterNamespace is SET, all namespaces share the one KafkaCluster
//     object in that namespace. So (clusterRef, topicName) collisions are
//     cluster-wide across every namespace — scope is (clusterRef, topicName).
//
// We implement exactly that by requiring a candidate to match on clusterRef.name
// AND on the EFFECTIVE cluster namespace (clusterNamespaceFor): with the
// override every topic yields the same cluster namespace (cluster-wide); without
// it the cluster namespace equals the topic's own namespace (namespace-scoped).
//
// A candidate with the same UID is the object itself (self-update) and is
// skipped. A candidate with a non-zero DeletionTimestamp is STILL considered to
// occupy the identity: its finalizer may still be running cluster-side cleanup,
// so re-claiming the identity before it is fully gone is the user's race to
// lose. We reject in that case rather than allow an early duplicate.
func (v *KafkaTopicValidator) checkIdentityUnique(ctx context.Context, topic *v1alpha1.KafkaTopic) error {
	wantName := ResolvedTopicName(topic)
	wantClusterRef := topic.Spec.ClusterRef.Name
	wantClusterNS := v.clusterNamespaceFor(topic)

	var list v1alpha1.KafkaTopicList
	if err := v.Reader.List(ctx, &list,
		client.MatchingFields{ClusterRefNameIndex: wantClusterRef},
	); err != nil {
		return fmt.Errorf("listing KafkaTopics for identity check: %w", err)
	}

	for i := range list.Items {
		other := &list.Items[i]
		if other.UID == topic.UID {
			continue // the object itself (self-update); not a conflict
		}
		if ResolvedTopicName(other) != wantName {
			continue
		}
		// Same clusterRef.name is guaranteed by the field-index filter; require
		// the same EFFECTIVE cluster namespace so the scope matches the operator
		// convention (namespace-local without the override; cluster-wide with it).
		if v.clusterNamespaceFor(other) != wantClusterNS {
			continue
		}
		return fmt.Errorf(
			"KafkaTopic %s/%s conflicts with %s/%s: both resolve to topicName %q on cluster %q (namespace %q); topic identity (cluster, topicName) must be unique (spec §5.3)",
			topic.Namespace, topic.Name, other.Namespace, other.Name,
			wantName, wantClusterRef, wantClusterNS)
	}
	return nil
}

// checkTenancy resolves the referenced KafkaCluster and runs tenancy.Check
// against the topic's namespace and resolved name.
//
// A MISSING cluster ALLOWS the operation: admission must not block on a lagging
// KafkaCluster object (the cluster CR may be applied moments after its topics in
// the same GitOps push). The reconciler surfaces the unknown-cluster condition
// terminally, so nothing is lost by deferring to it here.
func (v *KafkaTopicValidator) checkTenancy(ctx context.Context, topic *v1alpha1.KafkaTopic) error {
	var cluster v1alpha1.KafkaCluster
	key := types.NamespacedName{Namespace: v.clusterNamespaceFor(topic), Name: topic.Spec.ClusterRef.Name}
	if err := v.Reader.Get(ctx, key, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return nil // cluster not yet present: allow; reconcile surfaces it
		}
		return fmt.Errorf("resolving clusterRef for tenancy check: %w", err)
	}
	return tenancy.Check(cluster.Spec.Tenancy, topic.Namespace, ResolvedTopicName(topic))
}
