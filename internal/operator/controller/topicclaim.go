package controller

// Cross-kind topic claims. A Kafka topic on a cluster can be claimed by a
// KafkaTopic (its resolved topic name) or by a KafkaMirrorTopic (its effective
// destination topic name). Both kinds share ONE identity: the claim key is
// (clusterRef, topic name) — not kind-qualified — and the same key drives the
// claim scan, the winner order, the per-identity lock (topicIdentityKind) and
// the quorum recheck. Scan semantics follow duplicate.go exactly: in-process
// filtering on spec.clusterRef.name (no field index, so the plain-client
// envtest harness and the APIReader work unchanged), namespace-scoped when
// clusterRef is namespace-local, and deleting CRs never count as claimants.

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

// topicIdentityKind is the lockIdentity kind shared by KafkaTopic and
// KafkaMirrorTopic, so two claimants of one destination topic serialize on the
// same per-identity lock regardless of their CR kind.
const topicIdentityKind = "topic"

const (
	kindKafkaTopic       = "KafkaTopic"
	kindKafkaMirrorTopic = "KafkaMirrorTopic"
)

// topicClaimName returns the claim key of a topic claimant: its
// spec.clusterRef.name and the Kafka topic name it manages (the resolved name
// for a KafkaTopic, the effective destination name for a KafkaMirrorTopic).
// ok is false for any other object.
func topicClaimName(obj client.Object) (clusterRef string, topic string, ok bool) {
	switch o := obj.(type) {
	case *v1alpha1.KafkaTopic:
		return o.Spec.ClusterRef.Name, o.ResolvedTopicName(), true
	case *v1alpha1.KafkaMirrorTopic:
		return o.Spec.ClusterRef.Name, o.Spec.EffectiveTopicName(), true
	}
	return "", "", false
}

// topicClaimKind returns the CR kind of a topic claimant. It is derived from
// the Go type because typed List items carry an empty TypeMeta.
func topicClaimKind(obj client.Object) string {
	switch obj.(type) {
	case *v1alpha1.KafkaTopic:
		return kindKafkaTopic
	case *v1alpha1.KafkaMirrorTopic:
		return kindKafkaMirrorTopic
	}
	return ""
}

// topicClaimKindRank orders kinds for the final tiebreak: KafkaTopic first.
func topicClaimKindRank(obj client.Object) int {
	if topicClaimKind(obj) == kindKafkaTopic {
		return 0
	}
	return 1
}

// olderTopicClaim reports whether a's claim on a topic identity precedes b's:
// the olderIdentityClaim order (earlier creationTimestamp, then smaller
// namespace/name), then KafkaTopic before KafkaMirrorTopic. The kind tiebreak
// makes this a strict total order across both kinds, where a KafkaTopic and a
// KafkaMirrorTopic may share namespace, name and timestamp.
func olderTopicClaim(a, b client.Object) bool {
	if olderIdentityClaim(a, b) {
		return true
	}
	if olderIdentityClaim(b, a) {
		return false
	}
	return topicClaimKindRank(a) < topicClaimKindRank(b)
}

// sameTopicClaimant reports whether a and b are the same CR (same kind,
// namespace and name). Kind matters: a KafkaTopic and a KafkaMirrorTopic may
// share a namespace/name and are still rivals.
func sameTopicClaimant(a, b client.Object) bool {
	return topicClaimKind(a) == topicClaimKind(b) &&
		a.GetNamespace() == b.GetNamespace() && a.GetName() == b.GetName()
}

// listTopicClaims lists every live (non-deleting) KafkaTopic and
// KafkaMirrorTopic claiming (clusterRef, topic). With namespaceLocal the scan
// is restricted to namespace (clusterRef resolves per namespace); otherwise
// every namespace is scanned (a global --cluster-namespace). A List error is
// returned as-is, wrapped: a failed scan is never a negative result.
func listTopicClaims(ctx context.Context, c client.Reader, clusterRef, topic, namespace string, namespaceLocal bool) ([]client.Object, error) {
	var listOpts []client.ListOption
	if namespaceLocal {
		listOpts = append(listOpts, client.InNamespace(namespace))
	}
	var claims []client.Object
	keep := func(obj client.Object) {
		if !obj.GetDeletionTimestamp().IsZero() {
			return // a deleting CR never blocks others
		}
		if cr, name, ok := topicClaimName(obj); ok && cr == clusterRef && name == topic {
			claims = append(claims, obj)
		}
	}

	var topics v1alpha1.KafkaTopicList
	if err := c.List(ctx, &topics, listOpts...); err != nil {
		return nil, fmt.Errorf("listing KafkaTopics for topic-claim check: %w", err)
	}
	for i := range topics.Items {
		keep(&topics.Items[i])
	}
	var mirrors v1alpha1.KafkaMirrorTopicList
	if err := c.List(ctx, &mirrors, listOpts...); err != nil {
		return nil, fmt.Errorf("listing KafkaMirrorTopics for topic-claim check: %w", err)
	}
	for i := range mirrors.Items {
		keep(&mirrors.Items[i])
	}
	return claims, nil
}

// topicRivals lists the live claimants of obj's topic identity, excluding obj.
func topicRivals(ctx context.Context, c client.Reader, obj client.Object, namespaceLocal bool) ([]client.Object, error) {
	clusterRef, topic, ok := topicClaimName(obj)
	if !ok {
		return nil, fmt.Errorf("%T is not a topic claimant", obj)
	}
	claims, err := listTopicClaims(ctx, c, clusterRef, topic, obj.GetNamespace(), namespaceLocal)
	if err != nil {
		return nil, err
	}
	rivals := claims[:0]
	for _, other := range claims {
		if !sameTopicClaimant(other, obj) {
			rivals = append(rivals, other)
		}
	}
	return rivals, nil
}

// findOlderTopicClaimant returns the claimant that beats obj in the topic
// identity contest — the OLDEST (per olderTopicClaim) live KafkaTopic or
// KafkaMirrorTopic that precedes obj on the same (clusterRef, topic) — or nil
// when obj is the winner (or the only claimant).
func findOlderTopicClaimant(ctx context.Context, c client.Reader, obj client.Object, namespaceLocal bool) (client.Object, error) {
	rivals, err := topicRivals(ctx, c, obj, namespaceLocal)
	if err != nil {
		return nil, err
	}
	var winner client.Object
	for _, r := range rivals {
		if !olderTopicClaim(r, obj) {
			continue // r is newer than obj: r loses to obj
		}
		if winner == nil || olderTopicClaim(r, winner) {
			winner = r
		}
	}
	return winner, nil
}

// findLiveTopicCoClaimant returns another live (non-deleting) claimant of
// obj's topic identity, of either kind, or nil when obj is the identity's only
// live claimant. It is age-blind (any other claimant counts, older or newer):
// on the deletion path the question is whether anyone still needs the topic.
// When several exist the oldest is returned, for a deterministic message.
func findLiveTopicCoClaimant(ctx context.Context, c client.Reader, obj client.Object, namespaceLocal bool) (client.Object, error) {
	rivals, err := topicRivals(ctx, c, obj, namespaceLocal)
	if err != nil {
		return nil, err
	}
	var found client.Object
	for _, r := range rivals {
		if found == nil || olderTopicClaim(r, found) {
			found = r
		}
	}
	return found, nil
}
