package importer_test

import (
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	apivalidation "k8s.io/apimachinery/pkg/util/validation"

	"github.com/monedula-dev/monedula-gitops/internal/access"
	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/importer"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
)

// acl builds an Allow/*/literal ACLState (the foldable-eligible default) unless
// overridden via the variadic options.
func acl(principal, resType, resName, op string, opts ...func(*kafka.ACLState)) kafka.ACLState {
	a := kafka.ACLState{
		Principal:    principal,
		Host:         "*",
		ResourceType: resType,
		ResourceName: resName,
		PatternType:  "literal",
		Operation:    op,
		Permission:   "Allow",
	}
	for _, o := range opts {
		o(&a)
	}
	return a
}

func withHost(h string) func(*kafka.ACLState) { return func(a *kafka.ACLState) { a.Host = h } }
func withPattern(p string) func(*kafka.ACLState) {
	return func(a *kafka.ACLState) { a.PatternType = p }
}
func withPerm(p string) func(*kafka.ACLState) { return func(a *kafka.ACLState) { a.Permission = p } }

func topic(name string) importer.TopicSnapshot {
	return importer.TopicSnapshot{Name: name, Partitions: 3, ReplicationFactor: 2}
}

// stateToACL converts a kafka.ACLState to an access.ACL.
func stateToACL(s kafka.ACLState) access.ACL {
	return access.ACL{
		Principal:    s.Principal,
		Host:         s.Host,
		ResourceType: s.ResourceType,
		ResourceName: s.ResourceName,
		PatternType:  s.PatternType,
		Operation:    s.Operation,
		Permission:   s.Permission,
	}
}

// snapshotKeySet returns the FullKey set of a snapshot's ACLs.
func snapshotKeySet(snap importer.Snapshot) map[string]bool {
	out := map[string]bool{}
	for _, a := range snap.ACLs {
		out[stateToACL(a).FullKey()] = true
	}
	return out
}

// compileAll compiles the Result back into a desired ACL set and returns the
// FullKey set. This is the inverse of import; the round-trip invariant requires
// it to equal the original snapshot's ACL set.
func compileAll(r importer.Result) map[string]bool {
	var all []access.ACL
	for _, tp := range r.Topics {
		all = append(all, access.CompileTopic(tp)...)
	}
	for _, pol := range r.Policies {
		all = append(all, access.CompilePolicy(pol)...)
	}
	desired, errs := access.BuildDesiredSet(all)
	if len(errs) > 0 {
		// Conflicts would mean the import produced contradictory ACLs.
		panic(errs)
	}
	out := map[string]bool{}
	for _, a := range desired {
		out[a.FullKey()] = true
	}
	return out
}

func keySetsEqual(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

func TestBuildProducerFolds(t *testing.T) {
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("t")},
		ACLs: []kafka.ACLState{
			acl("User:a", "topic", "t", "Write"),
			acl("User:a", "topic", "t", "Describe"),
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	if len(r.Topics) != 1 {
		t.Fatalf("want 1 topic, got %d", len(r.Topics))
	}
	prods := r.Topics[0].Spec.Access.Producers
	if len(prods) != 1 || prods[0].Principal != "User:a" {
		t.Fatalf("want one producer User:a, got %+v", prods)
	}
	if prods[0].Operations != nil {
		t.Fatalf("want nil operations (default), got %+v", prods[0].Operations)
	}
	if len(r.Policies) != 0 {
		t.Fatalf("want no policies, got %d", len(r.Policies))
	}
	if len(r.Warnings) != 0 {
		t.Fatalf("want no warnings, got %v", r.Warnings)
	}
}

func TestBuildConsumerFoldsWithSingleGroup(t *testing.T) {
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("t")},
		ACLs: []kafka.ACLState{
			acl("User:a", "topic", "t", "Read"),
			acl("User:a", "topic", "t", "Describe"),
			acl("User:a", "group", "g", "Read"),
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	cons := r.Topics[0].Spec.Access.Consumers
	if len(cons) != 1 || cons[0].Principal != "User:a" || cons[0].Group != "g" {
		t.Fatalf("want one consumer User:a/g, got %+v", cons)
	}
	if len(r.Policies) != 0 {
		t.Fatalf("want no policies, got %d", len(r.Policies))
	}
	if len(r.Warnings) != 0 {
		t.Fatalf("want no warnings, got %v", r.Warnings)
	}
}

func TestBuildAmbiguousConsumerToPolicy(t *testing.T) {
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("t")},
		ACLs: []kafka.ACLState{
			acl("User:a", "topic", "t", "Read"),
			acl("User:a", "topic", "t", "Describe"),
			acl("User:a", "group", "g1", "Read"),
			acl("User:a", "group", "g2", "Read"),
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	if len(r.Topics[0].Spec.Access.Consumers) != 0 {
		t.Fatalf("want no folded consumers, got %+v", r.Topics[0].Spec.Access.Consumers)
	}
	if len(r.Policies) != 1 {
		t.Fatalf("want 1 policy, got %d", len(r.Policies))
	}
	if len(r.Warnings) == 0 {
		t.Fatalf("want a warning about ambiguous group mapping")
	}
	// The warning must say "ambiguous", not "no matching group-Read".
	var hasAmbiguous bool
	for _, w := range r.Warnings {
		if strings.Contains(w, "ambiguous") {
			hasAmbiguous = true
		}
		if strings.Contains(w, "no matching group-Read") {
			t.Fatalf("unexpected host-mismatch warning on a genuine >1-group case: %q", w)
		}
	}
	if !hasAmbiguous {
		t.Fatalf("want 'ambiguous' warning, got %v", r.Warnings)
	}
	// The policy must reproduce the original ACL set.
	if !keySetsEqual(compileAll(r), snapshotKeySet(snap)) {
		t.Fatalf("round-trip mismatch:\n got=%v\nwant=%v", compileAll(r), snapshotKeySet(snap))
	}
}

func TestBuildHostSplitConsumerEmittedRaw(t *testing.T) {
	// Spec §8.4 host-split case: the consumer's topic-Read+Describe is on hostA
	// and its group-Read is on hostB. After the (principal,host) regroup they land
	// in two different buckets, so the topic bucket has consumer topics but 0
	// group-Reads. Both buckets must be emitted raw (0 folded consumers) and the
	// warning must name the host and say "no matching group-Read in host", NOT
	// "ambiguous".
	const hostA = "10.0.1.0/24"
	const hostB = "10.0.2.0/24"
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("t")},
		ACLs: []kafka.ACLState{
			acl("User:a", "topic", "t", "Read", withHost(hostA)),
			acl("User:a", "topic", "t", "Describe", withHost(hostA)),
			acl("User:a", "group", "g", "Read", withHost(hostB)),
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	// No consumer must be folded.
	if len(r.Topics[0].Spec.Access.Consumers) != 0 {
		t.Fatalf("want no folded consumers, got %+v", r.Topics[0].Spec.Access.Consumers)
	}
	// Both ACL sets must appear in raw policies.
	if len(r.Policies) == 0 {
		t.Fatalf("want at least one raw policy, got none")
	}
	// The warning must reference the host-mismatch, not "ambiguous".
	var hasHostMismatch bool
	for _, w := range r.Warnings {
		if strings.Contains(w, "no matching group-Read in host") {
			hasHostMismatch = true
		}
		if strings.Contains(w, "ambiguous") {
			t.Fatalf("unexpected 'ambiguous' warning on a host-split (0 groups) case: %q", w)
		}
	}
	if !hasHostMismatch {
		t.Fatalf("want 'no matching group-Read in host' warning, got %v", r.Warnings)
	}
	// Round-trip invariant: all original ACLs must be recoverable.
	if !keySetsEqual(compileAll(r), snapshotKeySet(snap)) {
		t.Fatalf("round-trip mismatch:\n got=%v\nwant=%v", compileAll(r), snapshotKeySet(snap))
	}
}

func TestBuildGenuineTwoGroupsAmbiguousWarning(t *testing.T) {
	// Same principal+host, consumer topic Read+Describe, TWO distinct group-Read
	// groups: genuinely ambiguous → warning must say "ambiguous", not
	// "no matching group-Read".
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("t")},
		ACLs: []kafka.ACLState{
			acl("User:a", "topic", "t", "Read", withHost("10.0.0.0/8")),
			acl("User:a", "topic", "t", "Describe", withHost("10.0.0.0/8")),
			acl("User:a", "group", "g1", "Read", withHost("10.0.0.0/8")),
			acl("User:a", "group", "g2", "Read", withHost("10.0.0.0/8")),
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	if len(r.Topics[0].Spec.Access.Consumers) != 0 {
		t.Fatalf("want no folded consumers, got %+v", r.Topics[0].Spec.Access.Consumers)
	}
	var hasAmbiguous bool
	for _, w := range r.Warnings {
		if strings.Contains(w, "ambiguous") {
			hasAmbiguous = true
		}
		if strings.Contains(w, "no matching group-Read") {
			t.Fatalf("unexpected host-mismatch warning on a genuine >1-group case: %q", w)
		}
	}
	if !hasAmbiguous {
		t.Fatalf("want 'ambiguous' warning, got %v", r.Warnings)
	}
	if !keySetsEqual(compileAll(r), snapshotKeySet(snap)) {
		t.Fatalf("round-trip mismatch:\n got=%v\nwant=%v", compileAll(r), snapshotKeySet(snap))
	}
}

func TestBuildPrefixedToPolicy(t *testing.T) {
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("t")},
		ACLs: []kafka.ACLState{
			acl("User:a", "topic", "pre", "Write", withPattern("prefixed")),
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	if len(r.Topics[0].Spec.Access.Producers) != 0 {
		t.Fatalf("prefixed ACL must not fold to producer")
	}
	if len(r.Policies) != 1 {
		t.Fatalf("want 1 policy, got %d", len(r.Policies))
	}
	if r.Policies[0].Spec.Rules[0].Resource.PatternType != "prefixed" {
		t.Fatalf("want prefixed patternType, got %q", r.Policies[0].Spec.Rules[0].Resource.PatternType)
	}
	if !keySetsEqual(compileAll(r), snapshotKeySet(snap)) {
		t.Fatalf("round-trip mismatch")
	}
}

func TestBuildClusterAndTxnToPolicy(t *testing.T) {
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("t")},
		ACLs: []kafka.ACLState{
			acl("User:a", "cluster", "kafka-cluster", "Create"),
			acl("User:a", "transactionalId", "txn-1", "Write"),
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	if len(r.Policies) != 1 {
		t.Fatalf("want 1 policy, got %d", len(r.Policies))
	}
	if !keySetsEqual(compileAll(r), snapshotKeySet(snap)) {
		t.Fatalf("round-trip mismatch")
	}
}

func TestBuildDenyToPolicy(t *testing.T) {
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("t")},
		ACLs: []kafka.ACLState{
			acl("User:a", "topic", "t", "Write", withPerm("Deny")),
			acl("User:a", "topic", "t", "Describe", withPerm("Deny")),
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	if len(r.Topics[0].Spec.Access.Producers) != 0 {
		t.Fatalf("Deny ACL must never fold")
	}
	if len(r.Policies) != 1 {
		t.Fatalf("want 1 policy, got %d", len(r.Policies))
	}
	if r.Policies[0].Spec.Rules[0].Permission != "Deny" {
		t.Fatalf("want Deny permission, got %q", r.Policies[0].Spec.Rules[0].Permission)
	}
	if !keySetsEqual(compileAll(r), snapshotKeySet(snap)) {
		t.Fatalf("round-trip mismatch")
	}
}

func TestBuildHostScopedProducerFolds(t *testing.T) {
	// Spec §8.4: a host-scoped producer (Write+Describe on an imported topic with
	// a CIDR host) now FOLDS into Spec.Access.Producers, stamped with the host,
	// rather than falling through to a raw KafkaAccessPolicy.
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("t")},
		ACLs: []kafka.ACLState{
			acl("User:a", "topic", "t", "Write", withHost("10.0.0.0/8")),
			acl("User:a", "topic", "t", "Describe", withHost("10.0.0.0/8")),
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	prods := r.Topics[0].Spec.Access.Producers
	if len(prods) != 1 {
		t.Fatalf("want one folded producer, got %+v", prods)
	}
	if prods[0].Principal != "User:a" || prods[0].Host != "10.0.0.0/8" {
		t.Fatalf("want producer User:a host 10.0.0.0/8, got %+v", prods[0])
	}
	if len(r.Policies) != 0 {
		t.Fatalf("host-scoped producer must not become a raw policy; got %d", len(r.Policies))
	}
	if !keySetsEqual(compileAll(r), snapshotKeySet(snap)) {
		t.Fatalf("round-trip mismatch")
	}
}

func TestBuildSamePrincipalTwoHostsTwoProducers(t *testing.T) {
	// Spec §8.4: the same principal granted the same producer ops from two hosts
	// (* and a CIDR) yields TWO ProducerAccess entries. The * entry leaves Host ""
	// (so omitempty drops it from the rendered manifest); the CIDR entry stamps it.
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("t")},
		ACLs: []kafka.ACLState{
			acl("User:a", "topic", "t", "Write"),
			acl("User:a", "topic", "t", "Describe"),
			acl("User:a", "topic", "t", "Write", withHost("10.0.0.0/8")),
			acl("User:a", "topic", "t", "Describe", withHost("10.0.0.0/8")),
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	prods := r.Topics[0].Spec.Access.Producers
	if len(prods) != 2 {
		t.Fatalf("want two producer entries (one per host), got %+v", prods)
	}
	// Deterministic order: sorted by (principal, host); "" sorts before "10...".
	if prods[0].Principal != "User:a" || prods[0].Host != "" {
		t.Fatalf("want first producer User:a with empty host (=*), got %+v", prods[0])
	}
	if prods[1].Principal != "User:a" || prods[1].Host != "10.0.0.0/8" {
		t.Fatalf("want second producer User:a host 10.0.0.0/8, got %+v", prods[1])
	}
	if len(r.Policies) != 0 {
		t.Fatalf("want no policies, got %d", len(r.Policies))
	}
	if !keySetsEqual(compileAll(r), snapshotKeySet(snap)) {
		t.Fatalf("round-trip mismatch")
	}
}

func TestBuildHostScopedConsumerFolds(t *testing.T) {
	// Spec §8.4: a host-scoped consumer (topic Read+Describe on host X plus group
	// Read on host X) folds into ConsumerAccess stamped with host X. The topic-Read
	// and group-Read share the same (principal,host) bucket, so the one-groupRead
	// rule resolves naturally.
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("t")},
		ACLs: []kafka.ACLState{
			acl("User:a", "topic", "t", "Read", withHost("10.0.0.0/8")),
			acl("User:a", "topic", "t", "Describe", withHost("10.0.0.0/8")),
			acl("User:a", "group", "g", "Read", withHost("10.0.0.0/8")),
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	cons := r.Topics[0].Spec.Access.Consumers
	if len(cons) != 1 {
		t.Fatalf("want one folded consumer, got %+v", cons)
	}
	if cons[0].Principal != "User:a" || cons[0].Group != "g" || cons[0].Host != "10.0.0.0/8" {
		t.Fatalf("want consumer User:a/g host 10.0.0.0/8, got %+v", cons[0])
	}
	if len(r.Policies) != 0 {
		t.Fatalf("host-scoped consumer must not become a raw policy; got %d", len(r.Policies))
	}
	if !keySetsEqual(compileAll(r), snapshotKeySet(snap)) {
		t.Fatalf("round-trip mismatch")
	}
}

func TestBuildStarHostProducerLeavesHostUnset(t *testing.T) {
	// Regression: a plain *-host producer still folds with no Host set, so the
	// common manifest stays clean (omitempty drops the field).
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("t")},
		ACLs: []kafka.ACLState{
			acl("User:a", "topic", "t", "Write"),
			acl("User:a", "topic", "t", "Describe"),
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	prods := r.Topics[0].Spec.Access.Producers
	if len(prods) != 1 {
		t.Fatalf("want one folded producer, got %+v", prods)
	}
	if prods[0].Host != "" {
		t.Fatalf("*-host producer must leave Host unset, got %q", prods[0].Host)
	}
	if len(r.Policies) != 0 {
		t.Fatalf("want no policies, got %d", len(r.Policies))
	}
}

func TestBuildTopicMetaAndConfig(t *testing.T) {
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{
			{Name: "withrf", Partitions: 6, ReplicationFactor: 3, Config: map[string]string{"retention.ms": "1000"}},
			{Name: "norf", Partitions: 1, ReplicationFactor: 0},
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	tWith, tNor := r.Topics[0], r.Topics[1]
	if tWith.Name != "withrf" || tNor.Name != "norf" {
		t.Fatalf("topic order/name wrong: %q %q", tWith.Name, tNor.Name)
	}
	if tWith.Kind != "KafkaTopic" || tWith.APIVersion == "" {
		t.Fatalf("typemeta wrong: %+v", tWith.TypeMeta)
	}
	if tWith.Annotations["gitops.monedula.dev/imported-from-cluster"] != "prod" {
		t.Fatalf("annotation wrong: %+v", tWith.Annotations)
	}
	if tWith.Spec.ClusterRef.Name != "prod" {
		t.Fatalf("clusterRef wrong: %+v", tWith.Spec.ClusterRef)
	}
	if tWith.Spec.TopicName != "withrf" {
		t.Fatalf("topicName wrong: %q", tWith.Spec.TopicName)
	}
	if tWith.Spec.Partitions != 6 {
		t.Fatalf("partitions wrong: %d", tWith.Spec.Partitions)
	}
	if tWith.Spec.ReplicationFactor == nil || *tWith.Spec.ReplicationFactor != 3 {
		t.Fatalf("RF wrong: %v", tWith.Spec.ReplicationFactor)
	}
	if tWith.Spec.Config["retention.ms"] != "1000" {
		t.Fatalf("config wrong: %+v", tWith.Spec.Config)
	}
	if tWith.Spec.DeletionPolicy != "Orphan" {
		t.Fatalf("deletionPolicy wrong: %q", tWith.Spec.DeletionPolicy)
	}
	if tNor.Spec.ReplicationFactor != nil {
		t.Fatalf("RF should be nil when 0, got %v", *tNor.Spec.ReplicationFactor)
	}
	if tNor.Spec.Config != nil {
		t.Fatalf("config should be nil when empty, got %+v", tNor.Spec.Config)
	}
}

func TestBuildSlugCollisionDisambiguated(t *testing.T) {
	// "User:a-b" and "User:a:b" both slug to the same base "user-a-b". Each
	// carries a raw (prefixed) ACL so both become policies. The names must be
	// distinct, a warning must be recorded, and the round-trip must hold.
	snap := importer.Snapshot{
		ACLs: []kafka.ACLState{
			acl("User:a-b", "topic", "pre", "Write", withPattern("prefixed")),
			acl("User:a:b", "topic", "pre", "Write", withPattern("prefixed")),
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	if len(r.Policies) != 2 {
		t.Fatalf("want 2 policies, got %d", len(r.Policies))
	}
	if r.Policies[0].Name == r.Policies[1].Name {
		t.Fatalf("policy names must be distinct, both are %q", r.Policies[0].Name)
	}
	var collisionWarn bool
	for _, w := range r.Warnings {
		if strings.Contains(w, "policy name collision") {
			collisionWarn = true
		}
	}
	if !collisionWarn {
		t.Fatalf("want a collision-disambiguation warning, got %v", r.Warnings)
	}
	if !keySetsEqual(compileAll(r), snapshotKeySet(snap)) {
		t.Fatalf("round-trip mismatch:\n got=%v\nwant=%v", compileAll(r), snapshotKeySet(snap))
	}
}

// TestBuildTopicMetaNameSlugged verifies that a Kafka topic name invalid as a
// Kubernetes object name (uppercase, underscore) gets a slugged metadata.name
// while spec.topicName carries the live name verbatim, and the manifest still
// round-trips (no drift in ACL/RBAC semantics — only the object identity changed).
func TestBuildTopicMetaNameSlugged(t *testing.T) {
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("Orders_V2")},
		ACLs: []kafka.ACLState{
			acl("User:a", "topic", "Orders_V2", "Write"),
			acl("User:a", "topic", "Orders_V2", "Describe"),
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	if len(r.Topics) != 1 {
		t.Fatalf("want 1 topic, got %d", len(r.Topics))
	}
	tp := r.Topics[0]
	if tp.Name != "orders-v2" {
		t.Fatalf("want slugged metadata.name orders-v2, got %q", tp.Name)
	}
	if tp.Spec.TopicName != "Orders_V2" {
		t.Fatalf("want spec.topicName to carry the live name Orders_V2 verbatim, got %q", tp.Spec.TopicName)
	}
	if errs := apivalidation.IsDNS1123Subdomain(tp.Name); len(errs) != 0 {
		t.Fatalf("metadata.name %q must be DNS-1123 valid: %v", tp.Name, errs)
	}
	if !keySetsEqual(compileAll(r), snapshotKeySet(snap)) {
		t.Fatalf("round-trip mismatch:\n got=%v\nwant=%v", compileAll(r), snapshotKeySet(snap))
	}
}

// TestBuildTopicMetaNameAlreadyValidUnchanged verifies that a topic name
// already valid as a Kubernetes object name (including one with dots, a
// legal DNS-1123 separator) is NOT altered by slugging: metadata.name stays
// byte-identical to the live topic name.
func TestBuildTopicMetaNameAlreadyValidUnchanged(t *testing.T) {
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("payments.orders"), topic("plain")},
	}
	r := importer.Build(snap, "prod", nil, nil)

	if r.Topics[0].Name != "payments.orders" {
		t.Fatalf("want unchanged metadata.name payments.orders, got %q", r.Topics[0].Name)
	}
	if r.Topics[0].Spec.TopicName != "payments.orders" {
		t.Fatalf("want spec.topicName payments.orders, got %q", r.Topics[0].Spec.TopicName)
	}
	if r.Topics[1].Name != "plain" {
		t.Fatalf("want unchanged metadata.name plain, got %q", r.Topics[1].Name)
	}
}

// TestBuildTopicMetaNameCollisionDisambiguated verifies that two distinct
// Kafka topic names that slug to the same base metadata.name ("Orders_V2" and
// "orders-v2" both -> "orders-v2") get deterministic "-2" disambiguation, a
// warning is recorded, and BOTH topics still round-trip (spec.topicName keeps
// each topic's ACLs targeting the correct live Kafka resource).
func TestBuildTopicMetaNameCollisionDisambiguated(t *testing.T) {
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("Orders_V2"), topic("orders-v2")},
		ACLs: []kafka.ACLState{
			acl("User:a", "topic", "Orders_V2", "Write"),
			acl("User:a", "topic", "Orders_V2", "Describe"),
			acl("User:b", "topic", "orders-v2", "Write"),
			acl("User:b", "topic", "orders-v2", "Describe"),
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	if len(r.Topics) != 2 {
		t.Fatalf("want 2 topics, got %d", len(r.Topics))
	}
	names := map[string]bool{}
	topicNames := map[string]bool{}
	for _, tp := range r.Topics {
		if names[tp.Name] {
			t.Fatalf("metadata.name %q used by more than one topic", tp.Name)
		}
		names[tp.Name] = true
		topicNames[tp.Spec.TopicName] = true
	}
	if !topicNames["Orders_V2"] || !topicNames["orders-v2"] {
		t.Fatalf("want spec.topicName set for both live names, got %+v", topicNames)
	}
	var collisionWarn bool
	for _, w := range r.Warnings {
		if strings.Contains(w, "topic name collision") {
			collisionWarn = true
		}
	}
	if !collisionWarn {
		t.Fatalf("want a topic name collision warning, got %v", r.Warnings)
	}
	if !keySetsEqual(compileAll(r), snapshotKeySet(snap)) {
		t.Fatalf("round-trip mismatch:\n got=%v\nwant=%v", compileAll(r), snapshotKeySet(snap))
	}
}

func TestBuildPrincipalWithProducerAndConsumerRoles(t *testing.T) {
	// P produces on T1 (Write+Describe) and consumes T2 (Read+Describe) with a
	// single group G (Read): P folds as producer under T1 and consumer under T2,
	// no policy for P, round-trip holds.
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("t1"), topic("t2")},
		ACLs: []kafka.ACLState{
			acl("User:p", "topic", "t1", "Write"),
			acl("User:p", "topic", "t1", "Describe"),
			acl("User:p", "topic", "t2", "Read"),
			acl("User:p", "topic", "t2", "Describe"),
			acl("User:p", "group", "g", "Read"),
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	byName := map[string]*v1alpha1.KafkaTopic{}
	for _, tp := range r.Topics {
		byName[tp.Name] = tp
	}
	t1, t2 := byName["t1"], byName["t2"]
	if len(t1.Spec.Access.Producers) != 1 || t1.Spec.Access.Producers[0].Principal != "User:p" {
		t.Fatalf("want User:p as producer on t1, got %+v", t1.Spec.Access.Producers)
	}
	if len(t2.Spec.Access.Consumers) != 1 || t2.Spec.Access.Consumers[0].Principal != "User:p" || t2.Spec.Access.Consumers[0].Group != "g" {
		t.Fatalf("want User:p/g as consumer on t2, got %+v", t2.Spec.Access.Consumers)
	}
	if len(r.Policies) != 0 {
		t.Fatalf("want no policies, got %d", len(r.Policies))
	}
	if !keySetsEqual(compileAll(r), snapshotKeySet(snap)) {
		t.Fatalf("round-trip mismatch")
	}
}

func TestBuildTopicAclForNonImportedTopic(t *testing.T) {
	// An ACL on a topic not present in snapshot.Topics becomes a raw policy rule
	// (not folded, not dropped); round-trip holds.
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("present")},
		ACLs: []kafka.ACLState{
			acl("User:a", "topic", "absent", "Write"),
			acl("User:a", "topic", "absent", "Describe"),
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	if len(r.Topics[0].Spec.Access.Producers) != 0 {
		t.Fatalf("ACL on non-imported topic must not fold")
	}
	if len(r.Policies) != 1 {
		t.Fatalf("want 1 policy for the non-imported-topic ACL, got %d", len(r.Policies))
	}
	if !keySetsEqual(compileAll(r), snapshotKeySet(snap)) {
		t.Fatalf("round-trip mismatch")
	}
}

func TestBuildSharedGroupAcrossPrincipals(t *testing.T) {
	// Group g is read by both P1 (consuming t1) and P2 (consuming t2): both fold
	// as consumers independently; round-trip holds.
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("t1"), topic("t2")},
		ACLs: []kafka.ACLState{
			acl("User:p1", "topic", "t1", "Read"),
			acl("User:p1", "topic", "t1", "Describe"),
			acl("User:p1", "group", "g", "Read"),
			acl("User:p2", "topic", "t2", "Read"),
			acl("User:p2", "topic", "t2", "Describe"),
			acl("User:p2", "group", "g", "Read"),
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	byName := map[string]*v1alpha1.KafkaTopic{}
	for _, tp := range r.Topics {
		byName[tp.Name] = tp
	}
	t1, t2 := byName["t1"], byName["t2"]
	if len(t1.Spec.Access.Consumers) != 1 || t1.Spec.Access.Consumers[0].Principal != "User:p1" || t1.Spec.Access.Consumers[0].Group != "g" {
		t.Fatalf("want User:p1/g consumer on t1, got %+v", t1.Spec.Access.Consumers)
	}
	if len(t2.Spec.Access.Consumers) != 1 || t2.Spec.Access.Consumers[0].Principal != "User:p2" || t2.Spec.Access.Consumers[0].Group != "g" {
		t.Fatalf("want User:p2/g consumer on t2, got %+v", t2.Spec.Access.Consumers)
	}
	if len(r.Policies) != 0 {
		t.Fatalf("want no policies, got %d", len(r.Policies))
	}
	if !keySetsEqual(compileAll(r), snapshotKeySet(snap)) {
		t.Fatalf("round-trip mismatch")
	}
}

func TestBuildRoundTripProperty(t *testing.T) {
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("orders"), topic("events")},
		ACLs: []kafka.ACLState{
			// producer on orders
			acl("User:prod", "topic", "orders", "Write"),
			acl("User:prod", "topic", "orders", "Describe"),
			// consumer w/ single group on events
			acl("User:cons", "topic", "events", "Read"),
			acl("User:cons", "topic", "events", "Describe"),
			acl("User:cons", "group", "cg", "Read"),
			// prefixed topic ACL
			acl("User:pre", "topic", "ord", "Write", withPattern("prefixed")),
			// cluster ACL
			acl("User:admin", "cluster", "kafka-cluster", "Alter"),
			// deny ACL
			acl("User:bad", "topic", "orders", "Read", withPerm("Deny")),
			// odd op set: Read only on a topic (not a valid producer/consumer combo)
			acl("User:odd", "topic", "orders", "Read"),
			// ambiguous consumer: two groups
			acl("User:amb", "topic", "events", "Read"),
			acl("User:amb", "topic", "events", "Describe"),
			acl("User:amb", "group", "g1", "Read"),
			acl("User:amb", "group", "g2", "Read"),
		},
	}
	// shuffle-independence: sort the input by key to mimic snapshot ordering.
	sort.Slice(snap.ACLs, func(i, j int) bool {
		return stateToACL(snap.ACLs[i]).FullKey() < stateToACL(snap.ACLs[j]).FullKey()
	})

	r := importer.Build(snap, "prod", nil, nil)

	got := compileAll(r)
	want := snapshotKeySet(snap)
	if !keySetsEqual(got, want) {
		t.Fatalf("ROUND-TRIP MISMATCH\n got=%v\nwant=%v", got, want)
	}
}

// avroBody is a representative AVRO schema definition used by the schema tests.
const avroBody = `{"type":"record","name":"Order","namespace":"payments","fields":[{"name":"id","type":"string"}]}`

func TestBuildWithSchema(t *testing.T) {
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("payments.orders")},
		Schemas: map[string]importer.TopicSchemas{
			"payments.orders": {
				Value: &schemaregistry.SubjectState{
					Subject:       "payments.orders-value",
					Schema:        schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: avroBody},
					Compatibility: "BACKWARD",
				},
			},
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	if len(r.Topics) != 1 {
		t.Fatalf("want 1 topic, got %d", len(r.Topics))
	}
	sc := r.Topics[0].Spec.Schema
	if sc == nil {
		t.Fatalf("want spec.schema set, got nil")
	}
	if sc.Format != "AVRO" {
		t.Fatalf("want format AVRO, got %q", sc.Format)
	}
	if sc.SubjectStrategy != "TopicName" {
		t.Fatalf("want subjectStrategy TopicName, got %q", sc.SubjectStrategy)
	}
	if sc.Compatibility != "BACKWARD" {
		t.Fatalf("want compatibility BACKWARD, got %q", sc.Compatibility)
	}
	if sc.ValueSchema == nil || sc.ValueSchema.ValueFrom.File != "../schemas/payments.orders-value.avsc" {
		t.Fatalf("want valueSchema file ../schemas/payments.orders-value.avsc, got %+v", sc.ValueSchema)
	}
	if sc.KeySchema != nil {
		t.Fatalf("want no keySchema, got %+v", sc.KeySchema)
	}

	// SchemaFiles records the verbatim content.
	if len(r.SchemaFiles) != 1 {
		t.Fatalf("want 1 schema file, got %d", len(r.SchemaFiles))
	}
	sf := r.SchemaFiles[0]
	if sf.BaseName != "payments.orders-value" || sf.Ext != "avsc" {
		t.Fatalf("want base payments.orders-value ext avsc, got %q.%q", sf.BaseName, sf.Ext)
	}
	if sf.Content != avroBody {
		t.Fatalf("want verbatim content, got %q", sf.Content)
	}

	// AssignNamespaces stamps the SchemaFile namespace from the owning topic.
	if err := importer.AssignNamespaces(&r, importer.NamespaceStrategy{Kind: "prefix", Separator: "."}); err != nil {
		t.Fatalf("AssignNamespaces: %v", err)
	}
	if r.Topics[0].Namespace != "payments" {
		t.Fatalf("want topic ns payments, got %q", r.Topics[0].Namespace)
	}
	if r.SchemaFiles[0].Namespace != "payments" {
		t.Fatalf("want schema file ns payments, got %q", r.SchemaFiles[0].Namespace)
	}
}

func TestBuildWithKeySchema(t *testing.T) {
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("t")},
		Schemas: map[string]importer.TopicSchemas{
			"t": {
				Value: &schemaregistry.SubjectState{
					Subject: "t-value",
					Schema:  schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: avroBody},
				},
				Key: &schemaregistry.SubjectState{
					Subject: "t-key",
					Schema:  schemaregistry.Schema{Type: schemaregistry.JSON, Definition: `{"type":"string"}`},
				},
			},
		},
	}
	r := importer.Build(snap, "prod", nil, nil)
	sc := r.Topics[0].Spec.Schema
	if sc == nil || sc.KeySchema == nil {
		t.Fatalf("want value+key schema, got %+v", sc)
	}
	if sc.Compatibility != "" {
		t.Fatalf("want empty compatibility omitted, got %q", sc.Compatibility)
	}
	if sc.KeySchema.ValueFrom.File != "../schemas/t-key.json" {
		t.Fatalf("want key file ../schemas/t-key.json, got %q", sc.KeySchema.ValueFrom.File)
	}
	if len(r.SchemaFiles) != 2 {
		t.Fatalf("want 2 schema files (value+key), got %d", len(r.SchemaFiles))
	}
}

func TestBuildUnmatchedSubjectWarns(t *testing.T) {
	snap := importer.Snapshot{
		Topics:            []importer.TopicSnapshot{topic("t")},
		UnmatchedSubjects: []string{"orphan-value", "zzz-key"},
	}
	r := importer.Build(snap, "prod", nil, nil)

	want := map[string]bool{
		`subject "orphan-value" not imported (does not map to an imported topic via TopicName/TopicRecordName)`: false,
		`subject "zzz-key" not imported (does not map to an imported topic via TopicName/TopicRecordName)`:      false,
	}
	for _, w := range r.Warnings {
		if _, ok := want[w]; ok {
			want[w] = true
		}
	}
	for w, seen := range want {
		if !seen {
			t.Fatalf("missing expected warning %q in %v", w, r.Warnings)
		}
	}
	if !sort.StringsAreSorted(r.Warnings) {
		t.Fatalf("warnings not sorted: %v", r.Warnings)
	}
}

func TestBuildKeyOnlySubjectWarns(t *testing.T) {
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("t")},
		Schemas: map[string]importer.TopicSchemas{
			"t": {
				Key: &schemaregistry.SubjectState{
					Subject: "t-key",
					Schema:  schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: avroBody},
				},
			},
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	// Value subject is required: no spec.schema, no schema files.
	if r.Topics[0].Spec.Schema != nil {
		t.Fatalf("want no spec.schema for key-only subject, got %+v", r.Topics[0].Spec.Schema)
	}
	if len(r.SchemaFiles) != 0 {
		t.Fatalf("want no schema files for key-only subject, got %d", len(r.SchemaFiles))
	}

	want := `subject "t-key" imported without a value subject; skipped (value schema required)`
	if !containsWarning(r.Warnings, want) {
		t.Fatalf("missing expected warning %q in %v", want, r.Warnings)
	}
	if !sort.StringsAreSorted(r.Warnings) {
		t.Fatalf("warnings not sorted: %v", r.Warnings)
	}
}

func TestBuildUnknownSchemaTypeWarns(t *testing.T) {
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("t")},
		Schemas: map[string]importer.TopicSchemas{
			"t": {
				Value: &schemaregistry.SubjectState{
					Subject: "t-value",
					Schema:  schemaregistry.Schema{Type: schemaregistry.SchemaType("WEIRD"), Definition: avroBody},
				},
			},
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	// Unknown type still writes the file as .txt.
	if len(r.SchemaFiles) != 1 || r.SchemaFiles[0].Ext != "txt" {
		t.Fatalf("want 1 schema file with ext txt, got %+v", r.SchemaFiles)
	}

	want := `subject "t-value" has unknown schema type "WEIRD"; writing as .txt`
	if !containsWarning(r.Warnings, want) {
		t.Fatalf("missing expected warning %q in %v", want, r.Warnings)
	}
	if !sort.StringsAreSorted(r.Warnings) {
		t.Fatalf("warnings not sorted: %v", r.Warnings)
	}
}

func containsWarning(warnings []string, want string) bool {
	for _, w := range warnings {
		if w == want {
			return true
		}
	}
	return false
}

func TestBuildOmitsRFUnderPlacementConstraints(t *testing.T) {
	// I14: a topic whose live config contains confluent.placement.constraints
	// must NOT carry spec.replicationFactor (the tool's own validation rejects
	// the combination; the constraint determines replication). A topic without
	// the constraint keeps its RF (existing behavior pinned).
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{
			{Name: "placed", Partitions: 6, ReplicationFactor: 3, Config: map[string]string{
				"confluent.placement.constraints": `{"version":2,"replicas":[{"count":3}]}`,
			}},
			{Name: "plain", Partitions: 6, ReplicationFactor: 3},
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	placed, plain := r.Topics[0], r.Topics[1]
	if placed.Name != "placed" || plain.Name != "plain" {
		t.Fatalf("topic order/name wrong: %q %q", placed.Name, plain.Name)
	}
	if placed.Spec.ReplicationFactor != nil {
		t.Fatalf("placement-constrained topic must omit RF, got %d", *placed.Spec.ReplicationFactor)
	}
	if placed.Spec.Config["confluent.placement.constraints"] == "" {
		t.Fatalf("placement constraint config must be retained, got %+v", placed.Spec.Config)
	}
	if plain.Spec.ReplicationFactor == nil || *plain.Spec.ReplicationFactor != 3 {
		t.Fatalf("plain topic must keep RF 3, got %v", plain.Spec.ReplicationFactor)
	}
}

// TestBuildTopicRecordNameStrategy verifies that a TopicRecordName-matched topic
// emits SubjectStrategy == "TopicRecordName" and that the value schema file is
// still written with the standard ../schemas/<metaName>-value.<ext> path.
func TestBuildTopicRecordNameStrategy(t *testing.T) {
	const orderBody = `{"type":"record","name":"Order","namespace":"com.example","fields":[{"name":"id","type":"string"}]}`
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("orders")},
		Schemas: map[string]importer.TopicSchemas{
			"orders": {
				Value: &schemaregistry.SubjectState{
					Subject:       "orders-com.example.Order",
					Schema:        schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: orderBody},
					Compatibility: "FULL",
				},
				ValueStrategy: "TopicRecordName",
			},
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	if len(r.Topics) != 1 {
		t.Fatalf("want 1 topic, got %d", len(r.Topics))
	}
	sc := r.Topics[0].Spec.Schema
	if sc == nil {
		t.Fatalf("want spec.schema set, got nil")
	}
	if sc.SubjectStrategy != "TopicRecordName" {
		t.Fatalf("want subjectStrategy TopicRecordName, got %q", sc.SubjectStrategy)
	}
	if sc.Format != "AVRO" {
		t.Fatalf("want format AVRO, got %q", sc.Format)
	}
	if sc.Compatibility != "FULL" {
		t.Fatalf("want compatibility FULL, got %q", sc.Compatibility)
	}
	// File path is unchanged: ../schemas/<metaName>-value.<ext>
	if sc.ValueSchema == nil || sc.ValueSchema.ValueFrom.File != "../schemas/orders-value.avsc" {
		t.Fatalf("want valueSchema file ../schemas/orders-value.avsc, got %+v", sc.ValueSchema)
	}
	// Schema file is emitted with the body content.
	if len(r.SchemaFiles) != 1 {
		t.Fatalf("want 1 schema file, got %d", len(r.SchemaFiles))
	}
	sf := r.SchemaFiles[0]
	if sf.BaseName != "orders-value" || sf.Ext != "avsc" {
		t.Fatalf("want base orders-value ext avsc, got %q.%q", sf.BaseName, sf.Ext)
	}
	if sf.Content != orderBody {
		t.Fatalf("want verbatim body content, got %q", sf.Content)
	}
}

// TestBuildTopicNameStrategyRegression ensures a TopicName-matched topic (Strategy
// == "TopicName" or "") still emits SubjectStrategy == "TopicName" (regression).
func TestBuildTopicNameStrategyRegression(t *testing.T) {
	for _, strategy := range []string{"TopicName", ""} {
		strategy := strategy
		t.Run("strategy="+strategy, func(t *testing.T) {
			snap := importer.Snapshot{
				Topics: []importer.TopicSnapshot{topic("t")},
				Schemas: map[string]importer.TopicSchemas{
					"t": {
						Value: &schemaregistry.SubjectState{
							Subject: "t-value",
							Schema:  schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: avroBody},
						},
						ValueStrategy: strategy,
					},
				},
			}
			r := importer.Build(snap, "prod", nil, nil)

			sc := r.Topics[0].Spec.Schema
			if sc == nil {
				t.Fatalf("want spec.schema set, got nil")
			}
			if sc.SubjectStrategy != "TopicName" {
				t.Fatalf("want subjectStrategy TopicName, got %q", sc.SubjectStrategy)
			}
		})
	}
}

// TestBuildRecordNameSubjectsCarried verifies that RecordNameSubjects from the
// snapshot are carried through to Result.RecordNameSubjects and do NOT appear in
// Result.Warnings.
func TestBuildRecordNameSubjectsCarried(t *testing.T) {
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("t")},
		RecordNameSubjects: []importer.RecordNameSubject{
			{Subject: "com.example.Payment", RecordName: "com.example.Payment", SchemaType: "AVRO"},
			{Subject: "com.example.Refund", RecordName: "com.example.Refund", SchemaType: "AVRO"},
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	if len(r.RecordNameSubjects) != 2 {
		t.Fatalf("want 2 RecordNameSubjects, got %d", len(r.RecordNameSubjects))
	}
	if r.RecordNameSubjects[0].Subject != "com.example.Payment" {
		t.Fatalf("want first subject com.example.Payment, got %q", r.RecordNameSubjects[0].Subject)
	}
	if r.RecordNameSubjects[1].Subject != "com.example.Refund" {
		t.Fatalf("want second subject com.example.Refund, got %q", r.RecordNameSubjects[1].Subject)
	}
	// RecordNameSubjects must NOT appear in Warnings.
	for _, w := range r.Warnings {
		if strings.Contains(w, "com.example.Payment") || strings.Contains(w, "com.example.Refund") {
			t.Fatalf("RecordNameSubject must not appear in Warnings; got %q", w)
		}
	}
}

// TestBuildRecordNameSubjectsPreservedOnACLFallback verifies Fix 1: when the ACL
// round-trip fails and fallbackAllRaw is invoked, RecordNameSubjects from the
// snapshot are re-attached to the result and are not silently dropped.
//
// The fallback is triggered by seeding a conflicting Allow+Deny pair on the same
// subject key. Both ACLs go to raw policies (Deny is not fold-eligible; the
// lone Allow-Write without Describe is not a recognized producer pattern and also
// goes raw). buildPolicies emits both as separate rules; CompilePolicy outputs
// them; BuildDesiredSet detects the Allow/Deny conflict and returns errors;
// roundTripsClean returns false → fallbackAllRaw is called.
func TestBuildRecordNameSubjectsPreservedOnACLFallback(t *testing.T) {
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("t")},
		ACLs: []kafka.ACLState{
			// Allow+Deny on same (principal,host,resource,op): triggers conflict in
			// BuildDesiredSet after recompile, causing roundTripsClean to return false.
			acl("User:a", "topic", "t", "Write"),
			acl("User:a", "topic", "t", "Write", withPerm("Deny")),
		},
		RecordNameSubjects: []importer.RecordNameSubject{
			{Subject: "com.example.Payment", RecordName: "com.example.Payment", SchemaType: "AVRO"},
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	// The fallback warning must be present, confirming the fallback path ran.
	var fallbackWarned bool
	for _, w := range r.Warnings {
		if strings.Contains(w, "access reconstruction could not reproduce live state") {
			fallbackWarned = true
		}
	}
	if !fallbackWarned {
		t.Fatalf("expected fallback warning to confirm fallback path was taken; got warnings: %v", r.Warnings)
	}

	// RecordNameSubjects must survive the fallback.
	if len(r.RecordNameSubjects) != 1 {
		t.Fatalf("want 1 RecordNameSubject after ACL fallback, got %d", len(r.RecordNameSubjects))
	}
	if r.RecordNameSubjects[0].Subject != "com.example.Payment" {
		t.Fatalf("want subject com.example.Payment, got %q", r.RecordNameSubjects[0].Subject)
	}
}

// TestBuildSchemaAmbiguitiesWarn verifies that SchemaAmbiguities from the snapshot
// are surfaced as warnings in Result.Warnings.
func TestBuildSchemaAmbiguitiesWarn(t *testing.T) {
	const ambiguityMsg = `topic "x": subject "x-com.example.OtherEvent" also reconstructs to a record-based value; already attributed to "x-com.example.Event", skipped this one`
	snap := importer.Snapshot{
		Topics:            []importer.TopicSnapshot{topic("x")},
		SchemaAmbiguities: []string{ambiguityMsg},
	}
	r := importer.Build(snap, "prod", nil, nil)

	if !containsWarning(r.Warnings, ambiguityMsg) {
		t.Fatalf("want SchemaAmbiguities message in Warnings, got %v", r.Warnings)
	}
	if !sort.StringsAreSorted(r.Warnings) {
		t.Fatalf("warnings not sorted: %v", r.Warnings)
	}
}

// ---- RBAC tests ----

func mdsCfgT() *v1alpha1.MDSConfig {
	return &v1alpha1.MDSConfig{Endpoint: "https://mds", Clusters: v1alpha1.MDSClusters{KafkaCluster: "kid"}}
}

func liveRB(principal, role, resType, resName string) mds.RoleBinding {
	rb := mds.RoleBinding{Principal: principal, Role: role, Scope: mds.Scope{Type: "kafka", KafkaCluster: "kid"}}
	if resType != "" {
		rb.Resource = &mds.ResourcePattern{Type: resType, Name: resName, PatternType: "literal"}
	}
	return rb
}

func TestBuildRBACOnlyFoldsProducer(t *testing.T) {
	snap := importer.Snapshot{
		Topics:       []importer.TopicSnapshot{{Name: "orders", Partitions: 1}},
		RoleBindings: []mds.RoleBinding{liveRB("User:svc", "DeveloperWrite", "Topic", "orders")},
	}
	r := importer.Build(snap, "prod", []string{"rbac"}, mdsCfgT())
	require.Empty(t, r.RoleBindings)
	require.Len(t, r.Topics[0].Spec.Access.Producers, 1)
}

func TestBuildEmitsExplicitForClusterScoped(t *testing.T) {
	snap := importer.Snapshot{
		Topics:       []importer.TopicSnapshot{{Name: "orders", Partitions: 1}},
		RoleBindings: []mds.RoleBinding{liveRB("User:admin", "SystemAdmin", "", "")},
	}
	r := importer.Build(snap, "prod", []string{"rbac"}, mdsCfgT())
	require.Len(t, r.RoleBindings, 1)
	require.Equal(t, "SystemAdmin", r.RoleBindings[0].Spec.Role)
}

func TestBuildRBACSplitFoldAndExplicit(t *testing.T) {
	prefixed := mds.RoleBinding{Principal: "User:svc", Role: "DeveloperWrite",
		Scope:    mds.Scope{Type: "kafka", KafkaCluster: "kid"},
		Resource: &mds.ResourcePattern{Type: "Topic", Name: "team-", PatternType: "prefixed"}}
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{{Name: "orders", Partitions: 1}},
		RoleBindings: []mds.RoleBinding{
			liveRB("User:svc", "DeveloperWrite", "Topic", "orders"), // folds
			prefixed, // explicit
		},
	}
	r := importer.Build(snap, "prod", []string{"rbac"}, mdsCfgT())
	require.Len(t, r.Topics[0].Spec.Access.Producers, 1)
	require.Len(t, r.RoleBindings, 1)
	require.Equal(t, "prefixed", r.RoleBindings[0].Spec.Resources[0].PatternType)
}

func TestBuildDualSymmetricFoldsOnce(t *testing.T) {
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{{Name: "orders", Partitions: 1}},
		ACLs: []kafka.ACLState{
			{Principal: "User:svc", Host: "*", ResourceType: "topic", ResourceName: "orders", PatternType: "literal", Operation: "Write", Permission: "Allow"},
			{Principal: "User:svc", Host: "*", ResourceType: "topic", ResourceName: "orders", PatternType: "literal", Operation: "Describe", Permission: "Allow"},
		},
		RoleBindings: []mds.RoleBinding{liveRB("User:svc", "DeveloperWrite", "Topic", "orders")},
	}
	r := importer.Build(snap, "prod", []string{"acl", "rbac"}, mdsCfgT())
	require.Len(t, r.Topics[0].Spec.Access.Producers, 1, "symmetric grant folds once")
	require.Empty(t, r.RoleBindings)
	require.Empty(t, r.Policies)
}

func TestBuildDualAsymmetricFallsBack(t *testing.T) {
	// RBAC-only grant on a dual cluster: folded access would imply an ACL not in
	// live → ACL verify fails → all-explicit fallback (access cleared, RB explicit).
	snap := importer.Snapshot{
		Topics:       []importer.TopicSnapshot{{Name: "orders", Partitions: 1}},
		RoleBindings: []mds.RoleBinding{liveRB("User:svc", "DeveloperWrite", "Topic", "orders")},
	}
	r := importer.Build(snap, "prod", []string{"acl", "rbac"}, mdsCfgT())
	require.Empty(t, r.Topics[0].Spec.Access.Producers)
	require.Len(t, r.RoleBindings, 1)
}

func TestBuildRBACRoundTripProperty(t *testing.T) {
	// folded + explicit together reproduce the live role-binding set exactly:
	// verify by asserting that Build produced no fallback warning.
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{{Name: "orders", Partitions: 1}},
		RoleBindings: []mds.RoleBinding{
			liveRB("User:svc", "DeveloperRead", "Topic", "orders"),
			liveRB("User:svc", "DeveloperRead", "Group", "cg"),
			liveRB("User:admin", "SystemAdmin", "", ""),
		},
	}
	r := importer.Build(snap, "prod", []string{"rbac"}, mdsCfgT())
	for _, w := range r.Warnings {
		require.NotContains(t, w, "could not reproduce live state",
			"unexpected fallback warning: round-trip invariant violated")
	}
}

// Dual [acl,rbac] cluster with an ACL-only grant and NO live role bindings:
// the ACL fold must NOT survive (re-apply would create a spurious role binding),
// so the two-sided verify falls back to all-explicit: the grant becomes a
// KafkaAccessPolicy and NO KafkaRoleBinding / topic access is emitted.
func TestBuildDualACLOnlyGrantNoLiveRBsFallsBack(t *testing.T) {
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{{Name: "orders", Partitions: 1}},
		ACLs: []kafka.ACLState{
			{Principal: "User:svc", Host: "*", ResourceType: "topic", ResourceName: "orders", PatternType: "literal", Operation: "Write", Permission: "Allow"},
			{Principal: "User:svc", Host: "*", ResourceType: "topic", ResourceName: "orders", PatternType: "literal", Operation: "Describe", Permission: "Allow"},
		},
		// no RoleBindings
	}
	r := importer.Build(snap, "prod", []string{"acl", "rbac"}, mdsCfgT())
	require.Empty(t, r.Topics[0].Spec.Access.Producers, "ACL-only grant must not fold into access on a dual cluster")
	require.Empty(t, r.RoleBindings, "no live role bindings → none emitted")
	require.NotEmpty(t, r.Policies, "ACL-only grant must fall back to KafkaAccessPolicy")
}

// TestBuildKeyCompatDivergenceWarns verifies that a key subject whose explicit
// compatibility level differs from the value subject's produces a warning: the
// spec.schema block carries a SINGLE compatibility level that apply stamps onto
// both subjects, so a diverging key level would otherwise be silently rewritten
// on the next apply. The manifest itself still carries the value subject's level.
func TestBuildKeyCompatDivergenceWarns(t *testing.T) {
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("t")},
		Schemas: map[string]importer.TopicSchemas{
			"t": {
				Value: &schemaregistry.SubjectState{
					Subject:       "t-value",
					Schema:        schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: avroBody},
					Compatibility: "BACKWARD",
				},
				Key: &schemaregistry.SubjectState{
					Subject:       "t-key",
					Schema:        schemaregistry.Schema{Type: schemaregistry.JSON, Definition: `{"type":"string"}`},
					Compatibility: "FULL",
				},
			},
		},
	}
	r := importer.Build(snap, "prod", nil, nil)

	sc := r.Topics[0].Spec.Schema
	if sc == nil {
		t.Fatalf("want spec.schema set, got nil")
	}
	if sc.Compatibility != "BACKWARD" {
		t.Fatalf("manifest must carry the value subject's level BACKWARD, got %q", sc.Compatibility)
	}
	want := `topic "t": key subject "t-key" compatibility "FULL" differs from value subject "t-value" compatibility "BACKWARD"; the manifest carries the value subject's level — align the key subject manually or manage it explicitly`
	if !containsWarning(r.Warnings, want) {
		t.Fatalf("missing expected divergence warning %q in %v", want, r.Warnings)
	}
	if !sort.StringsAreSorted(r.Warnings) {
		t.Fatalf("warnings not sorted: %v", r.Warnings)
	}
}

// TestBuildKeyCompatIdenticalOrUnsetDoesNotWarn pins the non-divergent cases:
// a key subject with no explicit level, or with the same level as the value
// subject, must NOT produce a compatibility warning.
func TestBuildKeyCompatIdenticalOrUnsetDoesNotWarn(t *testing.T) {
	for _, keyCompat := range []string{"", "BACKWARD"} {
		t.Run("keyCompat="+keyCompat, func(t *testing.T) {
			snap := importer.Snapshot{
				Topics: []importer.TopicSnapshot{topic("t")},
				Schemas: map[string]importer.TopicSchemas{
					"t": {
						Value: &schemaregistry.SubjectState{
							Subject:       "t-value",
							Schema:        schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: avroBody},
							Compatibility: "BACKWARD",
						},
						Key: &schemaregistry.SubjectState{
							Subject:       "t-key",
							Schema:        schemaregistry.Schema{Type: schemaregistry.JSON, Definition: `{"type":"string"}`},
							Compatibility: keyCompat,
						},
					},
				},
			}
			r := importer.Build(snap, "prod", nil, nil)

			if r.Topics[0].Spec.Schema == nil {
				t.Fatalf("want spec.schema set, got nil")
			}
			for _, w := range r.Warnings {
				if strings.Contains(w, "compatibility") {
					t.Fatalf("unexpected compatibility warning: %q", w)
				}
			}
		})
	}
}

// TestBuildFallbackPreservesSchemaWarnings verifies that the accumulated schema
// warnings survive the fallbackAllExplicit path. The fallback is triggered by a
// conflicting Allow+Deny ACL pair (see
// TestBuildRecordNameSubjectsPreservedOnACLFallback); the snapshot also carries
// an unmatched subject whose warning previously vanished when the Result was
// rebuilt by the fallback.
func TestBuildFallbackPreservesSchemaWarnings(t *testing.T) {
	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{topic("t")},
		ACLs: []kafka.ACLState{
			acl("User:a", "topic", "t", "Write"),
			acl("User:a", "topic", "t", "Write", withPerm("Deny")),
		},
		UnmatchedSubjects: []string{"orphan-value"},
	}
	r := importer.Build(snap, "prod", nil, nil)

	// The fallback path must have run.
	var fallbackWarned bool
	for _, w := range r.Warnings {
		if strings.Contains(w, "access reconstruction could not reproduce live state") {
			fallbackWarned = true
		}
	}
	if !fallbackWarned {
		t.Fatalf("expected fallback warning to confirm fallback path was taken; got warnings: %v", r.Warnings)
	}

	// The schema warning must survive the fallback.
	want := `subject "orphan-value" not imported (does not map to an imported topic via TopicName/TopicRecordName)`
	if !containsWarning(r.Warnings, want) {
		t.Fatalf("schema warning must survive fallbackAllExplicit; missing %q in %v", want, r.Warnings)
	}
	if !sort.StringsAreSorted(r.Warnings) {
		t.Fatalf("warnings not sorted: %v", r.Warnings)
	}
}
