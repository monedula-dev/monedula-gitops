package rbac

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

// helpers

func mds(kafkaCluster, srCluster, connectCluster, ksqlCluster string) *v1alpha1.MDSConfig {
	return &v1alpha1.MDSConfig{
		Endpoint: "https://mds.example.com",
		Clusters: v1alpha1.MDSClusters{
			KafkaCluster:          kafkaCluster,
			SchemaRegistryCluster: srCluster,
			ConnectCluster:        connectCluster,
			KsqlCluster:           ksqlCluster,
		},
	}
}

func rb(namespace, name, principal, role, scopeType string, resources []v1alpha1.RoleResource) *v1alpha1.KafkaRoleBinding {
	return &v1alpha1.KafkaRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Spec: v1alpha1.KafkaRoleBindingSpec{
			Principal: principal,
			Role:      role,
			Scope:     v1alpha1.RoleBindingScope{Type: scopeType},
			Resources: resources,
		},
	}
}

// --- Compile tests ---

func TestCompile_ClusterScoped_NoResources(t *testing.T) {
	krb := rb("ns1", "my-rb", "User:alice", "SystemAdmin", "kafka", nil)
	bindings, err := Compile(krb, mds("kafka-1", "", "", ""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(bindings) != 1 {
		t.Fatalf("got %d bindings, want 1", len(bindings))
	}
	b := bindings[0]
	if b.Resource != nil {
		t.Errorf("expected nil Resource for cluster-scoped binding, got %+v", b.Resource)
	}
	if b.Principal != "User:alice" {
		t.Errorf("Principal=%q, want %q", b.Principal, "User:alice")
	}
	if b.Role != "SystemAdmin" {
		t.Errorf("Role=%q, want %q", b.Role, "SystemAdmin")
	}
	if b.Scope.Type != "kafka" {
		t.Errorf("Scope.Type=%q, want %q", b.Scope.Type, "kafka")
	}
	if b.Scope.KafkaCluster != "kafka-1" {
		t.Errorf("Scope.KafkaCluster=%q, want %q", b.Scope.KafkaCluster, "kafka-1")
	}
	if b.Scope.SubCluster != "" {
		t.Errorf("Scope.SubCluster=%q, want empty", b.Scope.SubCluster)
	}
}

func TestCompile_ResourceScoped_NResources(t *testing.T) {
	resources := []v1alpha1.RoleResource{
		{Type: "Topic", Name: "orders", PatternType: "literal"},
		{Type: "Topic", Name: "payments", PatternType: "prefixed"},
		{Type: "Group", Name: "my-group", PatternType: "literal"},
	}
	krb := rb("ns1", "my-rb", "User:bob", "DeveloperRead", "kafka", resources)
	bindings, err := Compile(krb, mds("kafka-1", "", "", ""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(bindings) != 3 {
		t.Fatalf("got %d bindings, want 3", len(bindings))
	}
	for i, b := range bindings {
		if b.Resource == nil {
			t.Errorf("binding[%d]: Resource is nil, want non-nil", i)
			continue
		}
		if b.Resource.Type != resources[i].Type {
			t.Errorf("binding[%d]: Resource.Type=%q, want %q", i, b.Resource.Type, resources[i].Type)
		}
		if b.Resource.Name != resources[i].Name {
			t.Errorf("binding[%d]: Resource.Name=%q, want %q", i, b.Resource.Name, resources[i].Name)
		}
		if b.Resource.PatternType != resources[i].PatternType {
			t.Errorf("binding[%d]: Resource.PatternType=%q, want %q", i, b.Resource.PatternType, resources[i].PatternType)
		}
	}
}

func TestCompile_PatternTypeDefaultsToLiteral(t *testing.T) {
	resources := []v1alpha1.RoleResource{
		{Type: "Topic", Name: "events"},                        // PatternType empty
		{Type: "Topic", Name: "logs", PatternType: "prefixed"}, // explicit prefixed
	}
	krb := rb("ns1", "my-rb", "User:carol", "DeveloperWrite", "kafka", resources)
	bindings, err := Compile(krb, mds("kafka-1", "", "", ""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if bindings[0].Resource.PatternType != "literal" {
		t.Errorf("empty PatternType should default to literal, got %q", bindings[0].Resource.PatternType)
	}
	if bindings[1].Resource.PatternType != "prefixed" {
		t.Errorf("explicit prefixed PatternType should be preserved, got %q", bindings[1].Resource.PatternType)
	}
}

func TestCompile_ScopeResolution_SchemaRegistry(t *testing.T) {
	krb := rb("ns1", "my-rb", "User:alice", "ResourceOwner", "schema-registry", nil)
	bindings, err := Compile(krb, mds("kafka-1", "sr-1", "", ""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if bindings[0].Scope.SubCluster != "sr-1" {
		t.Errorf("SubCluster=%q, want %q", bindings[0].Scope.SubCluster, "sr-1")
	}
	if bindings[0].Scope.Type != "schema-registry" {
		t.Errorf("Type=%q, want %q", bindings[0].Scope.Type, "schema-registry")
	}
}

func TestCompile_ScopeResolution_Connect(t *testing.T) {
	krb := rb("ns1", "my-rb", "User:alice", "ResourceOwner", "connect", nil)
	bindings, err := Compile(krb, mds("kafka-1", "", "connect-1", ""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if bindings[0].Scope.SubCluster != "connect-1" {
		t.Errorf("SubCluster=%q, want %q", bindings[0].Scope.SubCluster, "connect-1")
	}
}

func TestCompile_ScopeResolution_Ksql(t *testing.T) {
	krb := rb("ns1", "my-rb", "User:alice", "ResourceOwner", "ksql", nil)
	bindings, err := Compile(krb, mds("kafka-1", "", "", "ksql-1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if bindings[0].Scope.SubCluster != "ksql-1" {
		t.Errorf("SubCluster=%q, want %q", bindings[0].Scope.SubCluster, "ksql-1")
	}
}

func TestCompile_Error_MissingSubCluster_SchemaRegistry(t *testing.T) {
	krb := rb("ns1", "my-rb", "User:alice", "ResourceOwner", "schema-registry", nil)
	_, err := Compile(krb, mds("kafka-1", "", "", ""))
	if err == nil {
		t.Fatal("expected error for missing SchemaRegistryCluster, got nil")
	}
	if !strings.Contains(err.Error(), "SchemaRegistryCluster") {
		t.Errorf("error should mention SchemaRegistryCluster, got: %v", err)
	}
}

func TestCompile_Error_MissingSubCluster_Connect(t *testing.T) {
	krb := rb("ns1", "my-rb", "User:alice", "ResourceOwner", "connect", nil)
	_, err := Compile(krb, mds("kafka-1", "", "", ""))
	if err == nil {
		t.Fatal("expected error for missing ConnectCluster, got nil")
	}
	if !strings.Contains(err.Error(), "ConnectCluster") {
		t.Errorf("error should mention ConnectCluster, got: %v", err)
	}
}

func TestCompile_Error_MissingSubCluster_Ksql(t *testing.T) {
	krb := rb("ns1", "my-rb", "User:alice", "ResourceOwner", "ksql", nil)
	_, err := Compile(krb, mds("kafka-1", "", "", ""))
	if err == nil {
		t.Fatal("expected error for missing KsqlCluster, got nil")
	}
	if !strings.Contains(err.Error(), "KsqlCluster") {
		t.Errorf("error should mention KsqlCluster, got: %v", err)
	}
}

func TestCompile_Error_MissingKafkaCluster(t *testing.T) {
	krb := rb("ns1", "my-rb", "User:alice", "SystemAdmin", "kafka", nil)
	_, err := Compile(krb, mds("", "", "", ""))
	if err == nil {
		t.Fatal("expected error for missing KafkaCluster, got nil")
	}
	if !strings.Contains(err.Error(), "KafkaCluster") {
		t.Errorf("error should mention KafkaCluster, got: %v", err)
	}
}

func TestCompile_SourceStamping(t *testing.T) {
	krb := rb("team-ns", "orders-rb", "User:alice", "DeveloperRead", "kafka", []v1alpha1.RoleResource{
		{Type: "Topic", Name: "orders"},
	})
	bindings, err := Compile(krb, mds("kafka-1", "", "", ""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	b := bindings[0]
	if b.SourceKind != "KafkaRoleBinding" {
		t.Errorf("SourceKind=%q, want KafkaRoleBinding", b.SourceKind)
	}
	if b.SourceNamespace != "team-ns" {
		t.Errorf("SourceNamespace=%q, want team-ns", b.SourceNamespace)
	}
	if b.SourceName != "orders-rb" {
		t.Errorf("SourceName=%q, want orders-rb", b.SourceName)
	}
}

// --- FullKey tests ---

func TestFullKey_ExcludesSource(t *testing.T) {
	b1 := RoleBinding{
		Principal: "User:alice", Role: "DeveloperRead",
		Scope:           Scope{Type: "kafka", KafkaCluster: "kafka-1"},
		Resource:        &ResourcePattern{Type: "Topic", Name: "orders", PatternType: "literal"},
		SourceKind:      "KafkaRoleBinding",
		SourceNamespace: "ns1",
		SourceName:      "rb-a",
	}
	b2 := b1
	b2.SourceNamespace = "ns2"
	b2.SourceName = "rb-b"
	b2.Prune = true

	if b1.FullKey() != b2.FullKey() {
		t.Errorf("FullKey should be same regardless of Source*, got %q vs %q", b1.FullKey(), b2.FullKey())
	}
}

// --- BuildDesiredSet tests ---

func TestBuildDesiredSet_DedupesSameSource(t *testing.T) {
	b := RoleBinding{
		Principal: "User:alice", Role: "DeveloperRead",
		Scope:           Scope{Type: "kafka", KafkaCluster: "kafka-1"},
		Resource:        &ResourcePattern{Type: "Topic", Name: "orders", PatternType: "literal"},
		SourceKind:      "KafkaRoleBinding",
		SourceNamespace: "ns1",
		SourceName:      "rb-a",
	}
	// Same binding twice from same source — should dedupe to one.
	out, errs := BuildDesiredSet([]RoleBinding{b, b})
	if len(errs) != 0 {
		t.Errorf("unexpected errors: %v", errs)
	}
	if len(out) != 1 {
		t.Errorf("got %d bindings, want 1", len(out))
	}
}

func TestBuildDesiredSet_CollisionDifferentSources(t *testing.T) {
	base := RoleBinding{
		Principal: "User:alice", Role: "DeveloperRead",
		Scope:    Scope{Type: "kafka", KafkaCluster: "kafka-1"},
		Resource: &ResourcePattern{Type: "Topic", Name: "orders", PatternType: "literal"},
	}
	b1 := base
	b1.SourceKind = "KafkaRoleBinding"
	b1.SourceNamespace = "ns1"
	b1.SourceName = "rb-a"

	b2 := base
	b2.SourceKind = "KafkaRoleBinding"
	b2.SourceNamespace = "ns2"
	b2.SourceName = "rb-b"

	out, errs := BuildDesiredSet([]RoleBinding{b1, b2})
	if len(errs) != 1 {
		t.Errorf("got %d errors, want 1 collision error", len(errs))
	}
	// The colliding binding is dropped; only one entry survives.
	if len(out) != 1 {
		t.Errorf("got %d bindings, want 1 (collision is dropped)", len(out))
	}
}

func TestBuildDesiredSet_Deterministic(t *testing.T) {
	m := mds("kafka-1", "", "", "")
	rb1, _ := Compile(rb("ns1", "rb-c", "User:carol", "DeveloperRead", "kafka", []v1alpha1.RoleResource{
		{Type: "Topic", Name: "zzz"},
	}), m)
	rb2, _ := Compile(rb("ns1", "rb-a", "User:alice", "DeveloperRead", "kafka", []v1alpha1.RoleResource{
		{Type: "Topic", Name: "aaa"},
	}), m)
	rb3, _ := Compile(rb("ns1", "rb-b", "User:bob", "DeveloperRead", "kafka", []v1alpha1.RoleResource{
		{Type: "Topic", Name: "mmm"},
	}), m)

	all := append(append(rb1, rb2...), rb3...)
	out1, _ := BuildDesiredSet(all)

	// Reverse and re-run — result should be in same order (sorted by FullKey).
	rev := make([]RoleBinding, len(all))
	for i, b := range all {
		rev[len(all)-1-i] = b
	}
	out2, _ := BuildDesiredSet(rev)

	if len(out1) != len(out2) {
		t.Fatalf("length mismatch: %d vs %d", len(out1), len(out2))
	}
	for i := range out1 {
		if out1[i].FullKey() != out2[i].FullKey() {
			t.Errorf("position %d: %q vs %q (not sorted)", i, out1[i].FullKey(), out2[i].FullKey())
		}
	}
}

func TestBuildDesiredSet_PruneAndMerge(t *testing.T) {
	base := RoleBinding{
		Principal:       "User:alice",
		Role:            "DeveloperRead",
		Scope:           Scope{Type: "kafka", KafkaCluster: "kafka-1"},
		Resource:        &ResourcePattern{Type: "Topic", Name: "orders", PatternType: "literal"},
		SourceKind:      "KafkaRoleBinding",
		SourceNamespace: "ns1",
		SourceName:      "rb-a", // same source
	}
	b1 := base
	b1.Prune = true
	b2 := base
	b2.Prune = false

	out, errs := BuildDesiredSet([]RoleBinding{b1, b2})
	if len(errs) != 0 {
		t.Errorf("unexpected errors: %v", errs)
	}
	// AND-merge: one true && one false = false
	if out[0].Prune != false {
		t.Errorf("Prune AND-merge: expected false, got true")
	}
}

// --- BuildScope tests ---

func TestBuildScope_Groups(t *testing.T) {
	m := mds("kafka-1", "", "", "")
	// Two bindings, same principal+role+scope but different resources → same scope key.
	krb := rb("ns1", "my-rb", "User:alice", "DeveloperRead", "kafka", []v1alpha1.RoleResource{
		{Type: "Topic", Name: "orders"},
		{Type: "Topic", Name: "payments"},
	})
	bindings, _ := Compile(krb, m)
	StampPrune(bindings, true)

	scope := BuildScope(bindings)
	if len(scope) != 1 {
		t.Errorf("got %d scope entries, want 1 (same principal+role+scope)", len(scope))
	}

	// Different principal → different scope key.
	krb2 := rb("ns1", "other-rb", "User:bob", "DeveloperRead", "kafka", []v1alpha1.RoleResource{
		{Type: "Topic", Name: "orders"},
	})
	bindings2, _ := Compile(krb2, m)

	allBindings := append(bindings, bindings2...)
	scope2 := BuildScope(allBindings)
	if len(scope2) != 2 {
		t.Errorf("got %d scope entries, want 2 (two principals)", len(scope2))
	}
}

func TestBuildScope_PruneAndMerge(t *testing.T) {
	b1 := RoleBinding{
		Principal: "User:alice", Role: "DeveloperRead",
		Scope:           Scope{Type: "kafka", KafkaCluster: "kafka-1"},
		Resource:        &ResourcePattern{Type: "Topic", Name: "t1", PatternType: "literal"},
		SourceNamespace: "ns1", SourceName: "rb-a",
		Prune: true,
	}
	b2 := RoleBinding{
		Principal: "User:alice", Role: "DeveloperRead",
		Scope:           Scope{Type: "kafka", KafkaCluster: "kafka-1"},
		Resource:        &ResourcePattern{Type: "Topic", Name: "t2", PatternType: "literal"},
		SourceNamespace: "ns1", SourceName: "rb-a",
		Prune: false,
	}
	scope := BuildScope([]RoleBinding{b1, b2})
	k := scopeKeyOf(b1)
	info, ok := scope[k]
	if !ok {
		t.Fatal("scope entry not found")
	}
	// AND-merge: true && false = false
	if info.Prune != false {
		t.Errorf("Prune AND-merge: expected false, got true")
	}
}

func TestBuildScope_Contains(t *testing.T) {
	b := RoleBinding{
		Principal: "User:alice", Role: "DeveloperRead",
		Scope:    Scope{Type: "kafka", KafkaCluster: "kafka-1"},
		Resource: &ResourcePattern{Type: "Topic", Name: "orders", PatternType: "literal"},
	}
	scope := BuildScope([]RoleBinding{b})

	if !scope.Contains(b) {
		t.Error("scope.Contains should return true for a managed binding")
	}

	other := b
	other.Principal = "User:bob"
	if scope.Contains(other) {
		t.Error("scope.Contains should return false for an unmanaged binding")
	}
}

// --- StampPrune tests ---

func TestStampPrune_SetsFlag(t *testing.T) {
	bindings := []RoleBinding{
		{Principal: "User:alice"},
		{Principal: "User:bob"},
	}
	StampPrune(bindings, true)
	for i, b := range bindings {
		if !b.Prune {
			t.Errorf("binding[%d].Prune=false, want true", i)
		}
	}
	StampPrune(bindings, false)
	for i, b := range bindings {
		if b.Prune {
			t.Errorf("binding[%d].Prune=true, want false", i)
		}
	}
}

// --- New tests (Task 2 review coverage) ---

// TestBuildScope_Info verifies that ManagedScope.Info returns the correct
// ScopeInfo — including Prune (AND-merged across contributors) and Source*
// attribution (first contributor wins) — for a present key, and (_, false)
// for an absent key.
func TestBuildScope_Info(t *testing.T) {
	// First contributor: prune=true, source=rb-a.
	b1 := RoleBinding{
		Principal:       "User:alice",
		Role:            "DeveloperRead",
		Scope:           Scope{Type: "kafka", KafkaCluster: "kafka-1"},
		Resource:        &ResourcePattern{Type: "Topic", Name: "orders", PatternType: "literal"},
		SourceKind:      "KafkaRoleBinding",
		SourceNamespace: "ns1",
		SourceName:      "rb-a",
		Prune:           true,
	}
	// Second contributor: same scope key (same principal+role+scope), different
	// resource pattern so it's a separate desired binding.  prune=false → the
	// AND-merge must veto.  Source* is rb-b but first contributor (rb-a) wins.
	b2 := RoleBinding{
		Principal:       "User:alice",
		Role:            "DeveloperRead",
		Scope:           Scope{Type: "kafka", KafkaCluster: "kafka-1"},
		Resource:        &ResourcePattern{Type: "Topic", Name: "payments", PatternType: "literal"},
		SourceKind:      "KafkaRoleBinding",
		SourceNamespace: "ns1",
		SourceName:      "rb-b",
		Prune:           false,
	}

	scope := BuildScope([]RoleBinding{b1, b2})

	// Info for a binding that shares the scope key → must be found.
	probe := b1 // same principal/role/scope; resource doesn't affect ScopeKey
	info, ok := scope.Info(probe)
	require.True(t, ok, "Info should find the managed scope entry")
	// AND-merge: true && false = false
	require.False(t, info.Prune, "Prune should be AND-merged to false when one contributor opts out")
	// Source* attribution stays with the first contributor.
	require.Equal(t, "KafkaRoleBinding", info.SourceKind)
	require.Equal(t, "ns1", info.SourceNamespace)
	require.Equal(t, "rb-a", info.SourceName, "first contributor owns Source attribution")

	// Info for an absent scope key → must return false.
	absent := RoleBinding{
		Principal: "User:charlie",
		Role:      "DeveloperRead",
		Scope:     Scope{Type: "kafka", KafkaCluster: "kafka-1"},
	}
	_, ok = scope.Info(absent)
	require.False(t, ok, "Info should return false for an absent key")
}

// TestFullKey_ExcludesPrune checks that two RoleBindings identical in every
// field except Prune produce the same FullKey — mirroring access's
// TestPruneExcludedFromIdentity.
func TestFullKey_ExcludesPrune(t *testing.T) {
	base := RoleBinding{
		Principal:       "User:alice",
		Role:            "DeveloperRead",
		Scope:           Scope{Type: "kafka", KafkaCluster: "kafka-1"},
		Resource:        &ResourcePattern{Type: "Topic", Name: "orders", PatternType: "literal"},
		SourceKind:      "KafkaRoleBinding",
		SourceNamespace: "ns1",
		SourceName:      "rb-a",
	}
	withPrune := base
	withPrune.Prune = true
	withoutPrune := base
	withoutPrune.Prune = false

	require.Equal(t, withPrune.FullKey(), withoutPrune.FullKey(),
		"Prune must not be part of RoleBinding identity (FullKey)")
}

// TestCompile_UnknownScopeType_PassesThrough locks the behaviour of
// resolveScope's default branch: an unrecognised scope type is NOT an error at
// compile time (Task 7 validation handles enum enforcement). The resulting
// Scope carries the unknown type verbatim, the KafkaCluster ID, and an empty
// SubCluster (no sub-cluster lookup is attempted for unknown types).
func TestCompile_UnknownScopeType_PassesThrough(t *testing.T) {
	krb := rb("ns1", "bogus-rb", "User:alice", "DeveloperRead", "bogus", nil)
	bindings, err := Compile(krb, mds("kafka-1", "", "", ""))

	// resolveScope's default branch returns the scope structurally with no
	// error; Task 7 is responsible for rejecting unknown scope-type values.
	require.NoError(t, err, "unknown scope type must not cause a Compile error (Task 7 handles enum validation)")
	require.Len(t, bindings, 1)

	s := bindings[0].Scope
	require.Equal(t, "bogus", s.Type, "unknown scope type should be preserved verbatim")
	require.Equal(t, "kafka-1", s.KafkaCluster, "KafkaCluster should always be populated")
	require.Equal(t, "", s.SubCluster, "SubCluster should be empty for an unknown scope type (no sub-cluster lookup attempted)")
}

// --- Mode threading tests (spec §16 / decision 4) ---

// TestCompile_ModeSetsFromSpec checks that Compile threads reconciliation.mode
// from the spec onto every emitted binding. Mirrors how access.CompileTopic /
// pipeline.stampACLs set Mode on ACLs.
func TestCompile_ModeSetsFromSpec(t *testing.T) {
	cases := []struct {
		name string
		mode string
		want string
	}{
		{"enforce", "Enforce", "Enforce"},
		{"detect-only", "DetectOnly", "DetectOnly"},
		{"observe-only", "ObserveOnly", "ObserveOnly"},
		{"nil-reconciliation", "", ""}, // nil Reconciliation → unattributed ""
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			krb := rb("ns1", "my-rb", "User:alice", "DeveloperRead", "kafka", []v1alpha1.RoleResource{
				{Type: "Topic", Name: "orders"},
				{Type: "Topic", Name: "payments"},
			})
			if tc.mode != "" {
				krb.Spec.Reconciliation = &v1alpha1.Reconciliation{Mode: tc.mode}
			}
			bindings, err := Compile(krb, mds("kafka-1", "", "", ""))
			require.NoError(t, err)
			for i, b := range bindings {
				require.Equal(t, tc.want, b.Mode, "binding[%d].Mode should match spec reconciliation.mode", i)
			}
		})
	}
}

// TestFullKey_ExcludesMode confirms Mode is not part of identity (FullKey),
// mirroring access.TestModeExcludedFromIdentity.
func TestFullKey_ExcludesMode(t *testing.T) {
	base := RoleBinding{
		Principal: "User:alice", Role: "DeveloperRead",
		Scope:    Scope{Type: "kafka", KafkaCluster: "kafka-1"},
		Resource: &ResourcePattern{Type: "Topic", Name: "orders", PatternType: "literal"},
	}
	enforce := base
	enforce.Mode = "Enforce"
	observe := base
	observe.Mode = "ObserveOnly"

	require.Equal(t, enforce.FullKey(), observe.FullKey(),
		"Mode must not be part of RoleBinding identity (FullKey)")
}

// TestBuildDesiredSet_ModeMostEnforcingWins checks that when the same binding
// (same source) appears twice with different modes, the most-enforcing one
// wins — mirroring access.BuildDesiredSet's mode-merge behaviour.
func TestBuildDesiredSet_ModeMostEnforcingWins(t *testing.T) {
	cases := []struct {
		name  string
		modes []string
		want  string
	}{
		{"enforce-beats-observe", []string{"ObserveOnly", "Enforce"}, "Enforce"},
		{"enforce-beats-observe-reversed", []string{"Enforce", "ObserveOnly"}, "Enforce"},
		{"detect-beats-observe", []string{"ObserveOnly", "DetectOnly"}, "DetectOnly"},
		{"enforce-beats-detect", []string{"DetectOnly", "Enforce"}, "Enforce"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base := RoleBinding{
				Principal:       "User:alice",
				Role:            "DeveloperRead",
				Scope:           Scope{Type: "kafka", KafkaCluster: "kafka-1"},
				Resource:        &ResourcePattern{Type: "Topic", Name: "orders", PatternType: "literal"},
				SourceKind:      "KafkaRoleBinding",
				SourceNamespace: "ns1",
				SourceName:      "rb-a", // same source
			}
			var in []RoleBinding
			for _, m := range tc.modes {
				b := base
				b.Mode = m
				in = append(in, b)
			}
			out, errs := BuildDesiredSet(in)
			require.Empty(t, errs)
			require.Len(t, out, 1)
			require.Equal(t, tc.want, out[0].Mode,
				"most-enforcing mode must win on same-source dedupe")
		})
	}
}

// --- v0.15 collision rule tests (spec §40 decision 4) ---

// explicit builds a RoleBinding with SourceKind="KafkaRoleBinding" (Mode Enforce).
func explicit(principal, role, resName, ns, name string) RoleBinding {
	return RoleBinding{
		Principal: principal, Role: role,
		Scope:      Scope{Type: "kafka", KafkaCluster: "kid"},
		Resource:   &ResourcePattern{Type: "Topic", Name: resName, PatternType: "literal"},
		SourceKind: "KafkaRoleBinding", SourceNamespace: ns, SourceName: name,
		Mode: "Enforce",
	}
}

// derived builds a RoleBinding with SourceKind="KafkaTopic" (Mode DetectOnly).
func derived(principal, role, resName, ns, name string) RoleBinding {
	b := explicit(principal, role, resName, ns, name)
	b.SourceKind = "KafkaTopic"
	b.Mode = "DetectOnly" // topic-derived bindings default to a less-enforcing mode for these tests
	return b
}

// TestBuildDesiredSetTwoExplicitCollide: two explicit KafkaRoleBindings with the
// same identity from different owners remain a collision (identity uniqueness,
// v0.14 behavior unchanged).
func TestBuildDesiredSetTwoExplicitCollide(t *testing.T) {
	_, errs := BuildDesiredSet([]RoleBinding{
		explicit("User:svc", "DeveloperRead", "orders", "team-a", "rb1"),
		explicit("User:svc", "DeveloperRead", "orders", "team-a", "rb2"),
	})
	if len(errs) == 0 {
		t.Fatal("two explicit bindings with same identity must collide")
	}
}

// TestBuildDesiredSetDerivedAndExplicitDedup: a topic-access-derived binding
// overlapping an explicit one dedups to a single grant (no collision);
// most-enforcing mode wins (spec §40 decision 4).
func TestBuildDesiredSetDerivedAndExplicitDedup(t *testing.T) {
	out, errs := BuildDesiredSet([]RoleBinding{
		derived("User:svc", "DeveloperRead", "orders", "team-a", "orders"),    // Mode DetectOnly
		explicit("User:svc", "DeveloperRead", "orders", "team-a", "rb-owner"), // Mode Enforce
	})
	if len(errs) != 0 {
		t.Fatalf("derived+explicit overlap must not collide, got %v", errs)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 deduped binding, got %d", len(out))
	}
	if out[0].Mode != "Enforce" {
		t.Fatalf("most-enforcing mode must win, got %q", out[0].Mode)
	}
}

// TestBuildDesiredSetTwoDerivedDedup: two topic-derived bindings with the same
// identity (e.g. two topics sharing a consumer group + principal) dedup silently.
func TestBuildDesiredSetTwoDerivedDedup(t *testing.T) {
	out, errs := BuildDesiredSet([]RoleBinding{
		derived("User:svc", "DeveloperRead", "cg", "team-a", "orders"),
		derived("User:svc", "DeveloperRead", "cg", "team-a", "payments"),
	})
	if len(errs) != 0 {
		t.Fatalf("two derived bindings must not collide, got %v", errs)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 deduped binding, got %d", len(out))
	}
}

// TestBuildScope_ModeMostEnforcingWins checks that BuildScope picks the
// most-enforcing mode across bindings that share a scope key — mirroring
// access.BuildScope's modeRank merge.
func TestBuildScope_ModeMostEnforcingWins(t *testing.T) {
	// b1 and b2 share the same ScopeKey (principal+role+scope type+cluster) but
	// have different resource patterns (Topic orders vs. Topic payments).
	b1 := RoleBinding{
		Principal: "User:alice", Role: "DeveloperRead",
		Scope:           Scope{Type: "kafka", KafkaCluster: "kafka-1"},
		Resource:        &ResourcePattern{Type: "Topic", Name: "orders", PatternType: "literal"},
		SourceKind:      "KafkaRoleBinding",
		SourceNamespace: "ns1", SourceName: "rb-a",
		Mode: "ObserveOnly",
	}
	b2 := RoleBinding{
		Principal: "User:alice", Role: "DeveloperRead",
		Scope:           Scope{Type: "kafka", KafkaCluster: "kafka-1"},
		Resource:        &ResourcePattern{Type: "Topic", Name: "payments", PatternType: "literal"},
		SourceKind:      "KafkaRoleBinding",
		SourceNamespace: "ns1", SourceName: "rb-a",
		Mode: "Enforce",
	}

	scope := BuildScope([]RoleBinding{b1, b2})
	require.Len(t, scope, 1, "both bindings share one ScopeKey")

	info, ok := scope.Info(b1)
	require.True(t, ok)
	require.Equal(t, "Enforce", info.Mode,
		"most-enforcing mode (Enforce > ObserveOnly) must win in BuildScope")
	// Source* stays with the first contributor.
	require.Equal(t, "KafkaRoleBinding", info.SourceKind)
	require.Equal(t, "ns1", info.SourceNamespace)
	require.Equal(t, "rb-a", info.SourceName)
}
