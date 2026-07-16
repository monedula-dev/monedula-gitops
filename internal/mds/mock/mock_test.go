package mock

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/mds"
)

func ctx() context.Context { return context.Background() }

// kafkaScope is a helper for building a typical kafka-scoped Scope.
func kafkaScope(cluster string) mds.Scope {
	return mds.Scope{Type: "kafka", KafkaCluster: cluster}
}

func srScope(cluster, srCluster string) mds.Scope {
	return mds.Scope{Type: "schema-registry", KafkaCluster: cluster, SubCluster: srCluster}
}

// rb is a convenience constructor for a resource-scoped role binding.
func rb(principal, role string, scope mds.Scope, resType, resName, patternType string) mds.RoleBinding {
	return mds.RoleBinding{
		Principal: principal,
		Role:      role,
		Scope:     scope,
		Resource:  &mds.ResourcePattern{Type: resType, Name: resName, PatternType: patternType},
	}
}

// clusterRB is a convenience constructor for a cluster-scoped role binding.
func clusterRB(principal, role string, scope mds.Scope) mds.RoleBinding {
	return mds.RoleBinding{Principal: principal, Role: role, Scope: scope}
}

// --- interface compliance ---

func TestImplementsInterface(t *testing.T) {
	var _ mds.Client = New()
}

// --- Add then List ---

func TestAddThenList(t *testing.T) {
	m := New()
	scope := kafkaScope("lkc-abc")
	binding := rb("User:alice", "DeveloperRead", scope, "Topic", "payments.orders", "literal")

	require.NoError(t, m.AddRoleBinding(ctx(), binding))

	got, err := m.ListRoleBindings(ctx(), scope)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, binding, got[0])
}

// --- Add idempotence ---

func TestAddIdempotent(t *testing.T) {
	m := New()
	scope := kafkaScope("lkc-abc")
	binding := rb("User:alice", "DeveloperRead", scope, "Topic", "payments.orders", "literal")

	require.NoError(t, m.AddRoleBinding(ctx(), binding))
	require.NoError(t, m.AddRoleBinding(ctx(), binding))

	got, err := m.ListRoleBindings(ctx(), scope)
	require.NoError(t, err)
	require.Len(t, got, 1, "adding the same binding twice must not duplicate it")

	// Both calls are recorded even though the second did not mutate.
	require.Len(t, m.Calls(), 2)
}

// --- Remove ---

func TestRemoveThenGone(t *testing.T) {
	m := New()
	scope := kafkaScope("lkc-abc")
	binding := rb("User:alice", "DeveloperRead", scope, "Topic", "payments.orders", "literal")

	require.NoError(t, m.AddRoleBinding(ctx(), binding))
	require.NoError(t, m.RemoveRoleBinding(ctx(), binding))

	got, err := m.ListRoleBindings(ctx(), scope)
	require.NoError(t, err)
	require.Empty(t, got)
}

// --- Remove absent is a no-op ---

func TestRemoveAbsentIsNoOp(t *testing.T) {
	m := New()
	scope := kafkaScope("lkc-abc")
	binding := rb("User:alice", "DeveloperRead", scope, "Topic", "payments.orders", "literal")

	// Remove before ever adding must not error.
	require.NoError(t, m.RemoveRoleBinding(ctx(), binding))

	// The call is still recorded (mirrors SR mock.DeleteSubject convention).
	require.Equal(t, []string{"RemoveRoleBinding " + binding.Key()}, m.Calls())
}

// --- Scope filtering ---

func TestListScopeFiltered(t *testing.T) {
	m := New()
	scopeA := kafkaScope("lkc-aaa")
	scopeB := kafkaScope("lkc-bbb")
	srScopeA := srScope("lkc-aaa", "lsrc-zzz")

	bindingA := rb("User:alice", "DeveloperRead", scopeA, "Topic", "orders", "literal")
	bindingB := rb("User:bob", "DeveloperRead", scopeB, "Topic", "orders", "literal")
	bindingSR := clusterRB("User:carol", "ResourceOwner", srScopeA)

	require.NoError(t, m.AddRoleBinding(ctx(), bindingA))
	require.NoError(t, m.AddRoleBinding(ctx(), bindingB))
	require.NoError(t, m.AddRoleBinding(ctx(), bindingSR))

	// Listing scopeA returns only bindingA.
	gotA, err := m.ListRoleBindings(ctx(), scopeA)
	require.NoError(t, err)
	require.Len(t, gotA, 1)
	require.Equal(t, bindingA, gotA[0])

	// Listing scopeB returns only bindingB.
	gotB, err := m.ListRoleBindings(ctx(), scopeB)
	require.NoError(t, err)
	require.Len(t, gotB, 1)
	require.Equal(t, bindingB, gotB[0])

	// Listing srScopeA returns only bindingSR (different Type + SubCluster).
	gotSR, err := m.ListRoleBindings(ctx(), srScopeA)
	require.NoError(t, err)
	require.Len(t, gotSR, 1)
	require.Equal(t, bindingSR, gotSR[0])
}

// --- Deterministic ordering ---

func TestListDeterministicOrder(t *testing.T) {
	m := New()
	scope := kafkaScope("lkc-abc")

	// Insert in reverse alphabetical order by principal to exercise sorting.
	for _, principal := range []string{"User:carol", "User:alice", "User:bob"} {
		require.NoError(t, m.AddRoleBinding(ctx(), rb(principal, "DeveloperRead", scope, "Topic", "t", "literal")))
	}

	got, err := m.ListRoleBindings(ctx(), scope)
	require.NoError(t, err)
	require.Len(t, got, 3)

	// Verify sorted by key (principal comes first in the key).
	for i := 1; i < len(got); i++ {
		require.Less(t, got[i-1].Key(), got[i].Key(), "results must be sorted by key")
	}

	// Deterministic across repeated calls.
	again, err := m.ListRoleBindings(ctx(), scope)
	require.NoError(t, err)
	require.Equal(t, got, again)
}

// --- New with initial bindings ---

func TestNewWithInitialBindings(t *testing.T) {
	scope := kafkaScope("lkc-abc")
	binding := rb("User:alice", "DeveloperRead", scope, "Topic", "payments.orders", "literal")

	m := New(binding)

	got, err := m.ListRoleBindings(ctx(), scope)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, binding, got[0])

	// Seeding via New should not record any calls.
	require.Empty(t, m.Calls())
}

// --- FromFile ---

func TestFromFileSeeds(t *testing.T) {
	m, err := FromFile(filepath.Join("testdata", "state.yaml"))
	require.NoError(t, err)

	// kafka scope bindings.
	kafkaS := kafkaScope("lkc-abc123")
	kafkaBindings, err := m.ListRoleBindings(ctx(), kafkaS)
	require.NoError(t, err)
	require.Len(t, kafkaBindings, 2)

	// schema-registry scope binding.
	srS := srScope("lkc-abc123", "lsrc-xyz789")
	srBindings, err := m.ListRoleBindings(ctx(), srS)
	require.NoError(t, err)
	require.Len(t, srBindings, 1)
	require.Equal(t, "User:bob", srBindings[0].Principal)
	require.Equal(t, "ResourceOwner", srBindings[0].Role)
	require.Nil(t, srBindings[0].Resource, "cluster-scoped binding must have nil Resource")

	// Seeding via FromFile must not record any calls.
	require.Empty(t, m.Calls())
}

func TestFromFileMissing(t *testing.T) {
	_, err := FromFile(filepath.Join("testdata", "does-not-exist.yaml"))
	require.Error(t, err)
}

// --- Calls recorder ---

func TestCallsRecorded(t *testing.T) {
	m := New()
	scope := kafkaScope("lkc-abc")
	binding := rb("User:alice", "DeveloperRead", scope, "Topic", "t", "literal")

	require.NoError(t, m.AddRoleBinding(ctx(), binding))
	require.NoError(t, m.RemoveRoleBinding(ctx(), binding))

	calls := m.Calls()
	require.Len(t, calls, 2)
	require.Equal(t, "AddRoleBinding "+binding.Key(), calls[0])
	require.Equal(t, "RemoveRoleBinding "+binding.Key(), calls[1])
}

func TestListNotRecorded(t *testing.T) {
	m := New()
	scope := kafkaScope("lkc-abc")

	_, err := m.ListRoleBindings(ctx(), scope)
	require.NoError(t, err)

	// List is a read; must not be recorded.
	require.Empty(t, m.Calls())
}

// --- FailOn ---

func TestFailOnAddDoesNotMutateState(t *testing.T) {
	m := New()
	scope := kafkaScope("lkc-abc")
	binding := rb("User:alice", "DeveloperRead", scope, "Topic", "t", "literal")

	boom := errors.New("boom")
	m.FailOn("AddRoleBinding", binding.Key(), boom)

	err := m.AddRoleBinding(ctx(), binding)
	require.ErrorIs(t, err, boom)

	// State not mutated.
	got, err := m.ListRoleBindings(ctx(), scope)
	require.NoError(t, err)
	require.Empty(t, got)

	// Call was still recorded.
	require.Len(t, m.Calls(), 1)
}

func TestFailOnRemoveDoesNotMutateState(t *testing.T) {
	m := New()
	scope := kafkaScope("lkc-abc")
	binding := rb("User:alice", "DeveloperRead", scope, "Topic", "t", "literal")

	require.NoError(t, m.AddRoleBinding(ctx(), binding))

	boom := errors.New("nope")
	m.FailOn("RemoveRoleBinding", binding.Key(), boom)

	err := m.RemoveRoleBinding(ctx(), binding)
	require.ErrorIs(t, err, boom)

	// Not removed.
	got, err := m.ListRoleBindings(ctx(), scope)
	require.NoError(t, err)
	require.Len(t, got, 1)
}

// --- Defensive copy of ResourcePattern pointer ---

func TestListReturnsDefensiveCopy(t *testing.T) {
	m := New()
	scope := kafkaScope("lkc-abc")
	binding := rb("User:alice", "DeveloperRead", scope, "Topic", "original-name", "literal")

	require.NoError(t, m.AddRoleBinding(ctx(), binding))

	// First list — get the binding back.
	got, err := m.ListRoleBindings(ctx(), scope)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NotNil(t, got[0].Resource)

	// Mutate the returned Resource.Name — must not corrupt mock state.
	got[0].Resource.Name = "mutated-name"

	// Second list — mock state must be unchanged.
	got2, err := m.ListRoleBindings(ctx(), scope)
	require.NoError(t, err)
	require.Len(t, got2, 1)
	require.Equal(t, "original-name", got2[0].Resource.Name, "ListRoleBindings must return defensive copies of ResourcePattern")
}

// --- Cluster-scoped (nil Resource) bindings ---

func TestClusterScopedBinding(t *testing.T) {
	m := New()
	scope := kafkaScope("lkc-abc")
	binding := clusterRB("User:admin", "CloudClusterAdmin", scope)

	require.NoError(t, m.AddRoleBinding(ctx(), binding))

	got, err := m.ListRoleBindings(ctx(), scope)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Nil(t, got[0].Resource)
	require.Equal(t, "User:admin", got[0].Principal)
}
