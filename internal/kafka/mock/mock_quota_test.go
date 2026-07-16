package mock_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
)

// helpers

func strPtr(s string) *string { return &s }

func userEntity(name string) []kafka.QuotaEntityComponent {
	return []kafka.QuotaEntityComponent{{Type: "user", Name: strPtr(name)}}
}

func userClientEntity(user, client string) []kafka.QuotaEntityComponent {
	return []kafka.QuotaEntityComponent{
		{Type: "user", Name: strPtr(user)},
		{Type: "client-id", Name: strPtr(client)},
	}
}

// TestSetThenListReturnsEntityAndLimits: basic round-trip.
func TestSetThenListReturnsEntityAndLimits(t *testing.T) {
	ctx := context.Background()
	c := mock.New(nil, nil)

	entity := userEntity("svc-checkout")
	limits := map[string]float64{"producer_byte_rate": 1048576}

	require.NoError(t, c.SetQuota(ctx, entity, limits))

	got, err := c.ListQuotas(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, entity, got[0].Entity)
	require.InDelta(t, 1048576.0, got[0].Limits["producer_byte_rate"], 0.001)
}

// TestSetTwiceSameEntityMergesKeys: second Set adds new keys without removing
// existing ones.
func TestSetTwiceSameEntityMergesKeys(t *testing.T) {
	ctx := context.Background()
	c := mock.New(nil, nil)

	entity := userEntity("svc-checkout")
	require.NoError(t, c.SetQuota(ctx, entity, map[string]float64{"producer_byte_rate": 1000}))
	require.NoError(t, c.SetQuota(ctx, entity, map[string]float64{"consumer_byte_rate": 2000}))

	got, err := c.ListQuotas(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.InDelta(t, 1000.0, got[0].Limits["producer_byte_rate"], 0.001)
	require.InDelta(t, 2000.0, got[0].Limits["consumer_byte_rate"], 0.001)
}

// TestSetOverwritesExistingKey: Set with the same key updates the value.
func TestSetOverwritesExistingKey(t *testing.T) {
	ctx := context.Background()
	c := mock.New(nil, nil)

	entity := userEntity("svc-checkout")
	require.NoError(t, c.SetQuota(ctx, entity, map[string]float64{"producer_byte_rate": 1000}))
	require.NoError(t, c.SetQuota(ctx, entity, map[string]float64{"producer_byte_rate": 9999}))

	got, err := c.ListQuotas(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.InDelta(t, 9999.0, got[0].Limits["producer_byte_rate"], 0.001)
}

// TestDeleteKeyLeavesOtherKeys: deleting one key preserves the rest.
func TestDeleteKeyLeavesOtherKeys(t *testing.T) {
	ctx := context.Background()
	c := mock.New(nil, nil)

	entity := userEntity("svc-checkout")
	require.NoError(t, c.SetQuota(ctx, entity, map[string]float64{
		"producer_byte_rate": 1000,
		"consumer_byte_rate": 2000,
	}))

	require.NoError(t, c.DeleteQuota(ctx, entity, []string{"producer_byte_rate"}))

	got, err := c.ListQuotas(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	_, hasProd := got[0].Limits["producer_byte_rate"]
	require.False(t, hasProd, "producer_byte_rate should have been deleted")
	require.InDelta(t, 2000.0, got[0].Limits["consumer_byte_rate"], 0.001)
}

// TestDeleteAllKeysRemovesEntity: when no limit keys remain the entity is gone
// from ListQuotas.
func TestDeleteAllKeysRemovesEntity(t *testing.T) {
	ctx := context.Background()
	c := mock.New(nil, nil)

	entity := userEntity("svc-checkout")
	require.NoError(t, c.SetQuota(ctx, entity, map[string]float64{"producer_byte_rate": 1000}))
	require.NoError(t, c.DeleteQuota(ctx, entity, []string{"producer_byte_rate"}))

	got, err := c.ListQuotas(ctx)
	require.NoError(t, err)
	require.Empty(t, got, "entity with no remaining limits must not appear in ListQuotas")
}

// TestDeleteAbsentEntityIsNoOp: DeleteQuota on an unknown entity returns no error.
func TestDeleteAbsentEntityIsNoOp(t *testing.T) {
	ctx := context.Background()
	c := mock.New(nil, nil)

	err := c.DeleteQuota(ctx, userEntity("nobody"), []string{"producer_byte_rate"})
	require.NoError(t, err)
}

// TestListQuotasSortedDeterministically: multiple entities always come back in
// the same order regardless of insertion order.
func TestListQuotasSortedDeterministically(t *testing.T) {
	ctx := context.Background()
	c := mock.New(nil, nil)

	// Insert in reverse alphabetical order by entity key.
	require.NoError(t, c.SetQuota(ctx, userEntity("z-svc"), map[string]float64{"producer_byte_rate": 1}))
	require.NoError(t, c.SetQuota(ctx, userEntity("a-svc"), map[string]float64{"producer_byte_rate": 2}))

	got, err := c.ListQuotas(ctx)
	require.NoError(t, err)
	require.Len(t, got, 2)
	// "user=a-svc" sorts before "user=z-svc".
	require.Equal(t, strPtr("a-svc"), got[0].Entity[0].Name)
	require.Equal(t, strPtr("z-svc"), got[1].Entity[0].Name)
}

// TestSetMultiComponentEntityOrdering: quotaKey is component-order-independent
// so {user, client-id} and {client-id, user} produce the same entry.
func TestSetMultiComponentEntityOrdering(t *testing.T) {
	ctx := context.Background()
	c := mock.New(nil, nil)

	// First insertion: user then client-id.
	entity1 := userClientEntity("svc-checkout", "batch")
	require.NoError(t, c.SetQuota(ctx, entity1, map[string]float64{"producer_byte_rate": 1000}))

	// Second insertion: client-id then user (reversed order).
	entity2 := []kafka.QuotaEntityComponent{
		{Type: "client-id", Name: strPtr("batch")},
		{Type: "user", Name: strPtr("svc-checkout")},
	}
	require.NoError(t, c.SetQuota(ctx, entity2, map[string]float64{"consumer_byte_rate": 2000}))

	got, err := c.ListQuotas(ctx)
	require.NoError(t, err)
	// Both insertions mapped to the same canonical key — only one entry.
	require.Len(t, got, 1)
	require.InDelta(t, 1000.0, got[0].Limits["producer_byte_rate"], 0.001)
	require.InDelta(t, 2000.0, got[0].Limits["consumer_byte_rate"], 0.001)
}

// TestNilNameDefaultEntity: a nil Name component (= per-type default) is
// handled correctly for both key generation and round-trip.
func TestNilNameDefaultEntity(t *testing.T) {
	ctx := context.Background()
	c := mock.New(nil, nil)

	entity := []kafka.QuotaEntityComponent{{Type: "user", Name: nil}}
	require.NoError(t, c.SetQuota(ctx, entity, map[string]float64{"producer_byte_rate": 512}))

	got, err := c.ListQuotas(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Nil(t, got[0].Entity[0].Name, "nil Name should remain nil after round-trip")
	require.InDelta(t, 512.0, got[0].Limits["producer_byte_rate"], 0.001)
}

// TestStateFileSeeding: quotas declared in the YAML state file are loaded and
// returned by ListQuotas.
func TestStateFileSeeding(t *testing.T) {
	ctx := context.Background()
	c, err := mock.FromFile("testdata/state.yaml")
	require.NoError(t, err)

	got, err := c.ListQuotas(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1, "state.yaml seeds exactly one quota entry")

	// Verify the entity components (sorted by Type: client-id < user).
	qs := got[0]
	require.Len(t, qs.Entity, 2)
	require.Equal(t, "client-id", qs.Entity[0].Type)
	require.Equal(t, strPtr("batch"), qs.Entity[0].Name)
	require.Equal(t, "user", qs.Entity[1].Type)
	require.Equal(t, strPtr("svc-checkout"), qs.Entity[1].Name)

	// Verify the limit.
	require.InDelta(t, 1048576.0, qs.Limits["producer_byte_rate"], 0.001)
}

// TestQuotaCallsRecorded: SetQuota and DeleteQuota are recorded in Calls().
func TestQuotaCallsRecorded(t *testing.T) {
	ctx := context.Background()
	c := mock.New(nil, nil)

	entity := userEntity("svc-checkout")
	require.NoError(t, c.SetQuota(ctx, entity, map[string]float64{"producer_byte_rate": 1000}))
	require.NoError(t, c.DeleteQuota(ctx, entity, []string{"producer_byte_rate"}))

	got := c.Calls()
	require.Len(t, got, 2)
	require.Equal(t, "SetQuota user=svc-checkout", got[0])
	require.Equal(t, "DeleteQuota user=svc-checkout", got[1])
}

// TestMockIPQuotaSetListDelete: ip entity type is stored, listed, and deleted
// just like any other entity type (the mock is entity-type-agnostic).
func TestMockIPQuotaSetListDelete(t *testing.T) {
	m := mock.New(nil, nil)
	ctx := context.Background()
	ip := "10.0.0.1"
	ent := []kafka.QuotaEntityComponent{{Type: "ip", Name: &ip}}
	require.NoError(t, m.SetQuota(ctx, ent, map[string]float64{"connection_creation_rate": 100}))

	qs, err := m.ListQuotas(ctx)
	require.NoError(t, err)
	var found bool
	for _, q := range qs {
		if len(q.Entity) == 1 && q.Entity[0].Type == "ip" && q.Entity[0].Name != nil && *q.Entity[0].Name == ip {
			require.InDelta(t, 100.0, q.Limits["connection_creation_rate"], 0.001)
			found = true
		}
	}
	require.True(t, found, "ip quota not listed")

	require.NoError(t, m.DeleteQuota(ctx, ent, []string{"connection_creation_rate"}))
	qs2, err := m.ListQuotas(ctx)
	require.NoError(t, err)
	for _, q := range qs2 {
		if len(q.Entity) == 1 && q.Entity[0].Type == "ip" && q.Entity[0].Name != nil && *q.Entity[0].Name == ip {
			t.Fatal("ip quota not deleted")
		}
	}
}

// TestQuotaFailOn: FailOn("SetQuota", <key>) and FailOn("DeleteQuota", <key>)
// inject failures and prevent state mutation.
func TestQuotaFailOn(t *testing.T) {
	boom := errors.New("boom")
	c := mock.New(nil, nil)
	entity := userEntity("svc-checkout")

	c.FailOn("SetQuota", "user=svc-checkout", boom)
	err := c.SetQuota(context.Background(), entity, map[string]float64{"producer_byte_rate": 1000})
	require.ErrorIs(t, err, boom)

	// Must NOT mutate state.
	got, _ := c.ListQuotas(context.Background())
	require.Empty(t, got)

	// The failed call is still recorded.
	calls := c.Calls()
	require.Len(t, calls, 1)
	require.Equal(t, "SetQuota user=svc-checkout", calls[0])

	// DeleteQuota FailOn also works.
	c2 := mock.New(nil, nil)
	require.NoError(t, c2.SetQuota(context.Background(), entity, map[string]float64{"producer_byte_rate": 1000}))

	c2.FailOn("DeleteQuota", "user=svc-checkout", boom)
	err = c2.DeleteQuota(context.Background(), entity, []string{"producer_byte_rate"})
	require.ErrorIs(t, err, boom)

	// DeleteQuota FailOn must not mutate state.
	got, _ = c2.ListQuotas(context.Background())
	require.Len(t, got, 1)
	require.InDelta(t, 1000.0, got[0].Limits["producer_byte_rate"], 0.001)
}
