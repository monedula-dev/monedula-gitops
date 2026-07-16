package mock

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMockLoadsStateAndReadsBack(t *testing.T) {
	c, err := FromFile("testdata/state.yaml")
	require.NoError(t, err)
	tp, err := c.GetTopic(context.Background(), "payments.orders")
	require.NoError(t, err)
	require.NotNil(t, tp)
	require.Equal(t, 12, tp.Partitions)
	require.Equal(t, "86400000", tp.Config["retention.ms"])
	acls, err := c.ListACLs(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, acls)
}

func TestMockGetTopicMissingReturnsNil(t *testing.T) {
	c, err := FromFile("testdata/state.yaml")
	require.NoError(t, err)
	tp, err := c.GetTopic(context.Background(), "does.not.exist")
	require.NoError(t, err)
	require.Nil(t, tp)
}

// TestReadFailureInjection: FailOn with an empty target makes the target-less
// read methods fail, both programmatically and via the fixture's `failures`
// list. Reads stay absent from Calls() (mutations-only), and unaffected reads
// keep working.
func TestReadFailureInjection(t *testing.T) {
	ctx := context.Background()

	c, err := FromFile("testdata/state.yaml")
	require.NoError(t, err)
	c.FailOn("ListQuotas", "", context.DeadlineExceeded)
	_, err = c.ListQuotas(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	_, err = c.ListTopics(ctx) // other reads unaffected
	require.NoError(t, err)
	require.Empty(t, c.Calls(), "reads must not be recorded as calls")

	// Fixture-seeded failures reach FailOn through FromFile.
	f, err := FromFile("testdata/failing-state.yaml")
	require.NoError(t, err)
	_, err = f.ListACLs(ctx)
	require.ErrorContains(t, err, "CLUSTER_AUTHORIZATION_FAILED")
	_, err = f.ListTopics(ctx)
	require.NoError(t, err)
}

func TestMockListIsDeterministic(t *testing.T) {
	c, err := FromFile("testdata/state.yaml")
	require.NoError(t, err)
	a, err := c.ListTopics(context.Background())
	require.NoError(t, err)
	b, err := c.ListTopics(context.Background())
	require.NoError(t, err)
	require.Equal(t, a, b) // stable order across calls
	// verify sorted by name
	for i := 1; i < len(a); i++ {
		require.LessOrEqual(t, a[i-1].Name, a[i].Name)
	}
}
