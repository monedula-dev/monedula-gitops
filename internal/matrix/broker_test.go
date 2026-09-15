//go:build integration

package matrix

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// StartBroker must produce a broker that answers a real Kafka API call, for
// whichever cell the environment selects — that is the whole point of the
// family dispatch.
func TestStartBrokerServesMetadata(t *testing.T) {
	SkipUnlessDocker(t)

	cell, err := FromEnv()
	require.NoError(t, err)
	t.Logf("cell %s (%s, %s)", cell.ID, cell.Family, cell.Image)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	broker, err := StartBroker(ctx, cell)
	if err != nil {
		if IsDockerUnavailable(err) {
			t.Skipf("Docker not available, skipping integration test: %v", err)
		}
		t.Fatalf("starting broker for cell %s: %v", cell.ID, err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(broker); err != nil {
			t.Logf("terminating broker: %v", err)
		}
	})

	require.NotEmpty(t, broker.Bootstrap)

	client, err := kgo.NewClient(kgo.SeedBrokers(broker.Bootstrap...))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	admin := kadm.NewClient(client)
	md, err := admin.Metadata(ctx)
	require.NoError(t, err, "fetching metadata from cell %s", cell.ID)
	assert.NotEmpty(t, md.Brokers, "cell %s reported no brokers", cell.ID)
}
