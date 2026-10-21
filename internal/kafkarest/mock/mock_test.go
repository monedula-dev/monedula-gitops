package mock

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/kafkarest"
)

func writeState(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "rest.yaml")
	require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
	return p
}

func TestMockFromFile(t *testing.T) {
	p := writeState(t, `
clusterId: dest-1
links:
  - name: link-a
    state: ACTIVE
    configs:
      topic.config.sync.include: "retention.ms"
mirrors:
  - linkName: link-a
    mirrorTopicName: zeta
    sourceTopicName: zeta
    status: ACTIVE
    numPartitions: 3
  - linkName: link-a
    mirrorTopicName: alpha
    sourceTopicName: alpha
    status: PAUSED
`)
	c, err := FromFile(p)
	require.NoError(t, err)
	ctx := context.Background()

	id, err := c.ClusterID(ctx)
	require.NoError(t, err)
	require.Equal(t, "dest-1", id)

	ms, err := c.ListMirrors(ctx)
	require.NoError(t, err)
	require.Len(t, ms, 2)
	require.Equal(t, "alpha", ms[0].MirrorTopicName)
	require.Equal(t, 3, ms[1].NumPartitions)

	ls, err := c.ListLinks(ctx)
	require.NoError(t, err)
	require.Equal(t, []kafkarest.Link{{Name: "link-a", State: "ACTIVE"}}, ls)

	cfg, err := c.LinkConfigs(ctx, "link-a")
	require.NoError(t, err)
	require.Equal(t, "retention.ms", cfg["topic.config.sync.include"])
	cfg["x"] = "y" // copy: must not leak into state
	cfg2, _ := c.LinkConfigs(ctx, "link-a")
	require.NotContains(t, cfg2, "x")

	_, err = c.LinkConfigs(ctx, "nope")
	require.ErrorIs(t, err, kafkarest.ErrNotFound)
}

func TestMockFromFileErrors(t *testing.T) {
	_, err := FromFile(filepath.Join(t.TempDir(), "missing.yaml"))
	require.Error(t, err)
	_, err = FromFile(writeState(t, "links: [[["))
	require.Error(t, err)
	_, err = FromFile(writeState(t, "failures:\n  - method: ListMirrors\n"))
	require.Error(t, err)
}

func TestMockClusterIDUnsetFailsClosed(t *testing.T) {
	_, err := New("").ClusterID(context.Background())
	require.Error(t, err)
}

func TestMockFailureInjection(t *testing.T) {
	p := writeState(t, `
clusterId: c
failures:
  - method: ListMirrors
    error: boom
`)
	c, err := FromFile(p)
	require.NoError(t, err)
	_, err = c.ListMirrors(context.Background())
	require.EqualError(t, err, "boom")

	c.Fail("CreateMirror", errors.New("nope"))
	require.EqualError(t, c.CreateMirror(context.Background(), kafkarest.CreateMirrorRequest{MirrorTopicName: "t"}), "nope")
	require.Empty(t, c.Created())
}

func TestMockCreateMirrorRecorded(t *testing.T) {
	c := New("c")
	ctx := context.Background()
	req := kafkarest.CreateMirrorRequest{LinkName: "l", SourceTopicName: "s", MirrorTopicName: "m", Configs: map[string]string{"a": "b"}}
	require.NoError(t, c.CreateMirror(ctx, req))
	require.Equal(t, []kafkarest.CreateMirrorRequest{req}, c.Created())

	ms, err := c.ListMirrors(ctx)
	require.NoError(t, err)
	require.Equal(t, []kafkarest.Mirror{{LinkName: "l", MirrorTopicName: "m", SourceTopicName: "s", Status: "ACTIVE"}}, ms)

	require.NoError(t, c.AlterTopicConfigs(ctx, "m", map[string]string{"k": "v"}))
	require.Equal(t, map[string]map[string]string{"m": {"k": "v"}}, c.Altered())
}
