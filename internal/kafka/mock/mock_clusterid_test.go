package mock

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMockClusterID(t *testing.T) {
	c := New(nil, nil)
	_, err := c.ClusterID(context.Background())
	require.Error(t, err, "want error when unset")

	c.SetClusterID("abc")
	id, err := c.ClusterID(context.Background())
	require.NoError(t, err)
	require.Equal(t, "abc", id)
}

func TestMockClusterIDFromFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.yaml")
	require.NoError(t, os.WriteFile(path, []byte("clusterId: xyz\n"), 0o600))
	c, err := FromFile(path)
	require.NoError(t, err)
	id, err := c.ClusterID(context.Background())
	require.NoError(t, err)
	require.Equal(t, "xyz", id)
}

func TestMockClusterIDInjectedFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.yaml")
	require.NoError(t, os.WriteFile(path, []byte("clusterId: xyz\nfailures:\n  - method: ClusterID\n    error: boom\n"), 0o600))
	c, err := FromFile(path)
	require.NoError(t, err)
	_, err = c.ClusterID(context.Background())
	require.ErrorContains(t, err, "boom")
}
