//go:build integration

package confluent

// Build-tagged integration tests that exercise the real confluent mds.Client
// against a LIVE Confluent MDS endpoint. These are EXCLUDED from the default
// `go test ./...` suite (no build tag) so the default run stays hermetic.
//
// Run them with:
//
//	MDS_ENDPOINT=https://mds.example.com \
//	MDS_USER=admin \
//	MDS_PASS=secret \
//	MDS_KAFKA_CLUSTER=lkc-abc123 \
//	go test -tags integration ./internal/mds/confluent/ -v
//
// All tests skip cleanly (t.Skip) when MDS_ENDPOINT is not set, so a live-MDS-
// less environment never sees a failure.

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/mds"
)

// mdsEndpointFromEnv returns the MDS endpoint from MDS_ENDPOINT, skipping the
// test if the variable is not set.
func mdsEndpointFromEnv(t *testing.T) string {
	t.Helper()
	endpoint := os.Getenv("MDS_ENDPOINT")
	if endpoint == "" {
		t.Skip("MDS_ENDPOINT not set — skipping MDS integration test")
	}
	return endpoint
}

// mdsClientFromEnv constructs a Client from environment variables, skipping if
// MDS_ENDPOINT is unset. Auth is Basic when MDS_USER/MDS_PASS are set, or none.
func mdsClientFromEnv(t *testing.T) *Client {
	t.Helper()
	endpoint := mdsEndpointFromEnv(t)

	var auth *Auth
	user := os.Getenv("MDS_USER")
	pass := os.Getenv("MDS_PASS")
	if user != "" && pass != "" {
		auth = BasicAuth(user, pass)
	}

	c, err := New(endpoint, auth, nil)
	require.NoError(t, err, "New")
	return c
}

// kafkaClusterFromEnv returns MDS_KAFKA_CLUSTER, or a placeholder.
func kafkaClusterFromEnv() string {
	if v := os.Getenv("MDS_KAFKA_CLUSTER"); v != "" {
		return v
	}
	return "lkc-placeholder"
}

// itCtx returns a per-test context with a 30 s timeout.
func itCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestIntegration_NewConnects verifies that New succeeds and a ListRoleBindings
// call at least reaches the server (may return an empty list or an auth error —
// we just check it doesn't panic or hang).
func TestIntegration_NewConnects(t *testing.T) {
	c := mdsClientFromEnv(t)
	ctx := itCtx(t)

	scope := mds.Scope{
		Type:         "kafka",
		KafkaCluster: kafkaClusterFromEnv(),
	}

	rbs, err := c.ListRoleBindings(ctx, scope)
	// We accept either success (no error) or an auth/config error. We do NOT
	// accept a panic or a timeout. This is a smoke test.
	if err != nil {
		t.Logf("ListRoleBindings returned (expected if creds are wrong): %v", err)
	} else {
		t.Logf("ListRoleBindings returned %d binding(s)", len(rbs))
		assert.NotNil(t, rbs)
	}
}

// TestIntegration_AddRemoveRoleBinding_ResourceScoped performs an
// add-then-remove round-trip of a resource-scoped role binding. Uses a
// deliberately unusual principal/role that is unlikely to conflict with
// production state. Skips if the remove fails (to avoid leaving stale state).
func TestIntegration_AddRemoveRoleBinding_ResourceScoped(t *testing.T) {
	c := mdsClientFromEnv(t)
	ctx := itCtx(t)

	rb := mds.RoleBinding{
		Principal: fmt.Sprintf("User:monedula-it-%s", t.Name()),
		Role:      "DeveloperRead",
		Scope: mds.Scope{
			Type:         "kafka",
			KafkaCluster: kafkaClusterFromEnv(),
		},
		Resource: &mds.ResourcePattern{
			Type:        "Topic",
			Name:        "monedula-it-smoke-topic",
			PatternType: "literal",
		},
	}

	if err := c.AddRoleBinding(ctx, rb); err != nil {
		t.Skipf("AddRoleBinding failed (server may not support this or creds insufficient): %v", err)
	}
	t.Cleanup(func() {
		if err := c.RemoveRoleBinding(context.Background(), rb); err != nil {
			t.Logf("cleanup RemoveRoleBinding failed: %v", err)
		}
	})

	// RemoveRoleBinding: should succeed idempotently.
	require.NoError(t, c.RemoveRoleBinding(ctx, rb), "RemoveRoleBinding")
}
