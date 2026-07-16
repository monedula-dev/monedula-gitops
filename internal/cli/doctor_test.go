package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// writeDoctorMDSCluster writes a KafkaCluster manifest (plus its mock kafka
// state file) into a temp dir, pointing authorization.mds.endpoint at an MDS
// test server so the real confluent MDS client is exercised (no mock-mds-file
// annotation). Reuses testdata/clusters/dev.yaml's kafka mock-state shape so
// the earlier checks (kafka-connect/kafka-admin/acl-read) pass identically.
func writeDoctorMDSCluster(t *testing.T, mdsEndpoint string) string {
	t.Helper()
	dir := t.TempDir()
	stateYAML := `topics:
  - name: payments.orders
    partitions: 12
    config:
      retention.ms: "86400000"
acls: []
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "state.yaml"), []byte(stateYAML), 0o644))

	clusterYAML := fmt.Sprintf(`apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaCluster
metadata:
  name: mds-doctor
  annotations:
    gitops.monedula.dev/mock-state-file: state.yaml
spec:
  bootstrapServers: localhost:9092
  authorization:
    mds:
      endpoint: %s
      clusters:
        kafkaCluster: lkc-doctor01
`, mdsEndpoint)
	path := filepath.Join(dir, "cluster.yaml")
	require.NoError(t, os.WriteFile(path, []byte(clusterYAML), 0o644))
	return path
}

// TestDoctorMDSPass: an MDS server that answers GET /security/1.0/roles with
// an empty role list makes ListRoleBindings succeed with zero bindings — a
// legitimate PASS (authenticate + cheap read both worked; nobody holding a
// role in scope is not an error).
func TestDoctorMDSPass(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/security/1.0/roles" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[]`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	clusterPath := writeDoctorMDSCluster(t, srv.URL)
	out, err := run(t, "doctor", "--cluster-config-file", clusterPath)
	require.NoError(t, err)
	require.Contains(t, out, "PASS mds: 0 role binding(s)")
	require.Contains(t, out, "healthy")
}

// TestDoctorMDSFail: a 401 from the roles endpoint (bad credentials) must
// surface as a FAIL, not be swallowed as "no role bindings".
func TestDoctorMDSFail(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error_code":401,"message":"Unauthorized"}`))
	}))
	defer srv.Close()

	clusterPath := writeDoctorMDSCluster(t, srv.URL)
	out, err := run(t, "doctor", "--cluster-config-file", clusterPath)
	requireExitCode(t, err, 2)
	require.Contains(t, out, "FAIL mds:")
	require.Contains(t, out, "401")
}

// TestDoctorMDSNotConfigured: a cluster with no authorization.mds gets no
// live probe; the mds check line is shown as SKIP, mirroring how
// schema-registry behaves when unconfigured (see TestDoctorHealthyNoSchemaRegistry).
func TestDoctorMDSNotConfigured(t *testing.T) {
	out, err := run(t, "doctor", "--cluster-config-file", "testdata/clusters/dev.yaml")
	require.NoError(t, err)
	require.Contains(t, out, "SKIP mds: not configured")
}

func TestDoctorHealthyNoSchemaRegistry(t *testing.T) {
	// dev.yaml: mock-state seam, 1 topic, 1 ACL, no Schema Registry.
	out, err := run(t, "doctor", "--cluster-config-file", "testdata/clusters/dev.yaml")
	require.NoError(t, err)
	require.Contains(t, out, "PASS config:")
	require.Contains(t, out, "PASS kafka-connect:")
	require.Contains(t, out, "PASS kafka-admin: 1 topic(s)")
	require.Contains(t, out, "PASS acl-read: 1 ACL(s)")
	require.Contains(t, out, "SKIP schema-registry: not configured")
	require.Contains(t, out, "healthy")
	// Read-only contract: no secrets in the config summary.
	require.NotContains(t, out, "password")
}

func TestDoctorHealthyWithSchemaRegistry(t *testing.T) {
	// schema-cluster.yaml: mock-state + mock-schema seams, SR configured with
	// zero subjects.
	out, err := run(t, "doctor", "--cluster-config-file", "testdata/clusters/schema-cluster.yaml")
	require.NoError(t, err)
	require.Contains(t, out, "PASS kafka-admin: 1 topic(s)")
	require.Contains(t, out, "PASS schema-registry: 0 subject(s)")
}

func TestDoctorMultipleClustersWithoutSelectorFails(t *testing.T) {
	_, err := run(t, "doctor",
		"--cluster-config-file", "testdata/clusters/dev.yaml",
		"--cluster-config-file", "testdata/clusters/schema-cluster.yaml")
	requireExitCode(t, err, 2)
}

func TestDoctorClusterSelector(t *testing.T) {
	out, err := run(t, "doctor",
		"--cluster-config-file", "testdata/clusters/dev.yaml",
		"--cluster-config-file", "testdata/clusters/schema-cluster.yaml",
		"--cluster", "dev")
	require.NoError(t, err)
	require.Contains(t, out, "SKIP schema-registry: not configured")
}

func TestDoctorUnknownClusterSelectorFails(t *testing.T) {
	_, err := run(t, "doctor",
		"--cluster-config-file", "testdata/clusters/dev.yaml",
		"--cluster", "nope")
	requireExitCode(t, err, 2)
}

func TestDoctorConnectFailureReportsAndExits2(t *testing.T) {
	// The fixture's mock-state annotation points at a missing file, so the
	// client cannot be built: that is the simulated connect failure. Later
	// checks still appear (continue-on-failure) but cannot run.
	out, err := run(t, "doctor", "--cluster-config-file", "testdata/clusters/doctor-broken-cluster.yaml")
	requireExitCode(t, err, 2)
	require.Contains(t, out, "FAIL kafka-connect:")
	require.Contains(t, out, "FAIL kafka-admin:")
	require.Contains(t, out, "FAIL acl-read:")
}

func TestDoctorJSONOutput(t *testing.T) {
	out, err := run(t, "doctor", "--cluster-config-file", "testdata/clusters/dev.yaml", "-o", "json")
	require.NoError(t, err)
	var doc struct {
		Kind    string `json:"kind"`
		Cluster string `json:"cluster"`
		Checks  []struct {
			Name    string `json:"name"`
			Status  string `json:"status"`
			Message string `json:"message"`
		} `json:"checks"`
		Healthy bool `json:"healthy"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &doc))
	require.Equal(t, "DoctorOutput", doc.Kind)
	require.Equal(t, "dev", doc.Cluster)
	require.True(t, doc.Healthy)
	// Deterministic check ordering.
	names := make([]string, 0, len(doc.Checks))
	for _, c := range doc.Checks {
		names = append(names, c.Name)
	}
	require.Equal(t, []string{"config", "kafka-connect", "kafka-admin", "acl-read", "mds", "schema-registry"}, names)
}

func TestDoctorJSONOutputUnhealthy(t *testing.T) {
	out, err := run(t, "doctor",
		"--cluster-config-file", "testdata/clusters/doctor-broken-cluster.yaml", "-o", "json")
	requireExitCode(t, err, 2)
	var doc struct {
		Kind    string `json:"kind"`
		Healthy bool   `json:"healthy"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &doc))
	require.Equal(t, "DoctorOutput", doc.Kind)
	require.False(t, doc.Healthy)
}

func TestDoctorMissingClusterConfigFails(t *testing.T) {
	_, err := run(t, "doctor")
	requireExitCode(t, err, 2)
}

// TestDoctorOAuthBearerConfigPasses asserts that a cluster configured with
// mechanism: OAUTHBEARER passes the doctor config check and is no longer
// rejected as unsupported. The mock-state seam bypasses the live-broker path so
// no real token fetch or broker connection is needed.
func TestDoctorOAuthBearerConfigPasses(t *testing.T) {
	// Provide stub env vars that the oauth block references; even though the mock
	// seam bypasses buildSASL, having them set avoids any future resolver call.
	t.Setenv("OAUTH_TEST_CLIENT_ID", "test-client-id")
	t.Setenv("OAUTH_TEST_CLIENT_SECRET", "test-client-secret")

	out, err := run(t, "doctor", "--cluster-config-file", "testdata/clusters/oauth-cluster.yaml")
	require.NoError(t, err, "OAUTHBEARER cluster must no longer be rejected: %s", out)
	require.Contains(t, out, "PASS config:")
	require.Contains(t, out, "OAUTHBEARER")
	require.Contains(t, out, "PASS kafka-connect:")
	require.Contains(t, out, "healthy")
	// Security invariant: no secrets in output.
	require.NotContains(t, out, "test-client-secret")
	require.NotContains(t, out, "test-client-id")
}
