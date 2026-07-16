package manager

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

// repoRoot returns the repository root, computed once from this file's path.
// The test is in internal/operator/manager/ so the repo root is three levels up.
func repoRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok, "runtime.Caller failed")
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..")
}

// TestManifestsParseAsYAML verifies that every YAML file in the deploy config
// directories (config/webhook/, config/certmanager/, config/manager/,
// config/crd/, config/rbac/) parses as valid YAML without errors. This is a
// static check: no cluster is required.
func TestManifestsParseAsYAML(t *testing.T) {
	root := repoRoot(t)
	dirs := []string{
		filepath.Join(root, "config", "webhook"),
		filepath.Join(root, "config", "certmanager"),
		filepath.Join(root, "config", "manager"),
		filepath.Join(root, "config", "crd"),
		filepath.Join(root, "config", "rbac"),
	}

	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		require.NoErrorf(t, err, "reading directory %s", dir)
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
				continue
			}
			path := filepath.Join(dir, entry.Name())
			t.Run(strings.TrimPrefix(path, root+"/"), func(t *testing.T) {
				data, err := os.ReadFile(path)
				require.NoErrorf(t, err, "reading %s", path)

				// sigs.k8s.io/yaml handles multi-document YAML (---) by parsing
				// each document. We split manually and validate each document.
				docs := strings.Split(string(data), "\n---")
				for i, doc := range docs {
					doc = strings.TrimSpace(doc)
					if doc == "" {
						continue
					}
					var out interface{}
					err := yaml.Unmarshal([]byte(doc), &out)
					assert.NoErrorf(t, err, "%s: document %d failed to parse", path, i)
				}
			})
		}
	}
}

// TestWebhookManifestContent verifies that the generated
// config/webhook/manifests.yaml contains the values required by the
// +kubebuilder:webhook marker on KafkaTopicValidator: correct path,
// failurePolicy, sideEffects, resource rules, and the cert-manager annotation.
func TestWebhookManifestContent(t *testing.T) {
	root := repoRoot(t)
	path := filepath.Join(root, "config", "webhook", "manifests.yaml")
	data, err := os.ReadFile(path)
	require.NoError(t, err, "reading config/webhook/manifests.yaml")
	content := string(data)

	checks := []struct {
		name    string
		present string
	}{
		{
			name:    "webhook path",
			present: "/validate-gitops-monedula-dev-v1alpha1-kafkatopic",
		},
		{
			name:    "failurePolicy Fail",
			present: "failurePolicy: Fail",
		},
		{
			name:    "sideEffects None",
			present: "sideEffects: None",
		},
		{
			// Match the indented list entry as emitted by controller-gen/patch script:
			// four spaces + "- CREATE" on its own line, nested under `operations:`.
			name:    "CREATE operation",
			present: "    - CREATE\n",
		},
		{
			name:    "UPDATE operation",
			present: "    - UPDATE\n",
		},
		{
			name:    "kafkatopics resource",
			present: "    - kafkatopics\n",
		},
		{
			name:    "v1alpha1 version",
			present: "    - v1alpha1\n",
		},
		{
			name:    "webhook name",
			present: "name: vkafkatopic.gitops.monedula.dev",
		},
		{
			name:    "service name",
			present: "name: monedula-gitops-webhook-service",
		},
		{
			name:    "service namespace monedula-system",
			present: "namespace: monedula-system",
		},
		{
			name:    "cert-manager CA injection annotation",
			present: "cert-manager.io/inject-ca-from: monedula-system/monedula-gitops-webhook-cert",
		},
	}

	for _, tc := range checks {
		t.Run(tc.name, func(t *testing.T) {
			assert.Contains(t, content, tc.present,
				"config/webhook/manifests.yaml should contain %q", tc.present)
		})
	}
}

// TestCertManagerManifestContent verifies that the cert-manager Certificate
// resource references the correct secret name and DNS SANs for the webhook
// Service.
func TestCertManagerManifestContent(t *testing.T) {
	root := repoRoot(t)
	certPath := filepath.Join(root, "config", "certmanager", "certificate.yaml")
	data, err := os.ReadFile(certPath)
	require.NoError(t, err, "reading config/certmanager/certificate.yaml")
	content := string(data)

	checks := []struct {
		name    string
		present string
	}{
		{
			name:    "secretName",
			present: "secretName: monedula-gitops-webhook-serving-cert",
		},
		{
			name:    "SAN short form",
			present: "monedula-gitops-webhook-service.monedula-system.svc",
		},
		{
			name:    "SAN FQDN",
			present: "monedula-gitops-webhook-service.monedula-system.svc.cluster.local",
		},
		{
			name:    "certificate name matches inject-ca-from",
			present: "name: monedula-gitops-webhook-cert",
		},
		{
			name:    "namespace monedula-system",
			present: "namespace: monedula-system",
		},
		{
			name:    "issuer ref",
			present: "name: monedula-gitops-selfsigned-issuer",
		},
	}

	for _, tc := range checks {
		t.Run(tc.name, func(t *testing.T) {
			assert.Contains(t, content, tc.present,
				"config/certmanager/certificate.yaml should contain %q", tc.present)
		})
	}
}

func TestManagerDeploymentBaseIsWebhookFree(t *testing.T) {
	root := repoRoot(t)
	b, err := os.ReadFile(filepath.Join(root, "config", "manager", "manager.yaml"))
	require.NoError(t, err)
	s := string(b)
	require.NotContains(t, s, "--enable-webhooks", "base manager must be webhook-free; webhook lives in the overlay")
	require.NotContains(t, s, "webhook-serving-cert", "base manager must not mount the webhook cert")
	require.Contains(t, s, "--leader-elect")
	require.Contains(t, s, "--metrics-bind-address")
}
