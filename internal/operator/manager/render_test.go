package manager

import (
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

// kustomizeBuild runs `kustomize build <dir>` from the repo root and returns
// the rendered YAML. Skips if the kustomize binary is absent (envtest-style).
func kustomizeBuild(t *testing.T, dir string) string {
	t.Helper()
	if _, err := exec.LookPath("kustomize"); err != nil {
		t.Skip("kustomize binary not on PATH")
	}
	cmd := exec.Command("kustomize", "build", dir)
	cmd.Dir = repoRoot(t)
	out, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "kustomize build %s:\n%s", dir, out)
	return string(out)
}

func TestKustomizeRBACBaseBuilds(t *testing.T) {
	out := kustomizeBuild(t, "config/rbac")
	require.Contains(t, out, "kind: RoleBinding")
	require.Contains(t, out, "monedula-leader-election-rolebinding")
	require.Contains(t, out, "name: monedula-leader-election-role")
	require.Contains(t, out, "name: monedula-manager")
}

func TestKustomizeDefaultTarget(t *testing.T) {
	out := kustomizeBuild(t, "config/default")
	require.NotContains(t, out, "--enable-webhooks", "default target must be webhook-free")
	require.Contains(t, out, "kind: Deployment")
	require.Contains(t, out, "kind: ClusterRole")
	require.Contains(t, out, "kind: CustomResourceDefinition")
	require.Contains(t, out, "name: monedula-manager")                      // ServiceAccount
	require.Contains(t, out, "image: ghcr.io/monedula-dev/monedula-gitops") // image transformer target
	require.Contains(t, out, "namespace: monedula-system")
}

func TestKustomizeWebhookOverlay(t *testing.T) {
	out := kustomizeBuild(t, "config/overlays/webhook")
	require.Contains(t, out, "--enable-webhooks")
	require.Contains(t, out, "kind: ValidatingWebhookConfiguration")
	require.Contains(t, out, "/validate-gitops-monedula-dev-v1alpha1-kafkatopic")
	require.Contains(t, out, "failurePolicy: Fail")
	require.Contains(t, out, "cert-manager.io/inject-ca-from: monedula-system/monedula-gitops-webhook-cert")
	require.Contains(t, out, "name: webhook-serving-cert")
	require.Contains(t, out, "kind: Certificate")
}

// helmTemplate renders the chart with the given --set flags (skip if helm absent).
func helmTemplate(t *testing.T, sets ...string) string {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm binary not on PATH")
	}
	args := []string{"template", "t", "charts/monedula-gitops", "--namespace", "monedula-system"}
	for _, s := range sets {
		args = append(args, "--set", s)
	}
	cmd := exec.Command("helm", args...)
	cmd.Dir = repoRoot(t)
	out, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "helm template:\n%s", out)
	return string(out)
}

// helmTemplateExpectError renders the chart with the given --set flags
// expecting a template-time failure, returning the combined output so callers
// can assert on the fail message. Skips if helm is absent (like helmTemplate).
func helmTemplateExpectError(t *testing.T, sets ...string) string {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm binary not on PATH")
	}
	args := []string{"template", "t", "charts/monedula-gitops", "--namespace", "monedula-system"}
	for _, s := range sets {
		args = append(args, "--set", s)
	}
	cmd := exec.Command("helm", args...)
	cmd.Dir = repoRoot(t)
	out, err := cmd.CombinedOutput()
	require.Errorf(t, err, "helm template should have failed but rendered:\n%s", out)
	return string(out)
}

// crdNames extracts CustomResourceDefinition metadata.names from a multi-doc YAML stream.
func crdNames(t *testing.T, stream string) map[string]bool {
	t.Helper()
	names := map[string]bool{}
	for _, doc := range strings.Split(stream, "\n---\n") {
		var m map[string]interface{}
		if err := yaml.Unmarshal([]byte(doc), &m); err != nil || m == nil {
			continue
		}
		if m["kind"] == "CustomResourceDefinition" {
			meta, _ := m["metadata"].(map[string]interface{})
			if n, ok := meta["name"].(string); ok {
				names[n] = true
			}
		}
	}
	return names
}

// TestHelmChartCRDsMatchConfig fails if the chart's CRDs drift from config/crd
// (someone ran `make manifests` without `make helm-sync-crds`).
func TestHelmChartCRDsMatchConfig(t *testing.T) {
	rendered := crdNames(t, helmTemplate(t))
	root := repoRoot(t)
	entries, err := filepath.Glob(filepath.Join(root, "config", "crd", "gitops.monedula.dev_*.yaml"))
	require.NoError(t, err)
	want := map[string]bool{}
	for _, f := range entries {
		b, err := os.ReadFile(f)
		require.NoError(t, err)
		for n := range crdNames(t, string(b)) {
			want[n] = true
		}
	}
	require.NotEmpty(t, want, "no CRDs found in config/crd — drift test would be vacuous")
	require.Equal(t, want, rendered, "chart CRDs drifted from config/crd — run `make helm-sync-crds`")
}

// TestHelmAndKustomizeWebhookAgree asserts the webhook config matches across the
// chart, the kustomize overlay, and the operator marker paths. All five
// webhooks — KafkaTopic, KafkaQuota, KafkaAccessPolicy, KafkaRoleBinding, and
// KafkaUser — must render in each.
func TestHelmAndKustomizeWebhookAgree(t *testing.T) {
	const topicPath = "/validate-gitops-monedula-dev-v1alpha1-kafkatopic"
	const quotaPath = "/validate-gitops-monedula-dev-v1alpha1-kafkaquota"
	const policyPath = "/validate-gitops-monedula-dev-v1alpha1-kafkaaccesspolicy"
	const roleBindingPath = "/validate-gitops-monedula-dev-v1alpha1-kafkarolebinding"
	const userPath = "/validate-gitops-monedula-dev-v1alpha1-kafkauser"
	helmOn := helmTemplate(t, "webhook.enabled=true")
	require.Contains(t, helmOn, topicPath)
	require.Contains(t, helmOn, quotaPath)
	require.Contains(t, helmOn, policyPath)
	require.Contains(t, helmOn, roleBindingPath)
	require.Contains(t, helmOn, userPath)
	require.Contains(t, helmOn, "failurePolicy: Fail")
	require.Contains(t, helmOn, "vkafkatopic.gitops.monedula.dev")
	require.Contains(t, helmOn, "vkafkaquota.gitops.monedula.dev")
	require.Contains(t, helmOn, "vkafkaaccesspolicy.gitops.monedula.dev")
	require.Contains(t, helmOn, "vkafkarolebinding.gitops.monedula.dev")
	require.Contains(t, helmOn, "vkafkauser.gitops.monedula.dev")

	kust := kustomizeBuild(t, "config/overlays/webhook")
	require.Contains(t, kust, topicPath)
	require.Contains(t, kust, quotaPath)
	require.Contains(t, kust, policyPath)
	require.Contains(t, kust, roleBindingPath)
	require.Contains(t, kust, userPath)
	require.Contains(t, kust, "vkafkatopic.gitops.monedula.dev")
	require.Contains(t, kust, "vkafkaquota.gitops.monedula.dev")
	require.Contains(t, kust, "vkafkaaccesspolicy.gitops.monedula.dev")
	require.Contains(t, kust, "vkafkarolebinding.gitops.monedula.dev")
	require.Contains(t, kust, "vkafkauser.gitops.monedula.dev")
	require.Contains(t, kust, "failurePolicy: Fail")
}

// TestHelmDefaultIsWebhookFree mirrors the kustomize default invariant.
func TestHelmDefaultIsWebhookFree(t *testing.T) {
	out := helmTemplate(t)
	require.NotContains(t, out, "--enable-webhooks")
	require.NotContains(t, out, "ValidatingWebhookConfiguration")
}

// normaliseRules converts a raw rules slice (as decoded from YAML) into a
// canonical, order-independent representation so that comparison is stable.
// Each rule becomes a sorted key of the form
//
//	"apiGroups=<sorted> resources=<sorted> verbs=<sorted>"
//
// The returned map acts as a set of rule keys.
func normaliseRules(t *testing.T, raw interface{}) map[string]struct{} {
	t.Helper()
	rules, ok := raw.([]interface{})
	require.True(t, ok, "rules field is not a sequence")
	out := make(map[string]struct{}, len(rules))
	for _, r := range rules {
		rm, ok := r.(map[string]interface{})
		require.True(t, ok, "rule entry is not a mapping")

		join := func(field string) string {
			vs, _ := rm[field].([]interface{})
			strs := make([]string, 0, len(vs))
			for _, v := range vs {
				if s, ok := v.(string); ok {
					strs = append(strs, s)
				}
			}
			sort.Strings(strs)
			return strings.Join(strs, ",")
		}
		key := "apiGroups=" + join("apiGroups") +
			" resources=" + join("resources") +
			" verbs=" + join("verbs")
		out[key] = struct{}{}
	}
	return out
}

// extractClusterRoleRules scans a multi-doc YAML stream for a ClusterRole
// whose name contains nameSuffix and returns its rules field.
func extractClusterRoleRules(t *testing.T, stream, nameSuffix string) interface{} {
	t.Helper()
	for _, doc := range strings.Split(stream, "\n---\n") {
		var m map[string]interface{}
		if err := yaml.Unmarshal([]byte(doc), &m); err != nil || m == nil {
			continue
		}
		if m["kind"] != "ClusterRole" {
			continue
		}
		meta, _ := m["metadata"].(map[string]interface{})
		name, _ := meta["name"].(string)
		if strings.HasSuffix(name, nameSuffix) {
			rules, ok := m["rules"]
			require.True(t, ok, "ClusterRole %q has no rules field", name)
			return rules
		}
	}
	t.Fatalf("ClusterRole with suffix %q not found in stream", nameSuffix)
	return nil
}

// TestHelmClusterRoleRulesMatchConfig fails if the chart's ClusterRole rules
// drift from config/rbac/role.yaml (the controller-gen source of truth).
// It guards against exactly the class of drift that kafkaquotas RBAC exposed:
// make manifests updates config/rbac/role.yaml but the chart template is
// hand-authored and must be kept in sync.
func TestHelmClusterRoleRulesMatchConfig(t *testing.T) {
	rendered := helmTemplate(t, "rbac.create=true")

	// Parse rules from the rendered chart ClusterRole (*-manager-role).
	chartRules := normaliseRules(t, extractClusterRoleRules(t, rendered, "-manager-role"))

	// Parse rules from config/rbac/role.yaml.
	root := repoRoot(t)
	b, err := os.ReadFile(filepath.Join(root, "config", "rbac", "role.yaml"))
	require.NoError(t, err)
	var configRole map[string]interface{}
	require.NoError(t, yaml.Unmarshal(b, &configRole))
	configRules := normaliseRules(t, configRole["rules"])

	require.Equal(t, configRules, chartRules,
		"chart ClusterRole rules drifted from config/rbac/role.yaml — "+
			"update charts/monedula-gitops/templates/clusterrole.yaml to match")
}

// TestHelmClusterRoleGrantsSecureMetricsRBAC pins the --metrics-secure RBAC
// delta (v0.36 Task 7): TokenReview/SubjectAccessReview create must be granted
// unconditionally (RBAC is generated at build time, before flags are known),
// on both the kustomize/config source of truth and the chart's hand-authored
// mirror.
func TestHelmClusterRoleGrantsSecureMetricsRBAC(t *testing.T) {
	root := repoRoot(t)
	b, err := os.ReadFile(filepath.Join(root, "config", "rbac", "role.yaml"))
	require.NoError(t, err)
	require.Contains(t, string(b), "tokenreviews")
	require.Contains(t, string(b), "subjectaccessreviews")

	rendered := helmTemplate(t, "rbac.create=true")
	require.Contains(t, rendered, "tokenreviews")
	require.Contains(t, rendered, "subjectaccessreviews")
}

// TestHelmDeploymentResyncAndConcurrencyFlags asserts resyncInterval and
// maxConcurrentReconciles render as flags when set and are omitted (operator
// defaults apply) when left at their empty-string default.
func TestHelmDeploymentResyncAndConcurrencyFlags(t *testing.T) {
	withValues := helmTemplate(t, "resyncInterval=2m", "maxConcurrentReconciles=1")
	require.Contains(t, withValues, "--resync-interval=2m")
	require.Contains(t, withValues, "--max-concurrent-reconciles=1")

	defaults := helmTemplate(t)
	require.NotContains(t, defaults, "--resync-interval")
	require.NotContains(t, defaults, "--max-concurrent-reconciles")
}

// TestHelmConcurrencyRequiresLeaderElection pins the chart-side mirror of the
// CLI's startup guard (v0.37): maxConcurrentReconciles > 1 with leader
// election disabled must refuse to render — the operator's in-process locking
// only makes concurrency safe with a single ACTIVE replica — while >1 with
// leader election (the default) renders both flags, and 1 without leader
// election stays valid.
func TestHelmConcurrencyRequiresLeaderElection(t *testing.T) {
	out := helmTemplateExpectError(t, "maxConcurrentReconciles=4", "leaderElection.enabled=false")
	require.Contains(t, out, "maxConcurrentReconciles > 1 requires leaderElection.enabled=true")

	// Valid combo: >1 with leader election (enabled by default) renders both
	// flags — proving the guard's promise that --leader-elect actually reaches
	// the container args alongside the concurrency flag.
	rendered := helmTemplate(t, "maxConcurrentReconciles=4")
	require.Contains(t, rendered, "--max-concurrent-reconciles=4")
	require.Contains(t, rendered, "--leader-elect")

	// Valid combo: serialized reconciles never need leader election.
	serial := helmTemplate(t, "maxConcurrentReconciles=1", "leaderElection.enabled=false")
	require.Contains(t, serial, "--max-concurrent-reconciles=1")
	require.NotContains(t, serial, "--leader-elect")
}

// TestHelmDeploymentMetricsSecureFlag asserts metrics.secure renders
// --metrics-secure when true and omits it (plain HTTP, matching every prior
// release) by default.
func TestHelmDeploymentMetricsSecureFlag(t *testing.T) {
	secure := helmTemplate(t, "metrics.secure=true")
	require.Contains(t, secure, "--metrics-secure")

	defaults := helmTemplate(t)
	require.NotContains(t, defaults, "--metrics-secure")
}

// TestHelmServiceMonitorIntervalAndRelabelings covers Item 4: interval and
// relabelings/metricRelabelings render when set and are absent by default,
// tested both ways per the task instructions.
func TestHelmServiceMonitorIntervalAndRelabelings(t *testing.T) {
	// Default: enabled but interval/relabelings unset.
	unset := helmTemplate(t, "metrics.serviceMonitor.enabled=true")
	require.Contains(t, unset, "kind: ServiceMonitor")
	require.NotContains(t, unset, "interval:")
	require.NotContains(t, unset, "relabelings:")
	require.NotContains(t, unset, "metricRelabelings:")

	// Set: interval renders verbatim; relabelings/metricRelabelings render as
	// their configured lists.
	set := helmTemplate(t,
		"metrics.serviceMonitor.enabled=true",
		"metrics.serviceMonitor.interval=30s",
		`metrics.serviceMonitor.relabelings[0].targetLabel=pod`,
		`metrics.serviceMonitor.relabelings[0].sourceLabels[0]=__meta_kubernetes_pod_name`,
		`metrics.serviceMonitor.metricRelabelings[0].action=drop`,
		`metrics.serviceMonitor.metricRelabelings[0].regex=go_.*`,
	)
	require.Contains(t, set, "interval: 30s")
	require.Contains(t, set, "relabelings:")
	require.Contains(t, set, "targetLabel: pod")
	require.Contains(t, set, "metricRelabelings:")
	require.Contains(t, set, "regex: go_.*")
}

// TestHelmServiceMonitorSecureScheme covers the metrics.secure -> ServiceMonitor
// scheme/tlsConfig wiring: https + insecureSkipVerify by default when secure,
// http (no scheme override) when not, and an explicit scheme value always wins.
func TestHelmServiceMonitorSecureScheme(t *testing.T) {
	plain := helmTemplate(t, "metrics.serviceMonitor.enabled=true")
	require.NotContains(t, plain, "scheme:")
	require.NotContains(t, plain, "tlsConfig:")

	secure := helmTemplate(t, "metrics.serviceMonitor.enabled=true", "metrics.secure=true")
	require.Contains(t, secure, "scheme: https")
	require.Contains(t, secure, "tlsConfig:")
	require.Contains(t, secure, "insecureSkipVerify: true")

	override := helmTemplate(t, "metrics.serviceMonitor.enabled=true", "metrics.serviceMonitor.scheme=https")
	require.Contains(t, override, "scheme: https")
}
