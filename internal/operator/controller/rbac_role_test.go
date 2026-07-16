package controller

import (
	"os"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

// roleYAMLPath is the generated ClusterRole (make manifests), relative to this
// package — same layout convention as crdDir in the envtest suite.
const roleYAMLPath = "../../../config/rbac/role.yaml"

// TestGeneratedRoleGrantsRequiredVerbs keeps the regenerated RBAC honest
// (review I8): the operator reads Secrets through the manager's CACHED client,
// so its informer needs list+watch (get;list alone yields persistent Forbidden
// in a real cluster — envtest runs as admin and hides it), and Recorder.Eventf
// needs cluster-scoped events.k8s.io events create/patch for CRs outside the
// operator's own namespace. If a marker edit drops these, this test fails
// until `make manifests` is re-run with correct markers.
func TestGeneratedRoleGrantsRequiredVerbs(t *testing.T) {
	data, err := os.ReadFile(roleYAMLPath)
	if err != nil {
		t.Fatalf("reading generated role: %v", err)
	}
	var role rbacv1.ClusterRole
	if err := yaml.Unmarshal(data, &role); err != nil {
		t.Fatalf("parsing %s: %v", roleYAMLPath, err)
	}

	for _, want := range []struct {
		group    string
		resource string
		verbs    []string
	}{
		{group: "", resource: "secrets", verbs: []string{"get", "list", "watch"}},
		{group: "", resource: "configmaps", verbs: []string{"get", "list", "watch"}},
		{group: "events.k8s.io", resource: "events", verbs: []string{"create", "patch"}},
	} {
		if missing := missingVerbs(role.Rules, want.group, want.resource, want.verbs); len(missing) > 0 {
			t.Errorf("generated role is missing %q %q verbs %v (have rules: %+v)", want.group, want.resource, missing, role.Rules)
		}
	}
}

// missingVerbs returns the subset of verbs NOT granted on the group's resource
// by any rule.
func missingVerbs(rules []rbacv1.PolicyRule, group, resource string, verbs []string) []string {
	granted := map[string]bool{}
	for _, rule := range rules {
		if !contains(rule.APIGroups, group) || !contains(rule.Resources, resource) {
			continue
		}
		for _, v := range rule.Verbs {
			granted[v] = true
		}
	}
	var missing []string
	for _, v := range verbs {
		if !granted[v] && !granted["*"] {
			missing = append(missing, v)
		}
	}
	return missing
}

func contains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s || v == "*" {
			return true
		}
	}
	return false
}
