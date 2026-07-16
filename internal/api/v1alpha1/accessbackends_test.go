package v1alpha1

import (
	"reflect"
	"testing"
)

func clusterWithBackends(backends ...string) *KafkaCluster {
	c := &KafkaCluster{}
	if backends != nil {
		c.Spec.Authorization = &AuthorizationConfig{AccessBackends: backends}
	}
	return c
}

func TestEffectiveAccessBackends(t *testing.T) {
	cases := []struct {
		name string
		in   *KafkaCluster
		want []string
	}{
		{"nil cluster", nil, []string{"acl"}},
		{"nil authorization", &KafkaCluster{}, []string{"acl"}},
		{"nil slice", clusterWithBackends(), []string{"acl"}},
		{"explicit acl", clusterWithBackends("acl"), []string{"acl"}},
		{"rbac only", clusterWithBackends("rbac"), []string{"rbac"}},
		{"both", clusterWithBackends("acl", "rbac"), []string{"acl", "rbac"}},
		{"dedup preserves first-seen order", clusterWithBackends("rbac", "acl", "rbac"), []string{"rbac", "acl"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := EffectiveAccessBackends(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("EffectiveAccessBackends = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHasAccessBackend(t *testing.T) {
	if !HasAccessBackend(clusterWithBackends(), "acl") {
		t.Fatal("default cluster must have acl backend")
	}
	if HasAccessBackend(clusterWithBackends(), "rbac") {
		t.Fatal("default cluster must not have rbac backend")
	}
	if !HasAccessBackend(clusterWithBackends("acl", "rbac"), "rbac") {
		t.Fatal("[acl,rbac] must have rbac backend")
	}
	if HasAccessBackend(clusterWithBackends("rbac"), "acl") {
		t.Fatal("[rbac] must not have acl backend")
	}
}
