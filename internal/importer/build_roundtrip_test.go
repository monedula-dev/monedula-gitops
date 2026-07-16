package importer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	"github.com/monedula-dev/monedula-gitops/internal/rbac"
)

func rtMDS() *v1alpha1.MDSConfig {
	return &v1alpha1.MDSConfig{Endpoint: "https://mds", Clusters: v1alpha1.MDSClusters{KafkaCluster: "kid"}}
}

func rtLive(principal, role, resType, resName string) mds.RoleBinding {
	rb := mds.RoleBinding{Principal: principal, Role: role, Scope: mds.Scope{Type: "kafka", KafkaCluster: "kid"}}
	if resType != "" {
		rb.Resource = &mds.ResourcePattern{Type: resType, Name: resName, PatternType: "literal"}
	}
	return rb
}

func TestRoleBindingsRoundTripCleanTrueOnFaithfulBuild(t *testing.T) {
	snap := Snapshot{
		Topics: []TopicSnapshot{{Name: "orders", Partitions: 1}},
		RoleBindings: []mds.RoleBinding{
			rtLive("User:svc", "DeveloperRead", "Topic", "orders"),
			rtLive("User:svc", "DeveloperRead", "Group", "cg"),
			rtLive("User:admin", "SystemAdmin", "", ""),
		},
	}
	r := Build(snap, "prod", []string{"rbac"}, rtMDS())
	require.True(t, roleBindingsRoundTripClean(r, snap, rtMDS()))
}

func TestRoleBindingsRoundTripCleanFalseOnMismatch(t *testing.T) {
	snap := Snapshot{
		Topics:       []TopicSnapshot{{Name: "orders", Partitions: 1}},
		RoleBindings: []mds.RoleBinding{rtLive("User:svc", "DeveloperWrite", "Topic", "orders")},
	}
	// A Result that does NOT represent the live binding (no access, no explicit RB)
	// must fail the verify — directly exercising the !rbacOK path's predicate.
	empty := Result{Topics: []*v1alpha1.KafkaTopic{{Spec: v1alpha1.KafkaTopicSpec{TopicName: "orders"}}}}
	require.False(t, roleBindingsRoundTripClean(empty, snap, rtMDS()))
}

func TestRoleBindingsRoundTripCleanFalseOnNilMDS(t *testing.T) {
	require.False(t, roleBindingsRoundTripClean(Result{}, Snapshot{RoleBindings: []mds.RoleBinding{rtLive("User:svc", "DeveloperWrite", "Topic", "orders")}}, nil))
}

// TestRoleBindingKeyLayoutInvariant pins the invariant documented on both
// rbac.RoleBinding.FullKey and mds.RoleBinding.Key: the two types must
// serialize the identical (Principal, Role, Scope, Resource) field layout to
// byte-identical strings, since roleBindingsRoundTripClean (and
// liveRBFullKey) compare compiled rbac.RoleBinding.FullKey()s against live
// mds.RoleBinding.Key()s directly. If either type's field order or separator
// ever drifts from the other, this test must fail loudly instead of the
// round-trip check silently degrading (e.g. matching nothing, or aliasing
// two distinct bindings).
func TestRoleBindingKeyLayoutInvariant(t *testing.T) {
	cases := []struct {
		name string
		rb   rbac.RoleBinding
		live mds.RoleBinding
	}{
		{
			name: "cluster-scoped, nil resource",
			rb: rbac.RoleBinding{
				Principal: "User:admin",
				Role:      "SystemAdmin",
				Scope:     rbac.Scope{Type: "kafka", KafkaCluster: "kid"},
				Resource:  nil,
			},
			live: mds.RoleBinding{
				Principal: "User:admin",
				Role:      "SystemAdmin",
				Scope:     mds.Scope{Type: "kafka", KafkaCluster: "kid"},
				Resource:  nil,
			},
		},
		{
			name: "resource-scoped, literal pattern",
			rb: rbac.RoleBinding{
				Principal: "User:svc",
				Role:      "DeveloperRead",
				Scope:     rbac.Scope{Type: "kafka", KafkaCluster: "kid"},
				Resource:  &rbac.ResourcePattern{Type: "Topic", Name: "orders", PatternType: "literal"},
			},
			live: mds.RoleBinding{
				Principal: "User:svc",
				Role:      "DeveloperRead",
				Scope:     mds.Scope{Type: "kafka", KafkaCluster: "kid"},
				Resource:  &mds.ResourcePattern{Type: "Topic", Name: "orders", PatternType: "literal"},
			},
		},
		{
			name: "resource-scoped, prefixed pattern, sub-cluster scope",
			rb: rbac.RoleBinding{
				Principal: "User:svc",
				Role:      "ResourceOwner",
				Scope:     rbac.Scope{Type: "schema-registry", KafkaCluster: "kid", SubCluster: "sr-id"},
				Resource:  &rbac.ResourcePattern{Type: "Subject", Name: "orders-", PatternType: "prefixed"},
			},
			live: mds.RoleBinding{
				Principal: "User:svc",
				Role:      "ResourceOwner",
				Scope:     mds.Scope{Type: "schema-registry", KafkaCluster: "kid", SubCluster: "sr-id"},
				Resource:  &mds.ResourcePattern{Type: "Subject", Name: "orders-", PatternType: "prefixed"},
			},
		},
		{
			// A principal containing the legacy "|" join char must not alias
			// anything under the current NUL-separated layout.
			name: "principal containing legacy separator char",
			rb: rbac.RoleBinding{
				Principal: "User:a|b",
				Role:      "DeveloperRead",
				Scope:     rbac.Scope{Type: "kafka", KafkaCluster: "kid"},
				Resource:  &rbac.ResourcePattern{Type: "Topic", Name: "orders", PatternType: "literal"},
			},
			live: mds.RoleBinding{
				Principal: "User:a|b",
				Role:      "DeveloperRead",
				Scope:     mds.Scope{Type: "kafka", KafkaCluster: "kid"},
				Resource:  &mds.ResourcePattern{Type: "Topic", Name: "orders", PatternType: "literal"},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.rb.FullKey(), tc.live.Key(),
				"rbac.RoleBinding.FullKey() and mds.RoleBinding.Key() must be byte-identical for the same logical binding")
		})
	}
}
