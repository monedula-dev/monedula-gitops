package importer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
)

func TestImportRoleBindingRoundTrip(t *testing.T) {
	mdsCfg := &v1alpha1.MDSConfig{Endpoint: "https://mds", Clusters: v1alpha1.MDSClusters{KafkaCluster: "kid"}}
	rb := func(principal, role, resType, resName string) mds.RoleBinding {
		b := mds.RoleBinding{Principal: principal, Role: role, Scope: mds.Scope{Type: "kafka", KafkaCluster: "kid"}}
		if resType != "" {
			b.Resource = &mds.ResourcePattern{Type: resType, Name: resName, PatternType: "literal"}
		}
		return b
	}

	// topicMap builds a name→topic index from a Result, for convenient lookup in check funcs.
	topicMap := func(r Result) map[string]*v1alpha1.KafkaTopic {
		m := make(map[string]*v1alpha1.KafkaTopic, len(r.Topics))
		for _, tp := range r.Topics {
			m[tp.Name] = tp
		}
		return m
	}

	cases := []struct {
		name     string
		backends []string
		rbs      []mds.RoleBinding
		check    func(t *testing.T, r Result)
	}{
		{
			name:     "producer",
			backends: []string{"rbac"},
			rbs:      []mds.RoleBinding{rb("User:svc", "DeveloperWrite", "Topic", "orders")},
			// DeveloperWrite/Topic/orders folds: no explicit KafkaRoleBinding, producer
			// entry is injected directly into the orders topic's Access.Producers.
			check: func(t *testing.T, r Result) {
				t.Helper()
				require.Empty(t, r.RoleBindings, "producer fold: RoleBindings must be empty")
				tm := topicMap(r)
				orders := tm["orders"]
				require.NotNil(t, orders, "orders topic must be present")
				require.Len(t, orders.Spec.Access.Producers, 1, "orders must have exactly 1 producer entry")
				require.Equal(t, "User:svc", orders.Spec.Access.Producers[0].Principal)
				require.Empty(t, orders.Spec.Access.Consumers, "no consumer entries expected")
			},
		},
		{
			name:     "consumer",
			backends: []string{"rbac"},
			rbs: []mds.RoleBinding{
				rb("User:svc", "DeveloperRead", "Topic", "orders"),
				rb("User:svc", "DeveloperRead", "Group", "cg"),
			},
			// 1 topic-read + 1 group-read → unambiguous consumer fold: no explicit
			// KafkaRoleBinding, consumer entry injected into orders topic.
			check: func(t *testing.T, r Result) {
				t.Helper()
				require.Empty(t, r.RoleBindings, "consumer fold: RoleBindings must be empty")
				tm := topicMap(r)
				orders := tm["orders"]
				require.NotNil(t, orders, "orders topic must be present")
				require.Len(t, orders.Spec.Access.Consumers, 1, "orders must have exactly 1 consumer entry")
				require.Equal(t, "User:svc", orders.Spec.Access.Consumers[0].Principal)
				require.Equal(t, "cg", orders.Spec.Access.Consumers[0].Group)
				require.Empty(t, orders.Spec.Access.Producers, "no producer entries expected")
			},
		},
		{
			name:     "cluster-scoped",
			backends: []string{"rbac"},
			rbs:      []mds.RoleBinding{rb("User:a", "SystemAdmin", "", "")},
			// Cluster-scoped bindings (nil Resource) are never eligible for folding:
			// they go directly to leftover → explicit KafkaRoleBinding.
			check: func(t *testing.T, r Result) {
				t.Helper()
				require.Len(t, r.RoleBindings, 1, "cluster-scoped: must have exactly 1 explicit KafkaRoleBinding")
				require.Equal(t, "SystemAdmin", r.RoleBindings[0].Spec.Role)
				tm := topicMap(r)
				orders := tm["orders"]
				require.NotNil(t, orders, "orders topic must be present")
				require.Empty(t, orders.Spec.Access.Producers, "no producer entries expected")
				require.Empty(t, orders.Spec.Access.Consumers, "no consumer entries expected")
			},
		},
		{
			name:     "resource-owner",
			backends: []string{"rbac"},
			rbs:      []mds.RoleBinding{rb("User:b", "ResourceOwner", "Topic", "orders")},
			// ResourceOwner is resource-scoped but is not a DeveloperWrite/DeveloperRead
			// producer or consumer pattern, so foldRoleBindings routes it to leftover
			// via the default case → explicit KafkaRoleBinding; no topic access mutated.
			check: func(t *testing.T, r Result) {
				t.Helper()
				require.Len(t, r.RoleBindings, 1, "resource-owner: must have exactly 1 explicit KafkaRoleBinding")
				require.Equal(t, "ResourceOwner", r.RoleBindings[0].Spec.Role)
				tm := topicMap(r)
				orders := tm["orders"]
				require.NotNil(t, orders, "orders topic must be present")
				require.Empty(t, orders.Spec.Access.Producers, "no producer entries expected")
				require.Empty(t, orders.Spec.Access.Consumers, "no consumer entries expected")
			},
		},
		{
			name:     "ambiguous",
			backends: []string{"rbac"},
			rbs: []mds.RoleBinding{
				rb("User:svc", "DeveloperRead", "Topic", "orders"),
				rb("User:svc", "DeveloperRead", "Topic", "payments"),
				rb("User:svc", "DeveloperRead", "Group", "g1"),
				rb("User:svc", "DeveloperRead", "Group", "g2"),
			},
			// 2 topic-reads + 2 group-reads: cannot determine a unique (topic,group)
			// consumer pairing → foldRoleBindings hits the default branch and emits
			// all 4 as leftover → 4 explicit KafkaRoleBindings; no consumer folded.
			check: func(t *testing.T, r Result) {
				t.Helper()
				require.Len(t, r.RoleBindings, 4, "ambiguous: must have 4 explicit KafkaRoleBindings")
				tm := topicMap(r)
				orders := tm["orders"]
				require.NotNil(t, orders, "orders topic must be present")
				require.Empty(t, orders.Spec.Access.Consumers, "orders must have no folded consumers")
				payments := tm["payments"]
				require.NotNil(t, payments, "payments topic must be present")
				require.Empty(t, payments.Spec.Access.Consumers, "payments must have no folded consumers")
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snap := Snapshot{Topics: []TopicSnapshot{{Name: "orders", Partitions: 1}, {Name: "payments", Partitions: 1}}, RoleBindings: tc.rbs}
			r := Build(snap, "prod", tc.backends, mdsCfg)
			require.True(t, roleBindingsRoundTripClean(r, snap, mdsCfg), "imported manifests must reproduce the live role-binding set")
			tc.check(t, r)
		})
	}
}
