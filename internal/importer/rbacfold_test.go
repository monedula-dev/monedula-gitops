package importer

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
)

func foldTopic(name string) *v1alpha1.KafkaTopic {
	return &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1alpha1.KafkaTopicSpec{TopicName: name},
	}
}
func foldKafkaScope() mds.Scope { return mds.Scope{Type: "kafka", KafkaCluster: "kid"} }
func foldTopicRB(principal, role, topic string) mds.RoleBinding {
	return mds.RoleBinding{Principal: principal, Role: role, Scope: foldKafkaScope(),
		Resource: &mds.ResourcePattern{Type: "Topic", Name: topic, PatternType: "literal"}}
}
func foldGroupRB(principal, group string) mds.RoleBinding {
	return mds.RoleBinding{Principal: principal, Role: "DeveloperRead", Scope: foldKafkaScope(),
		Resource: &mds.ResourcePattern{Type: "Group", Name: group, PatternType: "literal"}}
}

func TestRBACFoldProducer(t *testing.T) {
	orders := foldTopic("orders")
	byName := map[string]*v1alpha1.KafkaTopic{"orders": orders}
	leftover := foldRoleBindings([]mds.RoleBinding{foldTopicRB("User:svc", "DeveloperWrite", "orders")}, byName)
	require.Empty(t, leftover)
	require.Len(t, orders.Spec.Access.Producers, 1)
	require.Equal(t, "User:svc", orders.Spec.Access.Producers[0].Principal)
}

func TestRBACFoldConsumerUnambiguous(t *testing.T) {
	orders := foldTopic("orders")
	byName := map[string]*v1alpha1.KafkaTopic{"orders": orders}
	leftover := foldRoleBindings([]mds.RoleBinding{
		foldTopicRB("User:svc", "DeveloperRead", "orders"),
		foldGroupRB("User:svc", "cg"),
	}, byName)
	require.Empty(t, leftover)
	require.Len(t, orders.Spec.Access.Consumers, 1)
	require.Equal(t, "cg", orders.Spec.Access.Consumers[0].Group)
	require.Equal(t, "User:svc", orders.Spec.Access.Consumers[0].Principal)
}

func TestRBACFoldAmbiguousConsumerStaysExplicit(t *testing.T) {
	orders, payments := foldTopic("orders"), foldTopic("payments")
	byName := map[string]*v1alpha1.KafkaTopic{"orders": orders, "payments": payments}
	leftover := foldRoleBindings([]mds.RoleBinding{
		foldTopicRB("User:svc", "DeveloperRead", "orders"),
		foldTopicRB("User:svc", "DeveloperRead", "payments"),
		foldGroupRB("User:svc", "cg1"),
		foldGroupRB("User:svc", "cg2"),
	}, byName)
	require.Len(t, leftover, 4)
	require.Empty(t, orders.Spec.Access.Consumers)
	require.Empty(t, payments.Spec.Access.Consumers)
}

func TestRBACFoldOtherRolesStayExplicit(t *testing.T) {
	byName := map[string]*v1alpha1.KafkaTopic{"orders": foldTopic("orders")}
	in := []mds.RoleBinding{
		{Principal: "User:a", Role: "SystemAdmin", Scope: foldKafkaScope()}, // cluster-scoped
		foldTopicRB("User:b", "ResourceOwner", "orders"),                    // resource owner
		{Principal: "User:c", Role: "DeveloperRead",
			Scope:    mds.Scope{Type: "schema-registry", KafkaCluster: "kid", SubCluster: "sr"},
			Resource: &mds.ResourcePattern{Type: "Subject", Name: "s", PatternType: "literal"}}, // SR scope
	}
	leftover := foldRoleBindings(in, byName)
	require.Len(t, leftover, 3)
	keys := map[string]bool{}
	for _, lr := range leftover {
		keys[lr.Key()] = true
	}
	require.True(t, keys[mds.RoleBinding{Principal: "User:a", Role: "SystemAdmin", Scope: foldKafkaScope()}.Key()])
	require.True(t, keys[foldTopicRB("User:b", "ResourceOwner", "orders").Key()])
	require.True(t, keys[mds.RoleBinding{Principal: "User:c", Role: "DeveloperRead", Scope: mds.Scope{Type: "schema-registry", KafkaCluster: "kid", SubCluster: "sr"}, Resource: &mds.ResourcePattern{Type: "Subject", Name: "s", PatternType: "literal"}}.Key()])
}

func TestRBACFoldProducerOnNonImportedTopicStaysExplicit(t *testing.T) {
	byName := map[string]*v1alpha1.KafkaTopic{} // no topics imported
	leftover := foldRoleBindings([]mds.RoleBinding{foldTopicRB("User:svc", "DeveloperWrite", "ghost")}, byName)
	require.Len(t, leftover, 1)
}

func TestRBACFoldPrefixedPatternStaysExplicit(t *testing.T) {
	orders := foldTopic("orders")
	byName := map[string]*v1alpha1.KafkaTopic{"orders": orders}
	prefixed := mds.RoleBinding{Principal: "User:svc", Role: "DeveloperWrite", Scope: foldKafkaScope(),
		Resource: &mds.ResourcePattern{Type: "Topic", Name: "team-", PatternType: "prefixed"}}
	leftover := foldRoleBindings([]mds.RoleBinding{prefixed}, byName)
	require.Len(t, leftover, 1)
	require.Empty(t, orders.Spec.Access.Producers)
}

func TestRBACFoldSkipsEntryAlreadyInAccess(t *testing.T) {
	orders := foldTopic("orders")
	orders.Spec.Access.Producers = []v1alpha1.ProducerAccess{{Principal: "User:svc"}} // already added by ACL fold
	byName := map[string]*v1alpha1.KafkaTopic{"orders": orders}
	leftover := foldRoleBindings([]mds.RoleBinding{foldTopicRB("User:svc", "DeveloperWrite", "orders")}, byName)
	require.Empty(t, leftover)                      // covered (don't emit explicit)
	require.Len(t, orders.Spec.Access.Producers, 1) // not duplicated
}

func TestRBACFoldUnpairedConsumerTopicStaysExplicit(t *testing.T) {
	orders := foldTopic("orders")
	byName := map[string]*v1alpha1.KafkaTopic{"orders": orders}
	// DeveloperRead on topic with NO group read → can't form a consumer → explicit
	leftover := foldRoleBindings([]mds.RoleBinding{foldTopicRB("User:svc", "DeveloperRead", "orders")}, byName)
	require.Len(t, leftover, 1)
	require.Empty(t, orders.Spec.Access.Consumers)
}

func TestRBACFoldUnpairedGroupReadStaysExplicit(t *testing.T) {
	orders := foldTopic("orders")
	byName := map[string]*v1alpha1.KafkaTopic{"orders": orders}
	leftover := foldRoleBindings([]mds.RoleBinding{foldGroupRB("User:svc", "cg")}, byName)
	require.Len(t, leftover, 1)
	require.Empty(t, orders.Spec.Access.Consumers)
}
