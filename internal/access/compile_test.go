package access

import (
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestProducerCompilesToWriteDescribe(t *testing.T) {
	tp := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "orders"},
		Spec: v1alpha1.KafkaTopicSpec{
			TopicName: "payments.orders",
			Access:    v1alpha1.TopicAccess{Producers: []v1alpha1.ProducerAccess{{Principal: "User:svc-checkout"}}},
		},
	}
	acls := CompileTopic(tp)
	require.ElementsMatch(t, []ACL{
		{Principal: "User:svc-checkout", Host: "*", ResourceType: "topic", ResourceName: "payments.orders", PatternType: "literal", Operation: "Write", Permission: "Allow"},
		{Principal: "User:svc-checkout", Host: "*", ResourceType: "topic", ResourceName: "payments.orders", PatternType: "literal", Operation: "Describe", Permission: "Allow"},
	}, acls)
}

func TestProducerOperationsOverride(t *testing.T) {
	tp := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "orders"},
		Spec: v1alpha1.KafkaTopicSpec{
			TopicName: "payments.orders",
			Access:    v1alpha1.TopicAccess{Producers: []v1alpha1.ProducerAccess{{Principal: "User:p", Operations: []string{"Write", "Describe", "DescribeConfigs"}}}},
		},
	}
	acls := CompileTopic(tp)
	require.Len(t, acls, 3)
}

func TestConsumerCompilesToTopicAndGroupACLs(t *testing.T) {
	tp := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "orders"},
		Spec: v1alpha1.KafkaTopicSpec{
			TopicName: "payments.orders",
			Access:    v1alpha1.TopicAccess{Consumers: []v1alpha1.ConsumerAccess{{Principal: "User:svc-fraud", Group: "fraud-orders-consumer"}}},
		},
	}
	acls := CompileTopic(tp)
	require.Contains(t, acls, ACL{Principal: "User:svc-fraud", Host: "*", ResourceType: "group", ResourceName: "fraud-orders-consumer", PatternType: "literal", Operation: "Read", Permission: "Allow"})
	require.Contains(t, acls, ACL{Principal: "User:svc-fraud", Host: "*", ResourceType: "topic", ResourceName: "payments.orders", PatternType: "literal", Operation: "Read", Permission: "Allow"})
	require.Contains(t, acls, ACL{Principal: "User:svc-fraud", Host: "*", ResourceType: "topic", ResourceName: "payments.orders", PatternType: "literal", Operation: "Describe", Permission: "Allow"})
}

func TestCompileTopicDefaultsTopicNameToMetadataName(t *testing.T) {
	tp := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "orders"},
		Spec: v1alpha1.KafkaTopicSpec{
			Access: v1alpha1.TopicAccess{Producers: []v1alpha1.ProducerAccess{{Principal: "User:p"}}},
		},
	}
	acls := CompileTopic(tp)
	require.Equal(t, "orders", acls[0].ResourceName)
}

func TestCompilePolicy(t *testing.T) {
	pol := &v1alpha1.KafkaAccessPolicy{
		Spec: v1alpha1.KafkaAccessPolicySpec{
			Rules: []v1alpha1.ACLRule{
				{Principal: "User:a", Permission: "Allow", Host: "*", Resource: v1alpha1.ACLResource{Type: "topic", Name: "billing.", PatternType: "prefixed"}, Operations: []string{"Read", "Write"}},
			},
		},
	}
	acls := CompilePolicy(pol)
	require.Len(t, acls, 2)
	require.Equal(t, "prefixed", acls[0].PatternType)
}

func TestCompilePolicyAppliesRuleDefaults(t *testing.T) {
	// permission/host/patternType empty => defaults Allow/*/literal
	pol := &v1alpha1.KafkaAccessPolicy{
		Spec: v1alpha1.KafkaAccessPolicySpec{
			Rules: []v1alpha1.ACLRule{
				{Principal: "User:a", Resource: v1alpha1.ACLResource{Type: "topic", Name: "t"}, Operations: []string{"Read"}},
			},
		},
	}
	acls := CompilePolicy(pol)
	require.Equal(t, "Allow", acls[0].Permission)
	require.Equal(t, "*", acls[0].Host)
	require.Equal(t, "literal", acls[0].PatternType)
}

func TestDesiredSetDedupesIdenticalTuples(t *testing.T) {
	a := ACL{Principal: "User:x", Host: "*", ResourceType: "topic", ResourceName: "t", PatternType: "literal", Operation: "Read", Permission: "Allow"}
	set, errs := BuildDesiredSet([]ACL{a, a})
	require.Empty(t, errs)
	require.Len(t, set, 1)
}

func TestDesiredSetDetectsAllowDenyConflict(t *testing.T) {
	base := ACL{Principal: "User:x", Host: "*", ResourceType: "topic", ResourceName: "t", PatternType: "literal", Operation: "Read"}
	allow := base
	allow.Permission = "Allow"
	deny := base
	deny.Permission = "Deny"
	set, errs := BuildDesiredSet([]ACL{allow, deny})
	require.Len(t, errs, 1)
	// The first-seen permission (Allow) is retained; the conflicting tuple is dropped.
	require.Len(t, set, 1)
	require.Equal(t, "Allow", set[0].Permission)
}

func TestCompilePolicyMultipleRules(t *testing.T) {
	pol := &v1alpha1.KafkaAccessPolicy{
		Spec: v1alpha1.KafkaAccessPolicySpec{
			Rules: []v1alpha1.ACLRule{
				{Principal: "User:a", Permission: "Allow", Host: "*", Resource: v1alpha1.ACLResource{Type: "topic", Name: "billing.", PatternType: "prefixed"}, Operations: []string{"Read", "Write"}},
				{Principal: "User:a", Permission: "Allow", Host: "*", Resource: v1alpha1.ACLResource{Type: "cluster", Name: "kafka-cluster", PatternType: "literal"}, Operations: []string{"IdempotentWrite"}},
			},
		},
	}
	acls := CompilePolicy(pol)
	require.Len(t, acls, 3)
	var sawPrefixed, sawCluster bool
	for _, a := range acls {
		if a.PatternType == "prefixed" {
			sawPrefixed = true
		}
		if a.ResourceType == "cluster" {
			sawCluster = true
		}
	}
	require.True(t, sawPrefixed, "expected a prefixed patternType in compiled ACLs")
	require.True(t, sawCluster, "expected a cluster resourceType in compiled ACLs")
}

func TestCompilePolicyDenyPassesThrough(t *testing.T) {
	pol := &v1alpha1.KafkaAccessPolicy{
		Spec: v1alpha1.KafkaAccessPolicySpec{
			Rules: []v1alpha1.ACLRule{
				{Principal: "User:a", Permission: "Deny", Host: "*", Resource: v1alpha1.ACLResource{Type: "topic", Name: "t", PatternType: "literal"}, Operations: []string{"Read"}},
			},
		},
	}
	acls := CompilePolicy(pol)
	require.Len(t, acls, 1)
	require.Equal(t, "Deny", acls[0].Permission)
}

func TestConsumerGroupOperationsOverride(t *testing.T) {
	tp := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "orders"},
		Spec: v1alpha1.KafkaTopicSpec{
			TopicName: "payments.orders",
			Access: v1alpha1.TopicAccess{Consumers: []v1alpha1.ConsumerAccess{{
				Principal:       "User:svc-fraud",
				Group:           "fraud-orders-consumer",
				TopicOperations: []string{"Read"},
				GroupOperations: []string{"Read", "Describe"},
			}}},
		},
	}
	acls := CompileTopic(tp)
	var groupACLs, topicACLs []ACL
	for _, a := range acls {
		switch a.ResourceType {
		case "group":
			groupACLs = append(groupACLs, a)
		case "topic":
			topicACLs = append(topicACLs, a)
		}
	}
	require.Len(t, groupACLs, 2)
	require.ElementsMatch(t, []string{"Read", "Describe"}, []string{groupACLs[0].Operation, groupACLs[1].Operation})
	require.Len(t, topicACLs, 1)
	require.Equal(t, "Read", topicACLs[0].Operation)
}

func hasACL(acls []ACL, principal, host, rtype, rname, op string) bool {
	for _, a := range acls {
		if a.Principal == principal && a.Host == host && a.ResourceType == rtype &&
			a.ResourceName == rname && a.Operation == op && a.Permission == "Allow" {
			return true
		}
	}
	return false
}

func TestCompileTopicHostScoping(t *testing.T) {
	tp := &v1alpha1.KafkaTopic{}
	tp.Name = "orders"
	tp.Spec.TopicName = "orders"
	tp.Spec.Access.Producers = []v1alpha1.ProducerAccess{
		{Principal: "User:p1"},                     // host defaults to *
		{Principal: "User:p2", Host: "10.0.0.0/8"}, // explicit host
	}
	tp.Spec.Access.Consumers = []v1alpha1.ConsumerAccess{
		{Principal: "User:c1", Group: "g1", Host: "192.168.1.5"},
	}
	acls := CompileTopic(tp)
	require.True(t, hasACL(acls, "User:p1", "*", "topic", "orders", "Write"))
	require.True(t, hasACL(acls, "User:p2", "10.0.0.0/8", "topic", "orders", "Write"))
	require.True(t, hasACL(acls, "User:c1", "192.168.1.5", "topic", "orders", "Read"))
	require.True(t, hasACL(acls, "User:c1", "192.168.1.5", "group", "g1", "Read"))
}

func TestBuildDesiredSetMixedInput(t *testing.T) {
	dup := ACL{Principal: "User:x", Host: "*", ResourceType: "topic", ResourceName: "t", PatternType: "literal", Operation: "Read", Permission: "Allow"}
	other1 := ACL{Principal: "User:y", Host: "*", ResourceType: "topic", ResourceName: "t", PatternType: "literal", Operation: "Write", Permission: "Allow"}
	other2 := ACL{Principal: "User:z", Host: "*", ResourceType: "group", ResourceName: "g", PatternType: "literal", Operation: "Read", Permission: "Allow"}
	set, errs := BuildDesiredSet([]ACL{dup, dup, other1, other2})
	require.Empty(t, errs)
	require.Len(t, set, 3)
}
