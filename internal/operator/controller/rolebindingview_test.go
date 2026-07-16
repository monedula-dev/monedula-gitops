package controller

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/rbac"
)

// mdsClusterForView builds a KafkaCluster with authorization.mds configured
// and the given accessBackends, suitable for buildClusterRoleBindingView tests.
func mdsClusterForView(ns, name string, accessBackends ...string) *v1alpha1.KafkaCluster {
	return &v1alpha1.KafkaCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: v1alpha1.KafkaClusterSpec{
			BootstrapServers: "localhost:9092",
			Authorization: &v1alpha1.AuthorizationConfig{
				MDS: &v1alpha1.MDSConfig{
					Endpoint: "http://mds:8090",
					Clusters: v1alpha1.MDSClusters{
						KafkaCluster: "lkc-view123",
					},
				},
				AccessBackends: accessBackends,
			},
		},
	}
}

// rbForView builds a minimal KafkaRoleBinding referencing clusterName.
func rbForView(ns, name, clusterName string) *v1alpha1.KafkaRoleBinding {
	return &v1alpha1.KafkaRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: v1alpha1.KafkaRoleBindingSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: clusterName},
			Principal:  "User:svc-consumer",
			Role:       "DeveloperRead",
			Scope:      v1alpha1.RoleBindingScope{Type: "kafka"},
			Resources: []v1alpha1.RoleResource{
				{Type: "Topic", Name: "explicit.topic", PatternType: "literal"},
			},
		},
	}
}

// topicForView builds a KafkaTopic with a producer access entry, referencing
// clusterName.
func topicForView(ns, name, clusterName, topicName, producerPrincipal string) *v1alpha1.KafkaTopic {
	return &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: clusterName},
			TopicName:  topicName,
			Partitions: 3,
			Access: v1alpha1.TopicAccess{
				Producers: []v1alpha1.ProducerAccess{
					{Principal: producerPrincipal},
				},
			},
		},
	}
}

// TestBuildClusterRoleBindingView_AggregatesTopicDerivedBindings verifies that
// when the cluster has accessBackends including "rbac", KafkaTopic producer
// access entries produce a DeveloperWrite binding in view.DesiredBindings.
func TestBuildClusterRoleBindingView_AggregatesTopicDerivedBindings(t *testing.T) {
	s := newScheme(t)
	cl := mdsClusterForView("ns1", "prod", "acl", "rbac")
	tp := topicForView("ns1", "orders", "prod", "orders", "User:svc-producer")
	rb := rbForView("ns1", "rb-consumer", "prod")

	fakeClient := newViewFakeClient(t, s, cl, tp, rb)

	view, err := buildClusterRoleBindingView(context.Background(), fakeClient,
		"prod", "ns1", true, cl.Spec.Authorization.MDS, cl)
	if err != nil {
		t.Fatalf("buildClusterRoleBindingView: %v", err)
	}

	// Expect DesiredBindings to contain:
	//   1. The explicit KafkaRoleBinding: User:svc-consumer DeveloperRead kafka lkc-view123 Topic:explicit.topic
	//   2. The topic-derived binding: User:svc-producer DeveloperWrite kafka lkc-view123 Topic:orders

	byKey := make(map[string]rbac.RoleBinding, len(view.DesiredBindings))
	for _, b := range view.DesiredBindings {
		byKey[b.FullKey()] = b
	}

	// Check topic-derived DeveloperWrite binding.
	wantWrite := rbac.RoleBinding{
		Principal: "User:svc-producer",
		Role:      "DeveloperWrite",
		Scope:     rbac.Scope{Type: "kafka", KafkaCluster: "lkc-view123"},
		Resource:  &rbac.ResourcePattern{Type: "Topic", Name: "orders", PatternType: "literal"},
	}
	if _, ok := byKey[wantWrite.FullKey()]; !ok {
		t.Errorf("topic-derived DeveloperWrite binding missing from DesiredBindings; got keys: %v", keysOf(byKey))
	}

	// Check explicit KafkaRoleBinding: User:svc-consumer DeveloperRead.
	wantRead := rbac.RoleBinding{
		Principal: "User:svc-consumer",
		Role:      "DeveloperRead",
		Scope:     rbac.Scope{Type: "kafka", KafkaCluster: "lkc-view123"},
		Resource:  &rbac.ResourcePattern{Type: "Topic", Name: "explicit.topic", PatternType: "literal"},
	}
	if _, ok := byKey[wantRead.FullKey()]; !ok {
		t.Errorf("explicit KafkaRoleBinding DeveloperRead binding missing from DesiredBindings; got keys: %v", keysOf(byKey))
	}
}

// TestBuildClusterRoleBindingView_SkipsTopicsWhenNoRBACBackend verifies that
// when the cluster does NOT include "rbac" in accessBackends, KafkaTopics are
// NOT listed and no topic-derived bindings appear.
func TestBuildClusterRoleBindingView_SkipsTopicsWhenNoRBACBackend(t *testing.T) {
	s := newScheme(t)
	// Cluster has only "acl" backend — no "rbac".
	cl := mdsClusterForView("ns1", "prod", "acl")
	tp := topicForView("ns1", "orders", "prod", "orders", "User:svc-producer")
	rb := rbForView("ns1", "rb-consumer", "prod")

	fakeClient := newViewFakeClient(t, s, cl, tp, rb)

	view, err := buildClusterRoleBindingView(context.Background(), fakeClient,
		"prod", "ns1", true, cl.Spec.Authorization.MDS, cl)
	if err != nil {
		t.Fatalf("buildClusterRoleBindingView: %v", err)
	}

	// Only the explicit KafkaRoleBinding should appear; no DeveloperWrite binding.
	for _, b := range view.DesiredBindings {
		if b.Role == "DeveloperWrite" {
			t.Errorf("topic-derived DeveloperWrite binding present despite no rbac backend: %+v", b)
		}
	}
}

// TestBuildClusterRoleBindingView_DesiredScopeMatchesBindings verifies that
// DesiredScope is derived from DesiredBindings (i.e. Contains works for
// every binding in the deduped set).
func TestBuildClusterRoleBindingView_DesiredScopeMatchesBindings(t *testing.T) {
	s := newScheme(t)
	cl := mdsClusterForView("ns1", "prod", "rbac")
	tp := topicForView("ns1", "orders", "prod", "orders", "User:svc-producer")

	fakeClient := newViewFakeClient(t, s, cl, tp)

	view, err := buildClusterRoleBindingView(context.Background(), fakeClient,
		"prod", "ns1", true, cl.Spec.Authorization.MDS, cl)
	if err != nil {
		t.Fatalf("buildClusterRoleBindingView: %v", err)
	}

	for _, b := range view.DesiredBindings {
		if !view.DesiredScope.Contains(b) {
			t.Errorf("binding %s is in DesiredBindings but not covered by DesiredScope", b.FullKey())
		}
	}
}

// keysOf extracts the map keys for readable test failure messages.
func keysOf(m map[string]rbac.RoleBinding) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
