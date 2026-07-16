package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

// newMapFuncClient builds a fake client seeded with the SchemaConfigMapIndex
// field index (using schemaConfigMapNames as the extractor, matching
// production) and the supplied objects.
func newMapFuncClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	s := topicScheme(t)
	return fake.NewClientBuilder().
		WithScheme(s).
		WithIndex(&v1alpha1.KafkaTopic{}, SchemaConfigMapIndex, func(o client.Object) []string {
			return schemaConfigMapNames(o.(*v1alpha1.KafkaTopic))
		}).
		WithObjects(objs...).
		Build()
}

// namedTopic returns a minimal named KafkaTopic in the given namespace.
func namedTopic(ns, name, keyCM, valCM string) *v1alpha1.KafkaTopic {
	tp := topicWithSchemaCMs(keyCM, valCM)
	tp.Name = name
	tp.Namespace = ns
	tp.Spec.ClusterRef = v1alpha1.ClusterRef{Name: "prod"}
	return tp
}

// namedCM returns a minimal ConfigMap in the given namespace.
func namedCM(ns, name string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},
	}
}

// TestMapConfigMapToTopics_KeyAndValueReferencer: ConfigMap cm-a in ns n1;
// topic-A references it via key schema, topic-B via value schema,
// topic-C references cm-b → only topic-A and topic-B are returned.
func TestMapConfigMapToTopics_KeyAndValueReferencer(t *testing.T) {
	topicA := namedTopic("n1", "topic-a", "cm-a", "")
	topicB := namedTopic("n1", "topic-b", "", "cm-a")
	topicC := namedTopic("n1", "topic-c", "cm-b", "")
	cm := namedCM("n1", "cm-a")

	cl := newMapFuncClient(t, topicA, topicB, topicC, cm)
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t)}

	got := r.mapConfigMapToTopics(context.Background(), cm)

	if len(got) != 2 {
		t.Fatalf("expected 2 requests, got %d: %v", len(got), got)
	}
	wantSet := map[types.NamespacedName]bool{
		{Namespace: "n1", Name: "topic-a"}: true,
		{Namespace: "n1", Name: "topic-b"}: true,
	}
	for _, req := range got {
		if !wantSet[req.NamespacedName] {
			t.Errorf("unexpected request %v", req.NamespacedName)
		}
		delete(wantSet, req.NamespacedName)
	}
	if len(wantSet) > 0 {
		t.Errorf("missing expected requests: %v", wantSet)
	}
}

// TestMapConfigMapToTopics_NamespaceIsolation: topic in ns n2 references cm-a
// but the ConfigMap is in ns n1 → not returned (namespace isolation).
func TestMapConfigMapToTopics_NamespaceIsolation(t *testing.T) {
	// topic in n2 references cm-a — wrong namespace, must not match
	topicWrongNS := namedTopic("n2", "topic-x", "cm-a", "")
	// topic in n1 also exists but references a different CM
	topicRightNS := namedTopic("n1", "topic-y", "cm-b", "")
	cm := namedCM("n1", "cm-a")

	cl := newMapFuncClient(t, topicWrongNS, topicRightNS, cm)
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t)}

	got := r.mapConfigMapToTopics(context.Background(), cm)
	if len(got) != 0 {
		t.Errorf("expected 0 requests (namespace isolation), got %d: %v", len(got), got)
	}
}

// TestMapConfigMapToTopics_NoReferencer: ConfigMap not referenced by any topic
// → empty result.
func TestMapConfigMapToTopics_NoReferencer(t *testing.T) {
	topicA := namedTopic("n1", "topic-a", "cm-other", "")
	cm := namedCM("n1", "cm-a")

	cl := newMapFuncClient(t, topicA, cm)
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t)}

	got := r.mapConfigMapToTopics(context.Background(), cm)
	if len(got) != 0 {
		t.Errorf("expected 0 requests, got %d: %v", len(got), got)
	}
}

// TestMapConfigMapToTopics_NonConfigMapObject: passing a non-ConfigMap object
// (e.g. a KafkaTopic) → nil returned, no panic.
func TestMapConfigMapToTopics_NonConfigMapObject(t *testing.T) {
	tp := namedTopic("n1", "topic-a", "cm-a", "")
	cl := newMapFuncClient(t)
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t)}

	got := r.mapConfigMapToTopics(context.Background(), tp)
	if got != nil {
		t.Errorf("expected nil for non-ConfigMap object, got %v", got)
	}
}
