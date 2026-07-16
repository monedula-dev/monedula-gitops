package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

// makeSchemaUnwatchedReconciler builds a KafkaTopicReconciler backed by a fake
// client seeded with the supplied ConfigMaps. The scheme includes corev1 and
// v1alpha1 (mirrors topicScheme).
func makeSchemaUnwatchedReconciler(t *testing.T, cms ...*corev1.ConfigMap) *KafkaTopicReconciler {
	t.Helper()
	s := topicScheme(t)
	b := fake.NewClientBuilder().WithScheme(s)
	for _, cm := range cms {
		b = b.WithObjects(cm)
	}
	return &KafkaTopicReconciler{Client: b.Build()}
}

// labelledCM returns a ConfigMap with the SchemaSourceLabel set.
func labelledCM(namespace, name string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			Labels:    map[string]string{SchemaSourceLabel: SchemaSourceLabelValue},
		},
	}
}

// unlabelledCM returns a ConfigMap without the SchemaSourceLabel.
func unlabelledCM(namespace, name string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
	}
}

// topicInNS returns a KafkaTopic with a value-schema ConfigMapKeyRef in the
// given namespace referencing cmName.
func topicInNS(namespace, cmName string) *v1alpha1.KafkaTopic {
	tp := topicWithSchemaCMs("", cmName)
	tp.Namespace = namespace
	tp.Generation = 2
	return tp
}

// emptyStatus returns a zero KafkaTopicStatus for use as the mutation target.
func emptyStatus() v1alpha1.KafkaTopicStatus {
	return v1alpha1.KafkaTopicStatus{}
}

// TestSchemaSourceUnwatched_NoRef: a topic with no schema ConfigMap reference
// sets the condition to False/AllWatchedOrNone.
func TestSchemaSourceUnwatched_NoRef(t *testing.T) {
	r := makeSchemaUnwatchedReconciler(t)
	topic := &v1alpha1.KafkaTopic{}
	topic.Namespace = "ns1"
	topic.Generation = 7

	st := emptyStatus()
	r.setSchemaSourceUnwatchedCondition(context.Background(), topic, &st)

	cond := findCond(st.Conditions, v1alpha1.CondSchemaSourceUnwatched)
	if cond == nil {
		t.Fatal("condition not set")
	}
	if cond.Status != metav1.ConditionFalse {
		t.Fatalf("status = %v want False", cond.Status)
	}
	if cond.Reason != "AllWatchedOrNone" {
		t.Fatalf("reason = %q want AllWatchedOrNone", cond.Reason)
	}
	if cond.ObservedGeneration != topic.Generation {
		t.Fatalf("ObservedGeneration = %v want %v", cond.ObservedGeneration, topic.Generation)
	}
}

// TestSchemaSourceUnwatched_CMPresentWithoutLabel: ConfigMap exists but lacks
// the label — condition must be True/ConfigMapNotLabeled, message mentions the CM name.
func TestSchemaSourceUnwatched_CMPresentWithoutLabel(t *testing.T) {
	cm := unlabelledCM("ns1", "cm-x")
	r := makeSchemaUnwatchedReconciler(t, cm)

	topic := topicInNS("ns1", "cm-x")
	st := emptyStatus()
	r.setSchemaSourceUnwatchedCondition(context.Background(), topic, &st)

	cond := findCond(st.Conditions, v1alpha1.CondSchemaSourceUnwatched)
	if cond == nil {
		t.Fatal("condition not set")
	}
	if cond.Status != metav1.ConditionTrue {
		t.Fatalf("status = %v want True", cond.Status)
	}
	if cond.Reason != "ConfigMapNotLabeled" {
		t.Fatalf("reason = %q want ConfigMapNotLabeled", cond.Reason)
	}
	if !strings.Contains(cond.Message, "cm-x") {
		t.Fatalf("message %q does not mention cm-x", cond.Message)
	}
	if cond.ObservedGeneration != topic.Generation {
		t.Fatalf("ObservedGeneration = %v want %v", cond.ObservedGeneration, topic.Generation)
	}
}

// TestSchemaSourceUnwatched_CMPresentWithLabel: ConfigMap exists and carries the
// label — condition must be False/AllWatchedOrNone.
func TestSchemaSourceUnwatched_CMPresentWithLabel(t *testing.T) {
	cm := labelledCM("ns1", "cm-x")
	r := makeSchemaUnwatchedReconciler(t, cm)

	topic := topicInNS("ns1", "cm-x")
	topic.Generation = 7
	st := emptyStatus()
	r.setSchemaSourceUnwatchedCondition(context.Background(), topic, &st)

	cond := findCond(st.Conditions, v1alpha1.CondSchemaSourceUnwatched)
	if cond == nil {
		t.Fatal("condition not set")
	}
	if cond.Status != metav1.ConditionFalse {
		t.Fatalf("status = %v want False", cond.Status)
	}
	if cond.Reason != "AllWatchedOrNone" {
		t.Fatalf("reason = %q want AllWatchedOrNone", cond.Reason)
	}
	if cond.ObservedGeneration != topic.Generation {
		t.Fatalf("ObservedGeneration = %v want %v", cond.ObservedGeneration, topic.Generation)
	}
}

// TestSchemaSourceUnwatched_CMNotFound: ConfigMap is referenced but does not exist —
// Get returns NotFound, the helper skips the CM (no false positive), condition is False.
func TestSchemaSourceUnwatched_CMNotFound(t *testing.T) {
	// No ConfigMaps seeded; "cm-missing" will produce a NotFound on Get.
	r := makeSchemaUnwatchedReconciler(t)

	topic := topicInNS("ns1", "cm-missing")
	topic.Generation = 7
	st := emptyStatus()
	r.setSchemaSourceUnwatchedCondition(context.Background(), topic, &st)

	cond := findCond(st.Conditions, v1alpha1.CondSchemaSourceUnwatched)
	if cond == nil {
		t.Fatal("condition not set")
	}
	// Read error → skip → no unlabelled entries → False (no false positive).
	if cond.Status != metav1.ConditionFalse {
		t.Fatalf("status = %v want False (read error must not flag as unwatched)", cond.Status)
	}
	if cond.Reason != "AllWatchedOrNone" {
		t.Fatalf("reason = %q want AllWatchedOrNone", cond.Reason)
	}
	if cond.ObservedGeneration != topic.Generation {
		t.Fatalf("ObservedGeneration = %v want %v", cond.ObservedGeneration, topic.Generation)
	}
}
