package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

func TestKafkaTopicRoundTrip(t *testing.T) {
	in := []byte(`apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaTopic
metadata:
  name: orders
  namespace: payments
spec:
  clusterRef:
    name: prod-eu
  topicName: payments.orders
  partitions: 12
  config:
    retention.ms: "604800000"
  access:
    producers:
      - principal: User:svc-checkout
`)
	var topic KafkaTopic
	require.NoError(t, yaml.Unmarshal(in, &topic))
	require.Equal(t, "KafkaTopic", topic.Kind)
	require.Equal(t, "orders", topic.Name)
	require.Equal(t, "payments", topic.Namespace)
	require.Equal(t, "prod-eu", topic.Spec.ClusterRef.Name)
	require.Equal(t, 12, topic.Spec.Partitions)
	require.Equal(t, "604800000", topic.Spec.Config["retention.ms"])
	require.Equal(t, "User:svc-checkout", topic.Spec.Access.Producers[0].Principal)
}

func TestKafkaTopicDeepCopyObject(t *testing.T) {
	orig := &KafkaTopic{
		TypeMeta: metav1.TypeMeta{APIVersion: APIVersion, Kind: "KafkaTopic"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        "orders",
			Namespace:   "payments",
			Annotations: map[string]string{"a": "1"},
		},
		Spec: KafkaTopicSpec{
			ClusterRef: ClusterRef{Name: "prod-eu"},
			Partitions: 3,
			Config:     map[string]string{"retention.ms": "1000"},
		},
	}

	cp, ok := orig.DeepCopyObject().(*KafkaTopic)
	require.True(t, ok)
	require.Equal(t, orig.Name, cp.Name)
	require.Equal(t, orig.Spec.Config["retention.ms"], cp.Spec.Config["retention.ms"])

	// Mutating the copy must not affect the original (independent copy).
	cp.Annotations["a"] = "mutated"
	cp.Spec.Config["retention.ms"] = "9999"
	require.Equal(t, "1", orig.Annotations["a"])
	require.Equal(t, "1000", orig.Spec.Config["retention.ms"])
}

func TestKafkaTopicStatusRoundTrip(t *testing.T) {
	now := metav1.Now()
	orig := &KafkaTopic{
		TypeMeta:   metav1.TypeMeta{APIVersion: APIVersion, Kind: "KafkaTopic"},
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "payments"},
		Spec:       KafkaTopicSpec{ClusterRef: ClusterRef{Name: "prod-eu"}, Partitions: 12},
		Status: &KafkaTopicStatus{
			ObservedGeneration: 7,
			Phase:              PhaseReady,
			Conditions: []metav1.Condition{{
				Type:               CondReady,
				Status:             metav1.ConditionTrue,
				Reason:             "Synced",
				Message:            "topic in sync",
				LastTransitionTime: now,
			}},
			ObservedTopic: &ObservedTopic{
				TopicName:         "payments.orders",
				Partitions:        12,
				ReplicationFactor: 3,
				Config:            map[string]string{"retention.ms": "604800000"},
			},
			Drift:           &DriftStatus{Detected: true, Fields: []string{"partitions"}},
			LastCheckedTime: &now,
		},
	}

	out, err := yaml.Marshal(orig)
	require.NoError(t, err)

	var back KafkaTopic
	require.NoError(t, yaml.Unmarshal(out, &back))

	require.NotNil(t, back.Status)
	require.Equal(t, int64(7), back.Status.ObservedGeneration)
	require.Equal(t, PhaseReady, back.Status.Phase)
	require.Len(t, back.Status.Conditions, 1)
	require.Equal(t, CondReady, back.Status.Conditions[0].Type)
	require.Equal(t, metav1.ConditionTrue, back.Status.Conditions[0].Status)
	require.NotNil(t, back.Status.ObservedTopic)
	require.Equal(t, 12, back.Status.ObservedTopic.Partitions)
	require.Equal(t, "604800000", back.Status.ObservedTopic.Config["retention.ms"])
	require.NotNil(t, back.Status.Drift)
	require.True(t, back.Status.Drift.Detected)
	require.Equal(t, []string{"partitions"}, back.Status.Drift.Fields)
	require.NotNil(t, back.Status.LastCheckedTime)
}

func TestKafkaTopicStatusDeepCopyIndependent(t *testing.T) {
	orig := &KafkaTopic{
		Status: &KafkaTopicStatus{
			Phase:         PhaseReady,
			Conditions:    []metav1.Condition{{Type: CondReady, Status: metav1.ConditionTrue}},
			ObservedTopic: &ObservedTopic{Partitions: 3, Config: map[string]string{"k": "v"}},
			Drift:         &DriftStatus{Detected: true, Fields: []string{"partitions"}},
		},
	}

	cp := orig.DeepCopy()
	cp.Status.Phase = PhaseError
	cp.Status.Conditions[0].Type = "Mutated"
	cp.Status.ObservedTopic.Config["k"] = "mutated"
	cp.Status.Drift.Fields[0] = "mutated"

	require.Equal(t, PhaseReady, orig.Status.Phase)
	require.Equal(t, CondReady, orig.Status.Conditions[0].Type)
	require.Equal(t, "v", orig.Status.ObservedTopic.Config["k"])
	require.Equal(t, "partitions", orig.Status.Drift.Fields[0])
}
