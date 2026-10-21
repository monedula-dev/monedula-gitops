package v1alpha1

import "testing"

func TestMirrorEffectiveTopicName(t *testing.T) {
	s := KafkaMirrorTopicSpec{SourceTopicName: "payments.orders"}
	if got := s.EffectiveTopicName(); got != "payments.orders" {
		t.Fatalf("omitted: %q", got)
	}
	s.TopicName = ""
	if got := s.EffectiveTopicName(); got != "payments.orders" {
		t.Fatalf("empty explicit: %q", got)
	}
	s.TopicName = "dr.payments.orders"
	if got := s.EffectiveTopicName(); got != "dr.payments.orders" {
		t.Fatalf("explicit: %q", got)
	}
}

func TestMirrorDeepCopyRoundTrip(t *testing.T) {
	rf := 3
	in := &KafkaMirrorTopic{Spec: KafkaMirrorTopicSpec{ClusterRef: ClusterRef{Name: "dr"}, LinkName: "prod-to-dr",
		SourceTopicName: "a", ReplicationFactor: &rf, Config: map[string]string{"min.insync.replicas": "2"}}}
	out := in.DeepCopy()
	*out.Spec.ReplicationFactor = 1
	out.Spec.Config["min.insync.replicas"] = "1"
	if *in.Spec.ReplicationFactor != 3 || in.Spec.Config["min.insync.replicas"] != "2" {
		t.Fatal("DeepCopy aliases spec")
	}
}

func TestKafkaClusterRESTDeepCopy(t *testing.T) {
	in := &KafkaCluster{Spec: KafkaClusterSpec{REST: &KafkaRESTConfig{Endpoint: "https://x:8090/kafka", ClusterID: "lkc-1"}}}
	out := in.DeepCopy()
	out.Spec.REST.ClusterID = "other"
	if in.Spec.REST.ClusterID != "lkc-1" {
		t.Fatal("DeepCopy aliases rest")
	}
}
