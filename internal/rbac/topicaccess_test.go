package rbac_test

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/rbac"
)

func mds() *v1alpha1.MDSConfig {
	return &v1alpha1.MDSConfig{Endpoint: "https://mds", Clusters: v1alpha1.MDSClusters{KafkaCluster: "kid"}}
}

func topic(name string, acc v1alpha1.TopicAccess) *v1alpha1.KafkaTopic {
	return &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: name},
		Spec:       v1alpha1.KafkaTopicSpec{TopicName: name, Access: acc},
	}
}

func keys(bs []rbac.RoleBinding) map[string]rbac.RoleBinding {
	m := map[string]rbac.RoleBinding{}
	for _, b := range bs {
		m[b.FullKey()] = b
	}
	return m
}

func TestCompileTopicAccessProducer(t *testing.T) {
	tp := topic("orders", v1alpha1.TopicAccess{
		Producers: []v1alpha1.ProducerAccess{{Principal: "User:svc"}},
	})
	bs, warns, err := rbac.CompileTopicAccess(tp, mds())
	if err != nil {
		t.Fatal(err)
	}
	if len(warns) != 0 {
		t.Fatalf("no coarsening expected, got %v", warns)
	}
	if len(bs) != 1 {
		t.Fatalf("producer → 1 binding, got %d", len(bs))
	}
	b := bs[0]
	if b.Principal != "User:svc" || b.Role != "DeveloperWrite" {
		t.Fatalf("bad principal/role: %+v", b)
	}
	if b.Resource == nil || b.Resource.Type != "Topic" || b.Resource.Name != "orders" || b.Resource.PatternType != "literal" {
		t.Fatalf("bad resource: %+v", b.Resource)
	}
	if b.Scope.Type != "kafka" || b.Scope.KafkaCluster != "kid" {
		t.Fatalf("bad scope: %+v", b.Scope)
	}
	if b.SourceKind != "KafkaTopic" || b.SourceNamespace != "team-a" || b.SourceName != "orders" {
		t.Fatalf("bad source attribution: %+v", b)
	}
}

func TestCompileTopicAccessConsumerEmitsTopicAndGroup(t *testing.T) {
	tp := topic("orders", v1alpha1.TopicAccess{
		Consumers: []v1alpha1.ConsumerAccess{{Principal: "User:svc", Group: "cg"}},
	})
	bs, warns, err := rbac.CompileTopicAccess(tp, mds())
	if err != nil {
		t.Fatal(err)
	}
	if len(warns) != 0 {
		t.Fatalf("no coarsening expected, got %v", warns)
	}
	if len(bs) != 2 {
		t.Fatalf("consumer → 2 bindings, got %d: %+v", len(bs), bs)
	}
	k := keys(bs)
	topicBinding := rbac.RoleBinding{Principal: "User:svc", Role: "DeveloperRead",
		Scope:    rbac.Scope{Type: "kafka", KafkaCluster: "kid"},
		Resource: &rbac.ResourcePattern{Type: "Topic", Name: "orders", PatternType: "literal"}}
	groupBinding := rbac.RoleBinding{Principal: "User:svc", Role: "DeveloperRead",
		Scope:    rbac.Scope{Type: "kafka", KafkaCluster: "kid"},
		Resource: &rbac.ResourcePattern{Type: "Group", Name: "cg", PatternType: "literal"}}
	if _, ok := k[topicBinding.FullKey()]; !ok {
		t.Fatalf("missing topic DeveloperRead binding; got %+v", bs)
	}
	if _, ok := k[groupBinding.FullKey()]; !ok {
		t.Fatalf("missing group DeveloperRead binding; got %+v", bs)
	}
}

func TestCompileTopicAccessTopicNameFallback(t *testing.T) {
	tp := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "metadata-name"},
		Spec:       v1alpha1.KafkaTopicSpec{Access: v1alpha1.TopicAccess{Producers: []v1alpha1.ProducerAccess{{Principal: "User:svc"}}}},
	}
	bs, _, err := rbac.CompileTopicAccess(tp, mds())
	if err != nil {
		t.Fatal(err)
	}
	if bs[0].Resource.Name != "metadata-name" {
		t.Fatalf("topicName fallback to metadata.name failed: %+v", bs[0].Resource)
	}
}

func TestCompileTopicAccessLossyHostWarns(t *testing.T) {
	tp := topic("orders", v1alpha1.TopicAccess{
		Producers: []v1alpha1.ProducerAccess{{Principal: "User:svc", Host: "10.0.0.1"}},
	})
	bs, warns, err := rbac.CompileTopicAccess(tp, mds())
	if err != nil {
		t.Fatal(err)
	}
	if len(bs) != 1 {
		t.Fatalf("lossy host still emits the binding, got %d", len(bs))
	}
	if len(warns) == 0 {
		t.Fatal("non-* host must produce a coarsening warning")
	}
}

func TestCompileTopicAccessStarHostNoWarn(t *testing.T) {
	for _, h := range []string{"", "*"} {
		tp := topic("orders", v1alpha1.TopicAccess{
			Producers: []v1alpha1.ProducerAccess{{Principal: "User:svc", Host: h}},
		})
		_, warns, err := rbac.CompileTopicAccess(tp, mds())
		if err != nil {
			t.Fatal(err)
		}
		if len(warns) != 0 {
			t.Fatalf("host %q must not warn, got %v", h, warns)
		}
	}
}

func TestCompileTopicAccessLossyOperationsWarns(t *testing.T) {
	tp := topic("orders", v1alpha1.TopicAccess{
		Producers: []v1alpha1.ProducerAccess{{Principal: "User:svc", Operations: []string{"Describe"}}},
	})
	bs, warns, err := rbac.CompileTopicAccess(tp, mds())
	if err != nil {
		t.Fatal(err)
	}
	if len(bs) != 1 {
		t.Fatalf("lossy ops still emits the binding, got %d", len(bs))
	}
	if len(warns) == 0 {
		t.Fatal("custom operation list must produce a coarsening warning")
	}
}

func TestCompileTopicAccessDefaultBundleNoWarn(t *testing.T) {
	tp := topic("orders", v1alpha1.TopicAccess{
		Producers: []v1alpha1.ProducerAccess{{Principal: "User:svc", Operations: []string{"Write", "Describe"}}},
	})
	_, warns, err := rbac.CompileTopicAccess(tp, mds())
	if err != nil {
		t.Fatal(err)
	}
	if len(warns) != 0 {
		t.Fatalf("default bundle must not warn, got %v", warns)
	}
}

func TestCompileTopicAccessMissingKafkaClusterID(t *testing.T) {
	tp := topic("orders", v1alpha1.TopicAccess{Producers: []v1alpha1.ProducerAccess{{Principal: "User:svc"}}})
	_, _, err := rbac.CompileTopicAccess(tp, &v1alpha1.MDSConfig{Endpoint: "https://mds"})
	if err == nil {
		t.Fatal("missing kafka-cluster id must error")
	}
}

func TestCompileTopicAccessConsumerHostWarnsOnce(t *testing.T) {
	tp := topic("orders", v1alpha1.TopicAccess{
		Consumers: []v1alpha1.ConsumerAccess{{Principal: "User:svc", Group: "cg", Host: "10.0.0.1"}},
	})
	bs, warns, err := rbac.CompileTopicAccess(tp, mds())
	if err != nil {
		t.Fatal(err)
	}
	if len(bs) != 2 {
		t.Fatalf("consumer still emits 2 bindings, got %d", len(bs))
	}
	if len(warns) != 1 {
		t.Fatalf("non-* consumer host must warn exactly once, got %d: %v", len(warns), warns)
	}
}

func TestSortedWarnings(t *testing.T) {
	in := []string{"c", "a", "b"}
	got := rbac.SortedWarnings(in)
	want := []string{"a", "b", "c"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("SortedWarnings = %v, want %v", got, want)
		}
	}
	if in[0] != "c" {
		t.Fatalf("SortedWarnings must not mutate input; input now %v", in)
	}
}
