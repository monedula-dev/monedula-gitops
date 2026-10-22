package webhook

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

// newMirrorFakeClient builds a fake client with both cluster-ref indexes
// registered (mirroring RegisterIndexes) so the validator's MatchingFields
// Lists work hermetically.
func newMirrorFakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithIndex(&v1alpha1.KafkaTopic{}, ClusterRefNameIndex, func(obj client.Object) []string {
			tp, ok := obj.(*v1alpha1.KafkaTopic)
			if !ok {
				return nil
			}
			return []string{tp.Spec.ClusterRef.Name}
		}).
		WithIndex(&v1alpha1.KafkaMirrorTopic{}, MirrorTopicClusterRefNameIndex, func(obj client.Object) []string {
			m, ok := obj.(*v1alpha1.KafkaMirrorTopic)
			if !ok {
				return nil
			}
			return []string{m.Spec.ClusterRef.Name}
		}).
		WithObjects(objs...).
		Build()
}

// mirrorCR builds a shape-valid KafkaMirrorTopic with a distinct UID.
// topicName "" exercises the sourceTopicName default.
func mirrorCR(uid, ns, name, clusterRef, source, topicName string) *v1alpha1.KafkaMirrorTopic {
	return &v1alpha1.KafkaMirrorTopic{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, UID: types.UID(uid)},
		Spec: v1alpha1.KafkaMirrorTopicSpec{
			ClusterRef:      v1alpha1.ClusterRef{Name: clusterRef},
			LinkName:        "link-1",
			SourceTopicName: source,
			TopicName:       topicName,
		},
	}
}

func mirrorCluster(ns, name string, tn *v1alpha1.TenancyConfig) *v1alpha1.KafkaCluster {
	return &v1alpha1.KafkaCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       v1alpha1.KafkaClusterSpec{BootstrapServers: "localhost:9092", Tenancy: tn},
	}
}

func TestMirrorValidateCreate_Valid_Allowed(t *testing.T) {
	v := &KafkaMirrorTopicValidator{Reader: newMirrorFakeClient(t)}
	if _, err := v.ValidateCreate(context.Background(), mirrorCR("u1", "team-a", "m1", "prod", "src", "")); err != nil {
		t.Fatalf("unexpected denial: %v", err)
	}
}

func TestMirrorValidateCreate_InvalidShape_Denied(t *testing.T) {
	v := &KafkaMirrorTopicValidator{Reader: newMirrorFakeClient(t)}
	bad := mirrorCR("u1", "team-a", "m1", "prod", "src", "")
	bad.Spec.LinkName = ""
	_, err := v.ValidateCreate(context.Background(), bad)
	if err == nil || !strings.Contains(err.Error(), "spec.linkName is required") {
		t.Fatalf("expected shape error, got %v", err)
	}
}

func TestMirrorValidateCreate_DeletionPolicyDelete_Denied(t *testing.T) {
	v := &KafkaMirrorTopicValidator{Reader: newMirrorFakeClient(t)}
	bad := mirrorCR("u1", "team-a", "m1", "prod", "src", "")
	bad.Spec.DeletionPolicy = "Delete"
	_, err := v.ValidateCreate(context.Background(), bad)
	if err == nil || !strings.Contains(err.Error(), "deletionPolicy") {
		t.Fatalf("expected deletionPolicy rejection, got %v", err)
	}
}

func TestMirrorValidateCreate_CollidesWithMirror_Denied(t *testing.T) {
	other := mirrorCR("u0", "team-a", "m0", "prod", "src.a", "orders")
	v := &KafkaMirrorTopicValidator{Reader: newMirrorFakeClient(t, other)}
	_, err := v.ValidateCreate(context.Background(), mirrorCR("u1", "team-a", "m1", "prod", "src.b", "orders"))
	if err == nil || !strings.Contains(err.Error(), "m0") {
		t.Fatalf("expected collision naming m0, got %v", err)
	}
}

func TestMirrorValidateCreate_CollidesWithTopic_Denied(t *testing.T) {
	tp := topic("ut", "team-a", "t0", "prod", "orders")
	v := &KafkaMirrorTopicValidator{Reader: newMirrorFakeClient(t, tp)}
	// Effective name defaults to sourceTopicName.
	_, err := v.ValidateCreate(context.Background(), mirrorCR("u1", "team-a", "m1", "prod", "orders", ""))
	if err == nil || !strings.Contains(err.Error(), "KafkaTopic") || !strings.Contains(err.Error(), "t0") {
		t.Fatalf("expected collision naming topic t0, got %v", err)
	}
}

func TestMirrorValidateCreate_OtherNamespaceNoOverride_Allowed(t *testing.T) {
	tp := topic("ut", "team-b", "t0", "prod", "orders")
	v := &KafkaMirrorTopicValidator{Reader: newMirrorFakeClient(t, tp)}
	if _, err := v.ValidateCreate(context.Background(), mirrorCR("u1", "team-a", "m1", "prod", "orders", "")); err != nil {
		t.Fatalf("different resolved cluster must not collide: %v", err)
	}
}

func TestMirrorValidateCreate_OtherNamespaceWithOverride_Denied(t *testing.T) {
	tp := topic("ut", "team-b", "t0", "prod", "orders")
	v := &KafkaMirrorTopicValidator{Reader: newMirrorFakeClient(t, tp), ClusterNamespace: "kafka"}
	if _, err := v.ValidateCreate(context.Background(), mirrorCR("u1", "team-a", "m1", "prod", "orders", "")); err == nil {
		t.Fatal("expected cluster-wide collision with the namespace override")
	}
}

func TestMirrorValidateUpdate_SelfIsNotAConflict(t *testing.T) {
	m := mirrorCR("u1", "team-a", "m1", "prod", "src", "")
	v := &KafkaMirrorTopicValidator{Reader: newMirrorFakeClient(t, m)}
	if _, err := v.ValidateUpdate(context.Background(), m, m.DeepCopy()); err != nil {
		t.Fatalf("self update must be allowed: %v", err)
	}
}

func TestMirrorValidateUpdate_Immutable(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(m *v1alpha1.KafkaMirrorTopic)
		want   string
	}{
		{"clusterRef", func(m *v1alpha1.KafkaMirrorTopic) { m.Spec.ClusterRef.Name = "other" }, `spec.clusterRef.name is immutable: prod -> other`},
		{"linkName", func(m *v1alpha1.KafkaMirrorTopic) { m.Spec.LinkName = "link-2" }, `spec.linkName is immutable: link-1 -> link-2`},
		{"sourceTopicName", func(m *v1alpha1.KafkaMirrorTopic) { m.Spec.SourceTopicName = "src2"; m.Spec.TopicName = "src" }, `spec.sourceTopicName is immutable: src -> src2`},
		{"effectiveName", func(m *v1alpha1.KafkaMirrorTopic) { m.Spec.TopicName = "renamed" }, `spec.topicName is immutable: src -> renamed`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			old := mirrorCR("u1", "team-a", "m1", "prod", "src", "")
			nw := old.DeepCopy()
			tc.mutate(nw)
			v := &KafkaMirrorTopicValidator{Reader: newMirrorFakeClient(t, old)}
			_, err := v.ValidateUpdate(context.Background(), old, nw)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestMirrorValidateUpdate_ExplicitNameEqualToSource_Allowed(t *testing.T) {
	old := mirrorCR("u1", "team-a", "m1", "prod", "src", "")
	nw := old.DeepCopy()
	nw.Spec.TopicName = "src"
	v := &KafkaMirrorTopicValidator{Reader: newMirrorFakeClient(t, old)}
	if _, err := v.ValidateUpdate(context.Background(), old, nw); err != nil {
		t.Fatalf("empty -> explicit-same must be allowed: %v", err)
	}
}

func TestMirrorValidateCreate_TenancyDenied(t *testing.T) {
	cl := mirrorCluster("team-a", "prod", &v1alpha1.TenancyConfig{
		TopicPrefixes: []v1alpha1.TopicPrefixRule{{Namespaces: []string{"team-a"}, Prefixes: []string{"allowed."}}},
	})
	v := &KafkaMirrorTopicValidator{Reader: newMirrorFakeClient(t, cl)}

	_, err := v.ValidateCreate(context.Background(), mirrorCR("u1", "team-a", "m1", "prod", "forbidden.x", ""))
	if err == nil || !strings.Contains(err.Error(), "tenancy") {
		t.Fatalf("expected tenancy denial, got %v", err)
	}
	// Tenancy is checked on the effective destination name, not the source.
	if _, err := v.ValidateCreate(context.Background(), mirrorCR("u2", "team-a", "m2", "prod", "forbidden.x", "allowed.x")); err != nil {
		t.Fatalf("effective name satisfies tenancy: %v", err)
	}
}

func TestMirrorValidateCreate_MissingClusterAllowed(t *testing.T) {
	v := &KafkaMirrorTopicValidator{Reader: newMirrorFakeClient(t)}
	if _, err := v.ValidateCreate(context.Background(), mirrorCR("u1", "team-a", "m1", "nocluster", "src", "")); err != nil {
		t.Fatalf("missing cluster must allow: %v", err)
	}
}

func TestMirrorValidateDelete_AlwaysAllowed(t *testing.T) {
	v := &KafkaMirrorTopicValidator{Reader: newMirrorFakeClient(t)}
	if _, err := v.ValidateDelete(context.Background(), mirrorCR("u1", "team-a", "m1", "prod", "src", "")); err != nil {
		t.Fatalf("delete must be allowed: %v", err)
	}
}

// ---- KafkaTopic webhook: cross-kind collisions with mirrors ----

func TestTopicValidateCreate_CollidesWithMirror_Denied(t *testing.T) {
	m := mirrorCR("uid-m", "team-a", "m1", "prod", "src.orders", "orders")
	v := &KafkaTopicValidator{Reader: newMirrorFakeClient(t, m)}

	_, err := v.ValidateCreate(context.Background(), topic("uid-t", "team-a", "t1", "prod", "orders"))
	if err == nil {
		t.Fatal("expected denial: mirror already claims the topic name")
	}
	if !strings.Contains(err.Error(), "KafkaMirrorTopic") || !strings.Contains(err.Error(), "m1") {
		t.Fatalf("rejection should name the conflicting mirror: %v", err)
	}
}

func TestTopicValidateCreate_MirrorDifferentName_Allowed(t *testing.T) {
	m := mirrorCR("uid-m", "team-a", "m1", "prod", "src.orders", "orders")
	v := &KafkaTopicValidator{Reader: newMirrorFakeClient(t, m)}
	if _, err := v.ValidateCreate(context.Background(), topic("uid-t", "team-a", "t1", "prod", "other")); err != nil {
		t.Fatalf("unexpected denial: %v", err)
	}
}

func TestTopicValidateCreate_MirrorOtherNamespaceNoOverride_Allowed(t *testing.T) {
	m := mirrorCR("uid-m", "team-b", "m1", "prod", "src.orders", "orders")
	v := &KafkaTopicValidator{Reader: newMirrorFakeClient(t, m)}
	if _, err := v.ValidateCreate(context.Background(), topic("uid-t", "team-a", "t1", "prod", "orders")); err != nil {
		t.Fatalf("different resolved cluster must not collide: %v", err)
	}
}
