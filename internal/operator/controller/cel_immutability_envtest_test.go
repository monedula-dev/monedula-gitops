//go:build envtest

// CEL immutability envtest suite (review finding I3). These tests prove that
// the x-kubernetes-validations transition rules on the CRD specs are enforced
// BY THE APISERVER ALONE — this package's startEnv installs only the CRDs from
// config/crd, with NO webhook configuration — so the default install
// (webhook.enabled: false) cannot silently orphan broker state by renaming
// identity fields.
//
// Each protected field gets: a mutation rejected with the rule's message, and a
// noop/benign update allowed. KafkaTopic additionally proves the once-set
// semantics: create WITHOUT topicName, then an update SETTING it is allowed
// (unset→set — defaulting resolves topicName from metadata.name in-memory, so
// making it explicit later must pass), and a subsequent CHANGE is rejected.
//
// Run (excluded from the default `go test ./...` by the envtest build tag):
//
//	export KUBEBUILDER_ASSETS="$(setup-envtest use -p path)"
//	go test -tags envtest ./internal/operator/controller/ -run CELImmutability -v
package controller

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

// mustReject asserts err is non-nil and carries the expected CEL message.
func mustReject(t *testing.T, err error, wantMsg, what string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: update should be rejected by the apiserver CEL rule", what)
	}
	if !strings.Contains(err.Error(), wantMsg) {
		t.Fatalf("%s: rejection should carry the CEL message %q; got: %v", what, wantMsg, err)
	}
}

// refetch re-Gets obj by its own key so each Update starts from the persisted
// resourceVersion (a prior rejected Update leaves the local copy dirty).
func refetch(t *testing.T, cl client.Client, obj client.Object) {
	t.Helper()
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(obj), obj); err != nil {
		t.Fatalf("re-get %s: %v", obj.GetName(), err)
	}
}

func TestCELImmutability_TopicNameAndClusterRef(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	topic := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "cel-topic", Namespace: testNamespace},
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "prod"},
			TopicName:  "orders.events",
			Partitions: 3,
		},
	}
	if err := env.cl.Create(ctx, topic); err != nil {
		t.Fatalf("create with topicName should be allowed: %v", err)
	}

	// Changing topicName once set -> rejected with the CEL message.
	refetch(t, env.cl, topic)
	topic.Spec.TopicName = "renamed.events"
	mustReject(t, env.cl.Update(ctx, topic),
		"spec.topicName is immutable once set", "topicName rename")

	// Removing topicName once set -> also rejected (identity would silently
	// fall back to metadata.name).
	refetch(t, env.cl, topic)
	topic.Spec.TopicName = ""
	mustReject(t, env.cl.Update(ctx, topic),
		"spec.topicName is immutable once set", "topicName unset")

	// Repointing clusterRef -> rejected with the CEL message.
	refetch(t, env.cl, topic)
	topic.Spec.ClusterRef.Name = "staging"
	mustReject(t, env.cl.Update(ctx, topic),
		"spec.clusterRef.name is immutable", "topic clusterRef repoint")

	// Benign update (identity untouched) -> allowed.
	refetch(t, env.cl, topic)
	topic.Spec.Partitions = 6
	if err := env.cl.Update(ctx, topic); err != nil {
		t.Fatalf("benign partitions update should be allowed: %v", err)
	}
}

func TestCELImmutability_TopicName_UnsetToSet_Allowed(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	// Create WITHOUT topicName: the identity resolves from metadata.name.
	topic := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "cel-implicit", Namespace: testNamespace},
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "prod"},
			Partitions: 1,
		},
	}
	if err := env.cl.Create(ctx, topic); err != nil {
		t.Fatalf("create without topicName should be allowed: %v", err)
	}

	// unset -> set: allowed (old value was unset; the once-set rule does not bite).
	refetch(t, env.cl, topic)
	topic.Spec.TopicName = "cel-implicit"
	if err := env.cl.Update(ctx, topic); err != nil {
		t.Fatalf("unset->set topicName update should be allowed: %v", err)
	}

	// Now that it is set, changing it -> rejected.
	refetch(t, env.cl, topic)
	topic.Spec.TopicName = "something-else"
	mustReject(t, env.cl.Update(ctx, topic),
		"spec.topicName is immutable once set", "topicName change after set")
}

func TestCELImmutability_AccessPolicyClusterRef(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	pol := &v1alpha1.KafkaAccessPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "cel-policy", Namespace: testNamespace},
		Spec: v1alpha1.KafkaAccessPolicySpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "prod"},
			Rules: []v1alpha1.ACLRule{{
				Principal:  "User:svc",
				Resource:   v1alpha1.ACLResource{Type: "topic", Name: "orders"},
				Operations: []string{"Read"},
			}},
		},
	}
	if err := env.cl.Create(ctx, pol); err != nil {
		t.Fatalf("create should be allowed: %v", err)
	}

	// Repointing clusterRef -> rejected with the CEL message.
	refetch(t, env.cl, pol)
	pol.Spec.ClusterRef.Name = "staging"
	mustReject(t, env.cl.Update(ctx, pol),
		"spec.clusterRef.name is immutable", "policy clusterRef repoint")

	// Rules changes -> allowed (only the cluster identity is immutable).
	refetch(t, env.cl, pol)
	pol.Spec.Rules[0].Operations = []string{"Read", "Describe"}
	if err := env.cl.Update(ctx, pol); err != nil {
		t.Fatalf("rules update should be allowed: %v", err)
	}
}

func TestCELImmutability_QuotaEntity(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	rate := 1024.0
	q := &v1alpha1.KafkaQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "cel-quota", Namespace: testNamespace},
		Spec: v1alpha1.KafkaQuotaSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "prod"},
			Entity:     v1alpha1.QuotaEntity{User: "User:svc-a"},
			Limits:     v1alpha1.QuotaLimits{ProducerByteRate: &rate},
		},
	}
	if err := env.cl.Create(ctx, q); err != nil {
		t.Fatalf("create should be allowed: %v", err)
	}

	// Changing the entity's user -> rejected with the CEL message.
	refetch(t, env.cl, q)
	q.Spec.Entity.User = "User:svc-b"
	mustReject(t, env.cl.Update(ctx, q),
		"spec.entity is immutable", "entity user change")

	// Switching entity dimension entirely -> rejected too.
	refetch(t, env.cl, q)
	q.Spec.Entity = v1alpha1.QuotaEntity{UserDefault: true}
	mustReject(t, env.cl.Update(ctx, q),
		"spec.entity is immutable", "entity dimension change")

	// Repointing clusterRef -> rejected with the CEL message.
	refetch(t, env.cl, q)
	q.Spec.ClusterRef.Name = "staging"
	mustReject(t, env.cl.Update(ctx, q),
		"spec.clusterRef.name is immutable", "quota clusterRef repoint")

	// Limits changes -> allowed (entity and clusterRef untouched).
	refetch(t, env.cl, q)
	doubled := 2048.0
	q.Spec.Limits.ProducerByteRate = &doubled
	if err := env.cl.Update(ctx, q); err != nil {
		t.Fatalf("limits update should be allowed: %v", err)
	}
}

func TestCELImmutability_RoleBindingIdentitySet(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	rb := &v1alpha1.KafkaRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "cel-rb", Namespace: testNamespace},
		Spec: v1alpha1.KafkaRoleBindingSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "prod"},
			Principal:  "User:svc",
			Role:       "DeveloperRead",
			Scope:      v1alpha1.RoleBindingScope{Type: "kafka"},
			Resources: []v1alpha1.RoleResource{{
				Type: "Topic", Name: "orders", PatternType: "literal",
			}},
		},
	}
	if err := env.cl.Create(ctx, rb); err != nil {
		t.Fatalf("create should be allowed: %v", err)
	}

	// Each identity field mutation -> rejected with its CEL message.
	refetch(t, env.cl, rb)
	rb.Spec.ClusterRef.Name = "staging"
	mustReject(t, env.cl.Update(ctx, rb),
		"spec.clusterRef.name is immutable", "rolebinding clusterRef repoint")

	refetch(t, env.cl, rb)
	rb.Spec.Principal = "User:other"
	mustReject(t, env.cl.Update(ctx, rb),
		"spec.principal is immutable", "rolebinding principal change")

	refetch(t, env.cl, rb)
	rb.Spec.Role = "DeveloperWrite"
	mustReject(t, env.cl.Update(ctx, rb),
		"spec.role is immutable", "rolebinding role change")

	refetch(t, env.cl, rb)
	rb.Spec.Scope.Type = "schema-registry"
	mustReject(t, env.cl.Update(ctx, rb),
		"spec.scope.type is immutable", "rolebinding scope.type change")

	// Resources changes -> allowed (explicitly mutable; the reconciler converges
	// the MDS binding set).
	refetch(t, env.cl, rb)
	rb.Spec.Resources = append(rb.Spec.Resources, v1alpha1.RoleResource{
		Type: "Topic", Name: "payments", PatternType: "literal",
	})
	if err := env.cl.Update(ctx, rb); err != nil {
		t.Fatalf("resources update should be allowed: %v", err)
	}
}
