//go:build envtest

package webhook

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

func TestEnvtestQuotaWebhook_IdentityUniqueness(t *testing.T) {
	env := startWebhookEnv(t, "")
	defer env.stop()
	ctx := context.Background()

	a := newQuota("quota-a", "default", "prod", "User:alice")
	if err := env.cl.Create(ctx, a); err != nil {
		t.Fatalf("create A should be allowed: %v", err)
	}

	// Duplicate identity in the same namespace -> rejected with the identity msg.
	b := newQuota("quota-b", "default", "prod", "User:alice")
	err := env.cl.Create(ctx, b)
	if err == nil {
		t.Fatal("duplicate B should be rejected")
	}
	if !strings.Contains(err.Error(), "user=alice") {
		t.Fatalf("rejection should name the contested entity: %v", err)
	}
}

func TestEnvtestQuotaWebhook_EntityImmutable(t *testing.T) {
	env := startWebhookEnv(t, "")
	defer env.stop()
	ctx := context.Background()

	a := newQuota("quota-immutable", "default", "prod", "User:alice")
	if err := env.cl.Create(ctx, a); err != nil {
		t.Fatalf("create should be allowed: %v", err)
	}

	var got v1alpha1.KafkaQuota
	if err := env.cl.Get(ctx, client.ObjectKeyFromObject(a), &got); err != nil {
		t.Fatalf("re-get: %v", err)
	}
	got.Spec.Entity.User = "User:bob"
	if err := env.cl.Update(ctx, &got); err == nil {
		t.Fatal("entity mutation should be rejected")
	} else if !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("expected an immutability error: %v", err)
	}
}

func TestEnvtestQuotaWebhook_InvalidShape(t *testing.T) {
	env := startWebhookEnv(t, "")
	defer env.stop()
	ctx := context.Background()

	// user AND userDefault set -> mutually exclusive shape violation at admission.
	bad := newQuota("quota-bad", "default", "prod", "User:alice")
	bad.Spec.Entity.UserDefault = true
	if err := env.cl.Create(ctx, bad); err == nil {
		t.Fatal("invalid-shape create should be rejected")
	} else if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("expected a shape error: %v", err)
	}
}

func TestEnvtestQuotaWebhook_ValidCreate(t *testing.T) {
	env := startWebhookEnv(t, "")
	defer env.stop()
	ctx := context.Background()

	good := newQuota("quota-good", "default", "prod", "User:carol")
	if err := env.cl.Create(ctx, good); err != nil {
		t.Fatalf("valid create should be allowed: %v", err)
	}
}

// TestEnvtestQuotaWebhook_ClusterWideScope verifies that when ClusterNamespace
// is set ("kafka"), two KafkaQuotas with the SAME entity and the SAME clusterRef
// but in DIFFERENT namespaces are still rejected as a collision — the
// cluster-namespace override makes identity scope cluster-wide (spec §39.5,
// §20.2): (clusterRef, entity) is unique across all namespaces, not just within
// the creating namespace.
func TestEnvtestQuotaWebhook_ClusterWideScope(t *testing.T) {
	env := startWebhookEnv(t, "kafka")
	defer env.stop()
	ctx := context.Background()

	// Ensure a second namespace "tenant-b" exists for the cross-namespace check.
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "tenant-b"}}
	if err := env.cl.Create(ctx, ns); err != nil {
		t.Fatalf("create namespace tenant-b: %v", err)
	}

	// First quota in "default" namespace: should be accepted.
	a := newQuota("quota-cw-a", "default", "prod", "User:dave")
	if err := env.cl.Create(ctx, a); err != nil {
		t.Fatalf("create A should be allowed: %v", err)
	}

	// Second quota with the SAME entity + clusterRef but in "tenant-b": with
	// cluster-wide scope (ClusterNamespace="kafka") the two quotas resolve to the
	// same identity and must be rejected.
	b := newQuota("quota-cw-b", "tenant-b", "prod", "User:dave")
	err := env.cl.Create(ctx, b)
	if err == nil {
		t.Fatal("cross-namespace duplicate with cluster-wide scope should be rejected")
	}
	if !strings.Contains(err.Error(), "user=dave") {
		t.Fatalf("rejection should name the contested entity: %v", err)
	}
}

// newQuota builds a minimal KafkaQuota CR for the envtest API server (no UID;
// the apiserver assigns one). A single producer-byte-rate limit satisfies the
// at-least-one-limit shape rule.
func newQuota(name, ns, clusterRef, user string) *v1alpha1.KafkaQuota {
	rate := 1024.0
	return &v1alpha1.KafkaQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.KafkaQuotaSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: clusterRef},
			Entity:     v1alpha1.QuotaEntity{User: user},
			Limits:     v1alpha1.QuotaLimits{ProducerByteRate: &rate},
		},
	}
}
