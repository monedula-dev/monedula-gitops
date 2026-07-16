//go:build envtest

package webhook

import (
	"context"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

const policyWebhookPath = "/validate-gitops-monedula-dev-v1alpha1-kafkaaccesspolicy"

// policyValidatingWebhookConfig is the ValidatingWebhookConfiguration for the
// KafkaAccessPolicy webhook, analogous to quotaValidatingWebhookConfig.
func policyValidatingWebhookConfig() *admissionv1.ValidatingWebhookConfiguration {
	fail := admissionv1.Fail
	none := admissionv1.SideEffectClassNone
	scope := admissionv1.AllScopes
	path := policyWebhookPath
	return &admissionv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "vkafkaaccesspolicy.gitops.monedula.dev"},
		Webhooks: []admissionv1.ValidatingWebhook{{
			Name:                    "vkafkaaccesspolicy.gitops.monedula.dev",
			FailurePolicy:           &fail,
			SideEffects:             &none,
			AdmissionReviewVersions: []string{"v1"},
			ClientConfig: admissionv1.WebhookClientConfig{
				Service: &admissionv1.ServiceReference{
					Name:      "webhook-service",
					Namespace: "default",
					Path:      &path,
				},
			},
			Rules: []admissionv1.RuleWithOperations{{
				Operations: []admissionv1.OperationType{
					admissionv1.Create, admissionv1.Update,
				},
				Rule: admissionv1.Rule{
					APIGroups:   []string{"gitops.monedula.dev"},
					APIVersions: []string{"v1alpha1"},
					Resources:   []string{"kafkaaccesspolicies"},
					Scope:       &scope,
				},
			}},
		}},
	}
}

// startPolicyWebhookEnv starts an envtest environment that registers all three
// validators — KafkaTopic, KafkaQuota, and KafkaAccessPolicy — with the
// policy VWC installed. It mirrors startWebhookEnv (suite_envtest_test.go)
// but includes the policy webhook in both the ValidatingWebhookInstallOptions
// and the manager setup.
func startPolicyWebhookEnv(t *testing.T) *webhookTestEnv {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("adding v1alpha1: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("adding corev1: %v", err)
	}
	if err := admissionv1.AddToScheme(scheme); err != nil {
		t.Fatalf("adding admissionv1: %v", err)
	}

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{crdDir},
		ErrorIfCRDPathMissing: true,
		Scheme:                scheme,
		WebhookInstallOptions: envtest.WebhookInstallOptions{
			ValidatingWebhooks: []*admissionv1.ValidatingWebhookConfiguration{
				validatingWebhookConfig(),
				quotaValidatingWebhookConfig(),
				policyValidatingWebhookConfig(),
			},
		},
	}

	cfg, err := env.Start()
	if err != nil {
		if isAssetsUnavailable(err) {
			t.Skip("envtest assets unavailable; run: setup-envtest use & export KUBEBUILDER_ASSETS")
		}
		t.Fatalf("starting envtest: %v", err)
	}

	wo := env.WebhookInstallOptions
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		WebhookServer: webhook.NewServer(webhook.Options{
			Host:    wo.LocalServingHost,
			Port:    wo.LocalServingPort,
			CertDir: wo.LocalServingCertDir,
		}),
	})
	if err != nil {
		_ = env.Stop()
		t.Fatalf("creating manager: %v", err)
	}

	if err := RegisterIndexes(context.Background(), mgr); err != nil {
		_ = env.Stop()
		t.Fatalf("registering indexes: %v", err)
	}
	if err := (&KafkaTopicValidator{Reader: mgr.GetClient()}).SetupWithManager(mgr); err != nil {
		_ = env.Stop()
		t.Fatalf("setting up topic validator: %v", err)
	}
	if err := (&KafkaQuotaValidator{Reader: mgr.GetClient()}).SetupWithManager(mgr); err != nil {
		_ = env.Stop()
		t.Fatalf("setting up quota validator: %v", err)
	}
	if err := (&KafkaAccessPolicyValidator{Reader: mgr.GetClient()}).SetupWithManager(mgr); err != nil {
		_ = env.Stop()
		t.Fatalf("setting up access policy validator: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	mgrDone := make(chan error, 1)
	go func() { mgrDone <- mgr.Start(ctx) }()

	if !mgr.GetCache().WaitForCacheSync(ctx) {
		cancel()
		_ = env.Stop()
		t.Fatal("cache did not sync")
	}
	waitForWebhookServer(t, wo.LocalServingHost, wo.LocalServingPort)

	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		cancel()
		_ = env.Stop()
		t.Fatalf("building client: %v", err)
	}

	return &webhookTestEnv{
		cl: cl,
		stop: func() {
			cancel()
			<-mgrDone
			_ = env.Stop()
		},
	}
}

// newEnvtestPolicy builds a valid KafkaAccessPolicy for the envtest apiserver
// (no UID; the apiserver assigns one).
func newEnvtestPolicy(name, ns, clusterRef, principal, resType, resName, permission string, ops []string) *v1alpha1.KafkaAccessPolicy {
	if permission == "" {
		permission = "Allow"
	}
	return &v1alpha1.KafkaAccessPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.KafkaAccessPolicySpec{
			ClusterRef: v1alpha1.ClusterRef{Name: clusterRef},
			Rules: []v1alpha1.ACLRule{{
				Principal:  principal,
				Permission: permission,
				Resource:   v1alpha1.ACLResource{Type: resType, Name: resName, PatternType: "literal"},
				Operations: ops,
			}},
		},
	}
}

// newEnvtestCluster builds a minimal KafkaCluster for the envtest apiserver.
func newEnvtestCluster(name, ns string) *v1alpha1.KafkaCluster {
	return &v1alpha1.KafkaCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       v1alpha1.KafkaClusterSpec{BootstrapServers: "localhost:9092"},
	}
}

// TestEnvtestPolicyWebhook_ConflictsWithExistingPolicy verifies that creating a
// policy that Denies a tuple already Allowed by another existing policy is
// rejected at admission.
func TestEnvtestPolicyWebhook_ConflictsWithExistingPolicy(t *testing.T) {
	env := startPolicyWebhookEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newEnvtestCluster("prod", "default")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	// First policy: Allow.
	allow := newEnvtestPolicy("pol-allow", "default", "prod", "User:alice", "topic", "orders", "Allow", []string{"Read"})
	if err := env.cl.Create(ctx, allow); err != nil {
		t.Fatalf("create pol-allow should be admitted: %v", err)
	}

	// Second policy: Deny same tuple -> conflict -> rejected.
	deny := newEnvtestPolicy("pol-deny", "default", "prod", "User:alice", "topic", "orders", "Deny", []string{"Read"})
	err := env.cl.Create(ctx, deny)
	if err == nil {
		t.Fatal("pol-deny should be rejected due to conflict with pol-allow")
	}
	if !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("expected conflict error, got: %v", err)
	}
}

// TestEnvtestPolicyWebhook_ConflictsWithExistingTopic verifies that creating a
// policy that Denies a tuple already Allowed by a KafkaTopic's inline access is
// rejected at admission.
func TestEnvtestPolicyWebhook_ConflictsWithExistingTopic(t *testing.T) {
	env := startPolicyWebhookEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newEnvtestCluster("prod", "default")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	// Existing topic with producer access (Allow) for User:bob.
	existingTopic := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "orders-topic", Namespace: "default"},
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "prod"},
			TopicName:  "orders",
			Partitions: 1,
			Access: v1alpha1.TopicAccess{
				Producers: []v1alpha1.ProducerAccess{{
					Principal:  "User:bob",
					Operations: []string{"Write"},
				}},
			},
		},
	}
	if err := env.cl.Create(ctx, existingTopic); err != nil {
		t.Fatalf("create topic should be admitted: %v", err)
	}

	// Policy denying the same Write tuple -> conflict with the topic.
	deny := newEnvtestPolicy("pol-deny-topic", "default", "prod", "User:bob", "topic", "orders", "Deny", []string{"Write"})
	err := env.cl.Create(ctx, deny)
	if err == nil {
		t.Fatal("pol-deny-topic should be rejected: conflicts with existing topic")
	}
	if !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("expected conflict error, got: %v", err)
	}
}

// TestEnvtestPolicyWebhook_NonConflicting verifies that a non-conflicting
// policy is admitted cleanly.
func TestEnvtestPolicyWebhook_NonConflicting(t *testing.T) {
	env := startPolicyWebhookEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newEnvtestCluster("prod", "default")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	// First policy on principal alice.
	a := newEnvtestPolicy("pol-alice", "default", "prod", "User:alice", "topic", "orders", "Allow", []string{"Read"})
	if err := env.cl.Create(ctx, a); err != nil {
		t.Fatalf("create pol-alice should be admitted: %v", err)
	}

	// Second policy on a DIFFERENT principal (bob) — no conflict.
	b := newEnvtestPolicy("pol-bob", "default", "prod", "User:bob", "topic", "orders", "Deny", []string{"Read"})
	if err := env.cl.Create(ctx, b); err != nil {
		t.Fatalf("create pol-bob (different principal) should be admitted: %v", err)
	}
}

// TestEnvtestPolicyWebhook_InvalidShape verifies that a malformed policy is
// rejected at admission by the webhook shape check. An invalid operation
// ("BOGUS_OP") is used because the CRD structural schema does not enumerate
// valid operations — that check lives only in the webhook and the CLI lint.
func TestEnvtestPolicyWebhook_InvalidShape(t *testing.T) {
	env := startPolicyWebhookEnv(t)
	defer env.stop()
	ctx := context.Background()

	// Invalid operation: the CRD allows any string in operations[]; the webhook
	// rejects it with an "invalid operation" shape error (see
	// internal/validation.opError, which produces the substring "invalid operation").
	bad := newEnvtestPolicy("pol-bad-shape", "default", "prod", "User:alice", "topic", "orders", "Allow", []string{"BOGUS_OP"})
	err := env.cl.Create(ctx, bad)
	if err == nil {
		t.Fatal("policy with invalid operation should be rejected by shape check")
	}
	if !strings.Contains(err.Error(), "invalid operation") {
		t.Fatalf("expected shape error containing %q, got: %v", "invalid operation", err)
	}
}

// TestEnvtestPolicyWebhook_UpdateIntroducesConflict verifies that updating an
// admitted policy to introduce a conflict with an existing one is rejected.
func TestEnvtestPolicyWebhook_UpdateIntroducesConflict(t *testing.T) {
	env := startPolicyWebhookEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newEnvtestCluster("prod", "default")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	// Existing allow policy.
	allow := newEnvtestPolicy("pol-allow-upd", "default", "prod", "User:carol", "topic", "events", "Allow", []string{"Read"})
	if err := env.cl.Create(ctx, allow); err != nil {
		t.Fatalf("create pol-allow-upd: %v", err)
	}

	// Create a neutral policy (different principal).
	neutral := newEnvtestPolicy("pol-neutral", "default", "prod", "User:dave", "topic", "events", "Allow", []string{"Read"})
	if err := env.cl.Create(ctx, neutral); err != nil {
		t.Fatalf("create pol-neutral: %v", err)
	}

	// Re-fetch to get the server-assigned ResourceVersion.
	var got v1alpha1.KafkaAccessPolicy
	if err := env.cl.Get(ctx, client.ObjectKeyFromObject(neutral), &got); err != nil {
		t.Fatalf("re-get pol-neutral: %v", err)
	}

	// Update pol-neutral to Deny User:carol Read on events — now conflicts with pol-allow-upd.
	got.Spec.Rules = []v1alpha1.ACLRule{{
		Principal:  "User:carol",
		Permission: "Deny",
		Resource:   v1alpha1.ACLResource{Type: "topic", Name: "events", PatternType: "literal"},
		Operations: []string{"Read"},
	}}
	err := env.cl.Update(ctx, &got)
	if err == nil {
		t.Fatal("update introducing a conflict should be rejected")
	}
	if !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("expected conflict error, got: %v", err)
	}
}
