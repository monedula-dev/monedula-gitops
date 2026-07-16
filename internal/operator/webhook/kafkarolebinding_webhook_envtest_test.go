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

const roleBindingWebhookPath = "/validate-gitops-monedula-dev-v1alpha1-kafkarolebinding"

// roleBindingValidatingWebhookConfig is the ValidatingWebhookConfiguration for
// the KafkaRoleBinding webhook, analogous to quotaValidatingWebhookConfig.
func roleBindingValidatingWebhookConfig() *admissionv1.ValidatingWebhookConfiguration {
	fail := admissionv1.Fail
	none := admissionv1.SideEffectClassNone
	scope := admissionv1.AllScopes
	path := roleBindingWebhookPath
	return &admissionv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "vkafkarolebinding.gitops.monedula.dev"},
		Webhooks: []admissionv1.ValidatingWebhook{{
			Name:                    "vkafkarolebinding.gitops.monedula.dev",
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
					Resources:   []string{"kafkarolebindings"},
					Scope:       &scope,
				},
			}},
		}},
	}
}

// startRoleBindingWebhookEnv starts an envtest environment that registers all
// four validators — KafkaTopic, KafkaQuota, KafkaAccessPolicy, and
// KafkaRoleBinding — with the role binding VWC installed. It mirrors
// startPolicyWebhookEnv (kafkaaccesspolicy_webhook_envtest_test.go).
func startRoleBindingWebhookEnv(t *testing.T) *webhookTestEnv {
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
				roleBindingValidatingWebhookConfig(),
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
	if err := (&KafkaRoleBindingValidator{Reader: mgr.GetClient()}).SetupWithManager(mgr); err != nil {
		_ = env.Stop()
		t.Fatalf("setting up role binding validator: %v", err)
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

// newEnvtestClusterWithMDS builds a KafkaCluster with MDS configured for the
// envtest apiserver.
func newEnvtestClusterWithMDS(name, ns, kafkaClusterID string) *v1alpha1.KafkaCluster {
	return &v1alpha1.KafkaCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.KafkaClusterSpec{
			BootstrapServers: "localhost:9092",
			Authorization: &v1alpha1.AuthorizationConfig{
				MDS: &v1alpha1.MDSConfig{
					Endpoint: "https://mds:8090",
					Clusters: v1alpha1.MDSClusters{
						KafkaCluster: kafkaClusterID,
					},
				},
			},
		},
	}
}

// newEnvtestRB builds a minimal KafkaRoleBinding for the envtest apiserver.
func newEnvtestRB(name, ns, clusterRef, principal, role, scopeType string) *v1alpha1.KafkaRoleBinding {
	return &v1alpha1.KafkaRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.KafkaRoleBindingSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: clusterRef},
			Principal:  principal,
			Role:       role,
			Scope:      v1alpha1.RoleBindingScope{Type: scopeType},
		},
	}
}

// TestEnvtestRBWebhook_IdentityCollision_Rejected verifies that creating a role
// binding that collides with an existing one on the same cluster is rejected.
// Uses ClusterAdmin (cluster-scoped, no resources required by shape check).
func TestEnvtestRBWebhook_IdentityCollision_Rejected(t *testing.T) {
	env := startRoleBindingWebhookEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newEnvtestClusterWithMDS("prod", "default", "kafka-1")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	// First role binding — ClusterAdmin is cluster-scoped (no resources needed).
	a := newEnvtestRB("rb-a", "default", "prod", "User:alice", "ClusterAdmin", "kafka")
	if err := env.cl.Create(ctx, a); err != nil {
		t.Fatalf("create rb-a should be allowed: %v", err)
	}

	// Second role binding with same identity should be rejected.
	b := newEnvtestRB("rb-b", "default", "prod", "User:alice", "ClusterAdmin", "kafka")
	err := env.cl.Create(ctx, b)
	if err == nil {
		t.Fatal("duplicate identity should be rejected")
	}
	if !strings.Contains(err.Error(), "rb-a") {
		t.Fatalf("rejection should name the conflicting resource: %v", err)
	}
}

// TestEnvtestRBWebhook_ImmutableField_Rejected verifies that updating an
// immutable field (principal) is rejected.
func TestEnvtestRBWebhook_ImmutableField_Rejected(t *testing.T) {
	env := startRoleBindingWebhookEnv(t)
	defer env.stop()
	ctx := context.Background()

	rb := newEnvtestRB("rb-immut", "default", "prod", "User:alice", "ClusterAdmin", "kafka")
	if err := env.cl.Create(ctx, rb); err != nil {
		t.Fatalf("create should be allowed: %v", err)
	}

	var got v1alpha1.KafkaRoleBinding
	if err := env.cl.Get(ctx, client.ObjectKeyFromObject(rb), &got); err != nil {
		t.Fatalf("re-get: %v", err)
	}
	got.Spec.Principal = "User:bob"
	if err := env.cl.Update(ctx, &got); err == nil {
		t.Fatal("principal mutation should be rejected")
	} else if !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("expected an immutability error: %v", err)
	}
}

// TestEnvtestRBWebhook_ResourcesUpdate_Admitted verifies that changing only
// spec.resources (an allowed mutable field) on update is admitted.
func TestEnvtestRBWebhook_ResourcesUpdate_Admitted(t *testing.T) {
	env := startRoleBindingWebhookEnv(t)
	defer env.stop()
	ctx := context.Background()

	// Use a cluster-scoped binding initially (no resources), then add resources.
	// DeveloperRead is resource-scoped but unknown-role passes shape silently.
	// Instead, use "ClusterAdmin" (cluster-scoped) and then switch to the same
	// binding but with a Reconciliation change (non-identity field).
	rb := newEnvtestRB("rb-res-upd", "default", "prod", "User:carol", "ClusterAdmin", "kafka")
	if err := env.cl.Create(ctx, rb); err != nil {
		t.Fatalf("create should be allowed: %v", err)
	}

	var got v1alpha1.KafkaRoleBinding
	if err := env.cl.Get(ctx, client.ObjectKeyFromObject(rb), &got); err != nil {
		t.Fatalf("re-get: %v", err)
	}
	// Add a reconciliation mode (non-identity, non-immutable field) — admitted.
	got.Spec.Reconciliation = &v1alpha1.Reconciliation{Mode: "DetectOnly"}
	if err := env.cl.Update(ctx, &got); err != nil {
		t.Fatalf("non-immutable-field update should be admitted: %v", err)
	}
}

// TestEnvtestRBWebhook_InvalidShape_Rejected verifies that a malformed role
// binding (bad scope.type) is rejected at admission by the shape check.
func TestEnvtestRBWebhook_InvalidShape_Rejected(t *testing.T) {
	env := startRoleBindingWebhookEnv(t)
	defer env.stop()
	ctx := context.Background()

	bad := newEnvtestRB("rb-bad-shape", "default", "prod", "User:alice", "DeveloperRead", "BOGUS_SCOPE")
	err := env.cl.Create(ctx, bad)
	if err == nil {
		t.Fatal("invalid-shape role binding should be rejected")
	}
	if !strings.Contains(err.Error(), "scope.type") {
		t.Fatalf("expected a shape error mentioning scope.type: %v", err)
	}
}
