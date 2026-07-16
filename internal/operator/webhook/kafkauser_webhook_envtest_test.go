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

const userWebhookPath = "/validate-gitops-monedula-dev-v1alpha1-kafkauser"

// userValidatingWebhookConfig is the ValidatingWebhookConfiguration for the
// KafkaUser webhook, analogous to quotaValidatingWebhookConfig.
func userValidatingWebhookConfig() *admissionv1.ValidatingWebhookConfiguration {
	fail := admissionv1.Fail
	none := admissionv1.SideEffectClassNone
	scope := admissionv1.AllScopes
	path := userWebhookPath
	return &admissionv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "vkafkauser.gitops.monedula.dev"},
		Webhooks: []admissionv1.ValidatingWebhook{{
			Name:                    "vkafkauser.gitops.monedula.dev",
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
					Resources:   []string{"kafkausers"},
					Scope:       &scope,
				},
			}},
		}},
	}
}

// startUserWebhookEnv starts an envtest environment that registers all five
// validators — KafkaTopic, KafkaQuota, KafkaAccessPolicy, KafkaRoleBinding,
// and KafkaUser — with the user VWC installed. It mirrors
// startRoleBindingWebhookEnv.
func startUserWebhookEnv(t *testing.T) *webhookTestEnv {
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
				userValidatingWebhookConfig(),
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
	if err := (&KafkaUserValidator{Reader: mgr.GetClient()}).SetupWithManager(mgr); err != nil {
		_ = env.Stop()
		t.Fatalf("setting up user validator: %v", err)
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

// newEnvtestUser builds a minimal KafkaUser for the envtest apiserver
// (generate-mode password so shape passes without a real Secret).
func newEnvtestUser(name, ns, clusterRef, username string) *v1alpha1.KafkaUser {
	return &v1alpha1.KafkaUser{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.KafkaUserSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: clusterRef},
			Username:   username,
			Password:   &v1alpha1.UserPassword{Generate: &v1alpha1.GeneratePassword{}},
		},
	}
}

// TestEnvtestUserWebhook_IdentityCollision_Rejected verifies that creating a
// KafkaUser that collides with an existing one on (cluster, username) is
// rejected.
func TestEnvtestUserWebhook_IdentityCollision_Rejected(t *testing.T) {
	env := startUserWebhookEnv(t)
	defer env.stop()
	ctx := context.Background()

	a := newEnvtestUser("user-a", "default", "prod", "alice")
	if err := env.cl.Create(ctx, a); err != nil {
		t.Fatalf("create user-a should be allowed: %v", err)
	}

	b := newEnvtestUser("user-b", "default", "prod", "alice")
	err := env.cl.Create(ctx, b)
	if err == nil {
		t.Fatal("duplicate identity should be rejected")
	}
	if !strings.Contains(err.Error(), "user-a") {
		t.Fatalf("rejection should name the conflicting resource: %v", err)
	}
}

// TestEnvtestUserWebhook_Rename_Rejected verifies that renaming
// spec.username on update is rejected.
func TestEnvtestUserWebhook_Rename_Rejected(t *testing.T) {
	env := startUserWebhookEnv(t)
	defer env.stop()
	ctx := context.Background()

	u := newEnvtestUser("user-rename", "default", "prod", "orig-user")
	if err := env.cl.Create(ctx, u); err != nil {
		t.Fatalf("create should be allowed: %v", err)
	}

	var got v1alpha1.KafkaUser
	if err := env.cl.Get(ctx, client.ObjectKeyFromObject(u), &got); err != nil {
		t.Fatalf("re-get: %v", err)
	}
	got.Spec.Username = "renamed-user"
	if err := env.cl.Update(ctx, &got); err == nil {
		t.Fatal("username mutation should be rejected")
	} else if !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("expected an immutability error: %v", err)
	}
}

// TestEnvtestUserWebhook_InvalidShape_Rejected verifies that a malformed
// KafkaUser (missing password) is rejected at admission by the shape check.
func TestEnvtestUserWebhook_InvalidShape_Rejected(t *testing.T) {
	env := startUserWebhookEnv(t)
	defer env.stop()
	ctx := context.Background()

	bad := newEnvtestUser("user-bad-shape", "default", "prod", "alice")
	bad.Spec.Password = nil
	err := env.cl.Create(ctx, bad)
	if err == nil {
		t.Fatal("invalid-shape user should be rejected")
	}
	if !strings.Contains(err.Error(), "spec.password") {
		t.Fatalf("expected a shape error mentioning spec.password: %v", err)
	}
}

// TestEnvtestUserWebhook_DeleteAlwaysAllowed verifies deletion is never
// blocked by the webhook.
func TestEnvtestUserWebhook_DeleteAlwaysAllowed(t *testing.T) {
	env := startUserWebhookEnv(t)
	defer env.stop()
	ctx := context.Background()

	u := newEnvtestUser("user-del", "default", "prod", "alice")
	if err := env.cl.Create(ctx, u); err != nil {
		t.Fatalf("create should be allowed: %v", err)
	}
	if err := env.cl.Delete(ctx, u); err != nil {
		t.Fatalf("delete must always be allowed: %v", err)
	}
}
