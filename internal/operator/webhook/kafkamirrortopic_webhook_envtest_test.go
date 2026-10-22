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

const mirrorWebhookPath = "/validate-gitops-monedula-dev-v1alpha1-kafkamirrortopic"

// mirrorValidatingWebhookConfig is the ValidatingWebhookConfiguration for the
// KafkaMirrorTopic webhook, analogous to userValidatingWebhookConfig.
func mirrorValidatingWebhookConfig() *admissionv1.ValidatingWebhookConfiguration {
	fail := admissionv1.Fail
	none := admissionv1.SideEffectClassNone
	scope := admissionv1.AllScopes
	path := mirrorWebhookPath
	return &admissionv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "vkafkamirrortopic.gitops.monedula.dev"},
		Webhooks: []admissionv1.ValidatingWebhook{{
			Name:                    "vkafkamirrortopic.gitops.monedula.dev",
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
					Resources:   []string{"kafkamirrortopics"},
					Scope:       &scope,
				},
			}},
		}},
	}
}

// startMirrorWebhookEnv starts an envtest environment that registers the
// KafkaTopic and KafkaMirrorTopic validators (both VWCs installed) so the
// cross-kind collision checks run against a real apiserver.
func startMirrorWebhookEnv(t *testing.T) *webhookTestEnv {
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
				mirrorValidatingWebhookConfig(),
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
	if err := (&KafkaMirrorTopicValidator{Reader: mgr.GetClient()}).SetupWithManager(mgr); err != nil {
		_ = env.Stop()
		t.Fatalf("setting up mirror validator: %v", err)
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

// newEnvtestMirror builds a minimal KafkaMirrorTopic for the envtest apiserver.
func newEnvtestMirror(name, ns, clusterRef, source, topicName string) *v1alpha1.KafkaMirrorTopic {
	return &v1alpha1.KafkaMirrorTopic{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.KafkaMirrorTopicSpec{
			ClusterRef:      v1alpha1.ClusterRef{Name: clusterRef},
			LinkName:        "link-1",
			SourceTopicName: source,
			TopicName:       topicName,
		},
	}
}

func TestEnvtestMirrorWebhook_CollidesWithTopic_Rejected(t *testing.T) {
	env := startMirrorWebhookEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newTopic("topic-a", "default", "prod", "orders")); err != nil {
		t.Fatalf("create topic should be allowed: %v", err)
	}
	err := env.cl.Create(ctx, newEnvtestMirror("mirror-a", "default", "prod", "orders", ""))
	if err == nil {
		t.Fatal("mirror colliding with a KafkaTopic should be rejected")
	}
	if !strings.Contains(err.Error(), "topic-a") {
		t.Fatalf("rejection should name the conflicting topic: %v", err)
	}
}

func TestEnvtestTopicWebhook_CollidesWithMirror_Rejected(t *testing.T) {
	env := startMirrorWebhookEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newEnvtestMirror("mirror-b", "default", "prod", "src.orders", "orders")); err != nil {
		t.Fatalf("create mirror should be allowed: %v", err)
	}
	err := env.cl.Create(ctx, newTopic("topic-b", "default", "prod", "orders"))
	if err == nil {
		t.Fatal("topic colliding with a KafkaMirrorTopic should be rejected")
	}
	if !strings.Contains(err.Error(), "mirror-b") {
		t.Fatalf("rejection should name the conflicting mirror: %v", err)
	}
}

func TestEnvtestMirrorWebhook_EffectiveNameChange_Rejected(t *testing.T) {
	env := startMirrorWebhookEnv(t)
	defer env.stop()
	ctx := context.Background()

	m := newEnvtestMirror("mirror-rename", "default", "prod", "src", "")
	if err := env.cl.Create(ctx, m); err != nil {
		t.Fatalf("create should be allowed: %v", err)
	}
	var got v1alpha1.KafkaMirrorTopic
	if err := env.cl.Get(ctx, client.ObjectKeyFromObject(m), &got); err != nil {
		t.Fatalf("re-get: %v", err)
	}
	got.Spec.TopicName = "renamed"
	if err := env.cl.Update(ctx, &got); err == nil {
		t.Fatal("effective name change should be rejected")
	} else if !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("expected an immutability error: %v", err)
	}
}

func TestEnvtestMirrorWebhook_DeleteAlwaysAllowed(t *testing.T) {
	env := startMirrorWebhookEnv(t)
	defer env.stop()
	ctx := context.Background()

	m := newEnvtestMirror("mirror-del", "default", "prod", "src", "")
	if err := env.cl.Create(ctx, m); err != nil {
		t.Fatalf("create should be allowed: %v", err)
	}
	if err := env.cl.Delete(ctx, m); err != nil {
		t.Fatalf("delete must always be allowed: %v", err)
	}
}
