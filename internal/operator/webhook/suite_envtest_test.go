//go:build envtest

// Package webhook's envtest suite (Task 10) exercises the KafkaTopic identity
// validating webhook against a REAL Kubernetes apiserver (started via
// controller-runtime's envtest), with the webhook server bound to the
// envtest-provided serving cert/host/port and a ValidatingWebhookConfiguration
// registered so the apiserver actually calls the webhook on create/update.
//
// Unlike the controllers' envtest suite (which drives Reconcile directly), this
// suite MUST run a real manager with the webhook server, because the admission
// path is exercised by the apiserver through the network — there is no way to
// drive it synchronously. The CRDs from config/crd are installed so KafkaTopic /
// KafkaCluster objects can be created.
//
// EXCLUDED from the default `go test ./...` by the //go:build envtest tag.
// Run with:
//
//	export KUBEBUILDER_ASSETS="$(setup-envtest use -p path)"
//	go test -tags envtest ./internal/operator/webhook/ -v
//
// SKIPS cleanly when the control-plane binaries are unavailable (mirrors the
// controller suite's isAssetsUnavailable pattern).
package webhook

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
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

const (
	crdDir           = "../../../config/crd"
	webhookPath      = "/validate-gitops-monedula-dev-v1alpha1-kafkatopic"
	quotaWebhookPath = "/validate-gitops-monedula-dev-v1alpha1-kafkaquota"
)

func init() {
	ctrl.SetLogger(logr.Discard())
}

// webhookTestEnv holds the started control plane + a client wired to the
// webhook-fronted apiserver, plus the manager-stop func.
type webhookTestEnv struct {
	cl   client.Client
	stop func()
}

// isAssetsUnavailable mirrors the controller suite: reports whether envtest's
// control-plane binaries are unavailable so a binary-less run skips.
func isAssetsUnavailable(err error) bool {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		return true
	}
	msg := err.Error()
	for _, needle := range []string{
		"unable to find", "no such file", "executable file not found",
		"KUBEBUILDER_ASSETS", "failed to start the controlplane",
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

// validatingWebhookConfig builds the ValidatingWebhookConfiguration installed
// into envtest. failurePolicy Fail, sideEffects None, admissionReviewVersions
// v1, rules create+update on kafkatopics. The clientConfig service is a stub;
// envtest rewrites it to point at the local serving host/port + injects the CA.
func validatingWebhookConfig() *admissionv1.ValidatingWebhookConfiguration {
	fail := admissionv1.Fail
	none := admissionv1.SideEffectClassNone
	scope := admissionv1.AllScopes
	path := webhookPath
	return &admissionv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "vkafkatopic.gitops.monedula.dev"},
		Webhooks: []admissionv1.ValidatingWebhook{{
			Name:                    "vkafkatopic.gitops.monedula.dev",
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
					Resources:   []string{"kafkatopics"},
					Scope:       &scope,
				},
			}},
		}},
	}
}

// quotaValidatingWebhookConfig is the analogous VWC for the KafkaQuota webhook.
func quotaValidatingWebhookConfig() *admissionv1.ValidatingWebhookConfiguration {
	fail := admissionv1.Fail
	none := admissionv1.SideEffectClassNone
	scope := admissionv1.AllScopes
	path := quotaWebhookPath
	return &admissionv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "vkafkaquota.gitops.monedula.dev"},
		Webhooks: []admissionv1.ValidatingWebhook{{
			Name:                    "vkafkaquota.gitops.monedula.dev",
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
					Resources:   []string{"kafkaquotas"},
					Scope:       &scope,
				},
			}},
		}},
	}
}

// startWebhookEnv starts envtest with the CRDs + the ValidatingWebhookConfig,
// runs a manager whose webhook server is bound to the envtest serving
// host/port/certdir, registers the validator and the field index, and waits for
// the webhook server to be ready before returning.
func startWebhookEnv(t *testing.T, clusterNamespace string) *webhookTestEnv {
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
	if err := (&KafkaTopicValidator{Reader: mgr.GetClient(), ClusterNamespace: clusterNamespace}).SetupWithManager(mgr); err != nil {
		_ = env.Stop()
		t.Fatalf("setting up topic validator: %v", err)
	}
	if err := (&KafkaQuotaValidator{Reader: mgr.GetClient(), ClusterNamespace: clusterNamespace}).SetupWithManager(mgr); err != nil {
		_ = env.Stop()
		t.Fatalf("setting up quota validator: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	mgrDone := make(chan error, 1)
	go func() { mgrDone <- mgr.Start(ctx) }()

	// Wait for the manager cache to sync (the validator lists from it).
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		cancel()
		_ = env.Stop()
		t.Fatal("cache did not sync")
	}
	// Wait for the webhook server's TLS listener to accept connections.
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

// waitForWebhookServer polls the serving cert dir + the TLS port until the
// webhook server is up, so the first create is not racing the listener.
func waitForWebhookServer(t *testing.T, host string, port int) {
	t.Helper()
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", addr,
			&tls.Config{InsecureSkipVerify: true}) //nolint:gosec // test-only liveness probe
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("webhook server did not become ready in time")
}

func TestEnvtestWebhook_IdentityUniqueness(t *testing.T) {
	env := startWebhookEnv(t, "")
	defer env.stop()
	ctx := context.Background()

	a := newTopic("topic-a", "default", "prod", "shared.events")
	if err := env.cl.Create(ctx, a); err != nil {
		t.Fatalf("create A should be allowed: %v", err)
	}

	// Duplicate identity in the same namespace -> rejected with the identity msg.
	b := newTopic("topic-b", "default", "prod", "shared.events")
	err := env.cl.Create(ctx, b)
	if err == nil {
		t.Fatal("duplicate B should be rejected")
	}
	if !strings.Contains(err.Error(), "shared.events") {
		t.Fatalf("rejection should name the contested identity: %v", err)
	}
}

func TestEnvtestWebhook_Immutability(t *testing.T) {
	env := startWebhookEnv(t, "")
	defer env.stop()
	ctx := context.Background()

	a := newTopic("immutable-a", "default", "prod", "orig.name")
	if err := env.cl.Create(ctx, a); err != nil {
		t.Fatalf("create should be allowed: %v", err)
	}

	var got v1alpha1.KafkaTopic
	if err := env.cl.Get(ctx, client.ObjectKeyFromObject(a), &got); err != nil {
		t.Fatalf("re-get: %v", err)
	}
	got.Spec.TopicName = "renamed"
	if err := env.cl.Update(ctx, &got); err == nil {
		t.Fatal("topicName mutation should be rejected")
	} else if !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("expected an immutability error: %v", err)
	}
}

func TestEnvtestWebhook_TenancyDenied(t *testing.T) {
	env := startWebhookEnv(t, "")
	defer env.stop()
	ctx := context.Background()

	cl := &v1alpha1.KafkaCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "tn-cluster", Namespace: "default"},
		Spec: v1alpha1.KafkaClusterSpec{
			BootstrapServers: "localhost:9092",
			Tenancy: &v1alpha1.TenancyConfig{
				TopicPrefixes: []v1alpha1.TopicPrefixRule{{
					Namespaces: []string{"default"},
					Prefixes:   []string{"allowed."},
				}},
			},
		},
	}
	if err := env.cl.Create(ctx, cl); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	// Not matching the allowed prefix -> tenancy denial at admission.
	bad := newTopic("tn-bad", "default", "tn-cluster", "forbidden.events")
	if err := env.cl.Create(ctx, bad); err == nil {
		t.Fatal("tenancy-violating create should be rejected")
	} else if !strings.Contains(err.Error(), "tenancy") {
		t.Fatalf("expected a tenancy error: %v", err)
	}

	// Matching the prefix -> allowed.
	good := newTopic("tn-good", "default", "tn-cluster", "allowed.events")
	if err := env.cl.Create(ctx, good); err != nil {
		t.Fatalf("tenancy-compliant create should be allowed: %v", err)
	}
}

// newTopic builds a minimal KafkaTopic CR for the envtest API server (no UID;
// the apiserver assigns one).
func newTopic(name, ns, clusterRef, topicName string) *v1alpha1.KafkaTopic {
	return &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: clusterRef},
			TopicName:  topicName,
			Partitions: 1,
		},
	}
}
