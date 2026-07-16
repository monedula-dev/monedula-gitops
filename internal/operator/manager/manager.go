// Package manager wires the controller-runtime manager for the Monedula GitOps
// operator: it builds the scheme, constructs the manager, registers the five
// reconcilers with the production ClientFactory and event recorders, and serves
// metrics + health probes. It lives in its own package (rather than internal/
// operator) because it imports internal/operator/controller, which itself
// imports internal/operator — keeping Run here avoids an import cycle.
package manager

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/operator/controller"
	"github.com/monedula-dev/monedula-gitops/internal/operator/index"
	"github.com/monedula-dev/monedula-gitops/internal/operator/locking"
	operatorwebhook "github.com/monedula-dev/monedula-gitops/internal/operator/webhook"
)

// Secure metrics serving (--metrics-secure) authenticates the scraper's bearer
// token via TokenReview and authorizes it via SubjectAccessReview against the
// non-resource URL "/metrics" — the standard controller-runtime recipe (see
// filters.WithAuthenticationAndAuthorization in Run). Both are always granted
// (not conditioned on --metrics-secure) because RBAC is generated once at
// build time (make manifests), before flags are known; a ClusterRole is cheap
// to over-grant here since TokenReview/SubjectAccessReview require an
// authenticated caller with this ClusterRole in the first place.
// +kubebuilder:rbac:groups=authentication.k8s.io,resources=tokenreviews,verbs=create
// +kubebuilder:rbac:groups=authorization.k8s.io,resources=subjectaccessreviews,verbs=create

// webhookPort is the port the validating-webhook server binds when webhooks are
// enabled. 9443 is the controller-runtime / kubebuilder default.
const webhookPort = 9443

// leaderElectionID is the lease name used when leader election is enabled, so
// only one operator replica reconciles at a time.
const leaderElectionID = "monedula-operator"

// Options configures the operator manager (Run). Zero values are sensible for
// local development (no leader election, default bind addresses come from the
// caller's flags).
type Options struct {
	// MetricsAddr is the bind address for the Prometheus metrics server (e.g.
	// ":8080"). "0" disables it.
	MetricsAddr string
	// HealthAddr is the bind address for the health/readiness probes (e.g.
	// ":8081").
	HealthAddr string
	// LeaderElect enables leader election so only one replica is active.
	LeaderElect bool
	// ClusterNamespace is where KafkaCluster CRs are resolved from for topics and
	// policies. Empty means use each object's own namespace.
	ClusterNamespace string
	// EnableWebhooks turns on the validating admission webhooks for KafkaTopic,
	// KafkaQuota, KafkaAccessPolicy, KafkaRoleBinding, and KafkaUser (spec
	// §20.3, §39.5, §40, v0.35 §T2). Default false: the operator runs without a
	// webhook server (and without needing serving certs), which is the right
	// default for local development and clusters that haven't installed the
	// webhook config yet.
	EnableWebhooks bool
	// WebhookCertDir overrides the directory the webhook server reads its serving
	// key/cert from. Empty uses the controller-runtime default
	// (/tmp/k8s-webhook-server/serving-certs). Only consulted when EnableWebhooks.
	WebhookCertDir string
	// ResyncInterval is the periodic resync cadence threaded into every
	// reconciler's RequeueAfter (and the duplicate-identity gate's loser-recovery
	// RequeueAfter), replacing each kind's previously-hardcoded 5-minute
	// constant. Zero uses controller.DefaultResyncInterval (5m) — see
	// internal/cli/operator.go's --resync-interval flag, which enforces a 30s
	// floor before this field is ever set.
	ResyncInterval time.Duration
	// MaxConcurrentReconciles is the per-kind reconcile concurrency from
	// --max-concurrent-reconciles, passed through unchanged to every
	// reconciler's controller.Options (values <1 normalize to 1 in
	// reconcilerOptions; internal/operator/controller/resync.go). Values >1
	// are safe only within a single ACTIVE replica: the cluster-wide ACL/
	// role-binding views (internal/operator/controller/aclview.go,
	// rolebindingview.go) and the duplicate-identity gate are protected by the
	// IN-PROCESS per-(cluster, substrate) and per-(cluster, kind, identity)
	// locks (internal/operator/locking, wired in Run) — a second active
	// replica would reintroduce every cross-process race those locks close, so
	// the CLI refuses >1 without --leader-elect (internal/cli/operator.go) and
	// the Helm chart refuses to render that combination
	// (charts/monedula-gitops/templates/deployment.yaml).
	MaxConcurrentReconciles int
	// MetricsSecure enables authentication + authorization on the metrics
	// endpoint (controller-runtime's metrics-server SecureServing +
	// filters.WithAuthenticationAndAuthorization: TokenReview to authenticate
	// the bearer token, SubjectAccessReview to authorize GET on the metrics
	// path). Default false keeps the endpoint plain HTTP, matching every prior
	// release. When true, scraping requires a bearer token authorized for the
	// non-resource URL "/metrics" (or a client cert) — the chart's RBAC grants
	// the operator's own ServiceAccount cluster-scoped TokenReview/
	// SubjectAccessReview create so it can host the endpoint (it does not need
	// to also grant scrapers access to the metrics path itself — that's the
	// scraper's own RBAC, e.g. Prometheus's ClusterRole).
	MetricsSecure bool
	// Config is the rest.Config to build the manager from. When nil, the
	// in-cluster / kubeconfig config is loaded via ctrl.GetConfigOrDie.
	Config *rest.Config
}

// BuildScheme returns a runtime.Scheme with the core Kubernetes types and the
// Monedula GitOps v1alpha1 API registered. Exported so tests can assert the
// scheme is constructible without starting a manager.
func BuildScheme() (*runtime.Scheme, error) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("adding client-go scheme: %w", err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("adding v1alpha1 scheme: %w", err)
	}
	return scheme, nil
}

// Run builds a controller-runtime manager, registers the six reconcilers with
// the production DefaultClientFactory and event recorders, wires health/readiness
// probes (and the Prometheus metrics server), and blocks until ctx is cancelled.
//
// If ctx has no deadline/cancel wired by the caller, the caller is expected to
// pass ctrl.SetupSignalHandler()'s context (the operator command does); Run uses
// ctx as-is so it stays testable.
func Run(ctx context.Context, opts Options) error {
	scheme, err := BuildScheme()
	if err != nil {
		return err
	}

	cfg := opts.Config
	if cfg == nil {
		cfg = ctrl.GetConfigOrDie()
	}

	// Passed through to every reconciler unchanged; each SetupWithManager
	// normalizes <1 to 1 via reconcilerOptions. Values >1 are honored (v0.37)
	// — see MaxConcurrentReconciles's doc for the single-active-replica
	// precondition the CLI/chart guards enforce.
	maxConcurrentReconciles := opts.MaxConcurrentReconciles

	metricsOpts := metricsserver.Options{BindAddress: opts.MetricsAddr}
	if opts.MetricsSecure {
		// SecureServing switches the endpoint to HTTPS (a self-signed cert is
		// generated when none is configured, mirroring the webhook server's
		// default). FilterProvider wraps the handler with TokenReview
		// (authenticate the bearer token) + SubjectAccessReview (authorize GET
		// on the metrics path) — the standard controller-runtime recipe; see
		// the ClusterRole rules added alongside this flag (config/rbac/role.yaml,
		// charts/monedula-gitops/templates/clusterrole.yaml).
		metricsOpts.SecureServing = true
		metricsOpts.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	mgrOpts := ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsOpts,
		HealthProbeBindAddress: opts.HealthAddr,
		LeaderElection:         opts.LeaderElect,
		LeaderElectionID:       leaderElectionID,
		// Release the leader lease as soon as ctx is cancelled instead of
		// waiting out the full lease duration (~15s of leader blindness per
		// deploy otherwise). controller-runtime's docs say this is safe only
		// when the binary "immediately ends" after mgr.Start returns — verified
		// here: Run's only statement after mgr.Start returns is wrapping/
		// returning its error (no further API writes), and the operator's
		// caller (internal/cli/operator.go -> cmd/monedula-gitops/main.go)
		// does nothing but format the error and os.Exit. No reconcile or API
		// activity happens after shutdown begins.
		LeaderElectionReleaseOnCancel: true,
	}

	// Label-scoped ConfigMap + Secret informers (spec §11.3, §11.4): the cache
	// watches ONLY ConfigMaps labelled gitops.monedula.dev/schema-source=true
	// and ONLY Secrets labelled gitops.monedula.dev/credential-source=true. This
	// is cheap and avoids standing up a cluster-wide informer for either type —
	// the credential-caching anti-pattern (the operator previously cached every
	// Secret in the cluster).
	//
	// Both types are read UNCACHED (directly from the API) via DisableFor so that
	// resolution can still read ANY referenced object — including unlabelled ones
	// not in the informer. Without this, client.Get on an unlabelled ConfigMap or
	// Secret would return NotFound from the label-scoped cache and break schema
	// resolution / credential lookups.
	mgrOpts.Cache = cache.Options{
		ByObject: map[client.Object]cache.ByObject{
			&corev1.ConfigMap{}: {
				Label: labels.SelectorFromSet(labels.Set{controller.SchemaSourceLabel: controller.SchemaSourceLabelValue}),
			},
			&corev1.Secret{}: {
				Label: labels.SelectorFromSet(labels.Set{controller.CredentialSourceLabel: controller.CredentialSourceLabelValue}),
			},
		},
	}
	mgrOpts.Client = client.Options{
		Cache: &client.CacheOptions{
			DisableFor: []client.Object{&corev1.ConfigMap{}, &corev1.Secret{}},
		},
	}

	// Only stand up a webhook server when webhooks are enabled; otherwise the
	// operator needs no serving certs (the default). Port 9443 is the
	// controller-runtime default; CertDir is overridden only when configured.
	if opts.EnableWebhooks {
		mgrOpts.WebhookServer = webhook.NewServer(webhook.Options{
			Port:    webhookPort,
			CertDir: opts.WebhookCertDir,
		})
	}

	mgr, err := ctrl.NewManager(cfg, mgrOpts)
	if err != nil {
		return fmt.Errorf("creating manager: %w", err)
	}

	// Register the shared spec.clusterRef.name cache indexes unconditionally
	// (cheap; both the webhook and future controllers can rely on it).
	if err := index.RegisterIndexes(ctx, mgr); err != nil {
		return fmt.Errorf("registering field indexes: %w", err)
	}

	// Register the schema-configmap refs index so the ConfigMap watch (Task 3)
	// can map a changed ConfigMap back to the KafkaTopics that reference it.
	if err := controller.RegisterSchemaConfigMapIndex(ctx, mgr); err != nil {
		return fmt.Errorf("registering schema-configmap index: %w", err)
	}
	// Register the cluster secret-refs index so the Secret watch (Tasks 3-4) can
	// map a changed Secret back to the KafkaCluster(s) that reference it.
	if err := controller.RegisterClusterSecretNamesIndex(ctx, mgr); err != nil {
		return fmt.Errorf("registering cluster secret-names index: %w", err)
	}
	// Register the user password-secret index so the KafkaUser Secret watch can
	// map a changed password Secret back to the KafkaUsers that reference it.
	if err := controller.RegisterUserPasswordSecretNamesIndex(ctx, mgr); err != nil {
		return fmt.Errorf("registering user password-secret index: %w", err)
	}

	// The production ClientFactory reads referenced Secrets via the manager's
	// cached client and builds the live Kafka/Schema-Registry clients.
	factory := &controller.DefaultClientFactory{Client: mgr.GetClient()}

	// ONE process-wide keyed lock registry serializes every substrate writer
	// per (KafkaCluster, substrate) and every identity claimant per
	// (KafkaCluster, kind, identity) — see internal/operator/locking and the
	// controllers' locks.go (which also documents the identity → acl → rbac
	// global lock order). Injected into the substrate writers (topic, policy,
	// role binding) and the identity-locked kinds (topic, quota, user, role
	// binding).
	// WIRING CHECKLIST: any future substrate/identity-lock consumer MUST
	// receive this same registry via its Locks field here — a reconciler left
	// nil-Locks runs unlocked (it logs an Info line from SetupWithManager, but
	// nothing fails). Likewise every duplicate-gated kind (topic, quota, user,
	// role binding) MUST receive apiReader via its APIReader field, or its
	// contested-path quorum recheck (duplicate.go) silently degrades to the
	// cached scan.
	locks := &locking.Registry{}

	// The manager's uncached quorum reader, used only by the duplicate gate's
	// contested-path rechecks — never on the steady-state hot path.
	apiReader := mgr.GetAPIReader()

	if err := (&controller.KafkaClusterReconciler{
		Client:                  mgr.GetClient(),
		Scheme:                  mgr.GetScheme(),
		Clients:                 factory,
		Recorder:                mgr.GetEventRecorder("kafkacluster-controller"),
		ResyncInterval:          opts.ResyncInterval,
		MaxConcurrentReconciles: maxConcurrentReconciles,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setting up KafkaCluster controller: %w", err)
	}

	if err := (&controller.KafkaTopicReconciler{
		Client:                  mgr.GetClient(),
		Scheme:                  mgr.GetScheme(),
		Clients:                 factory,
		Recorder:                mgr.GetEventRecorder("kafkatopic-controller"),
		ClusterNamespace:        opts.ClusterNamespace,
		ResyncInterval:          opts.ResyncInterval,
		MaxConcurrentReconciles: maxConcurrentReconciles,
		Locks:                   locks,
		APIReader:               apiReader,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setting up KafkaTopic controller: %w", err)
	}

	if err := (&controller.KafkaAccessPolicyReconciler{
		Client:                  mgr.GetClient(),
		Scheme:                  mgr.GetScheme(),
		Clients:                 factory,
		Recorder:                mgr.GetEventRecorder("kafkaaccesspolicy-controller"),
		ClusterNamespace:        opts.ClusterNamespace,
		ResyncInterval:          opts.ResyncInterval,
		MaxConcurrentReconciles: maxConcurrentReconciles,
		Locks:                   locks,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setting up KafkaAccessPolicy controller: %w", err)
	}

	if err := (&controller.KafkaQuotaReconciler{
		Client:                  mgr.GetClient(),
		Scheme:                  mgr.GetScheme(),
		Clients:                 factory,
		Recorder:                mgr.GetEventRecorder("kafkaquota-controller"),
		ClusterNamespace:        opts.ClusterNamespace,
		ResyncInterval:          opts.ResyncInterval,
		MaxConcurrentReconciles: maxConcurrentReconciles,
		Locks:                   locks,
		APIReader:               apiReader,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setting up KafkaQuota controller: %w", err)
	}

	if err := (&controller.KafkaRoleBindingReconciler{
		Client:                  mgr.GetClient(),
		Scheme:                  mgr.GetScheme(),
		Clients:                 factory,
		Recorder:                mgr.GetEventRecorder("kafkarolebinding-controller"),
		ClusterNamespace:        opts.ClusterNamespace,
		ResyncInterval:          opts.ResyncInterval,
		MaxConcurrentReconciles: maxConcurrentReconciles,
		Locks:                   locks,
		APIReader:               apiReader,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setting up KafkaRoleBinding controller: %w", err)
	}

	if err := (&controller.KafkaUserReconciler{
		Client:                  mgr.GetClient(),
		Scheme:                  mgr.GetScheme(),
		Clients:                 factory,
		Recorder:                mgr.GetEventRecorder("kafkauser-controller"),
		ClusterNamespace:        opts.ClusterNamespace,
		ResyncInterval:          opts.ResyncInterval,
		MaxConcurrentReconciles: maxConcurrentReconciles,
		Locks:                   locks,
		APIReader:               apiReader,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setting up KafkaUser controller: %w", err)
	}

	// Admission webhooks (spec §20.3, §39.5, §40, v0.35 §T2): identity, shape,
	// and ACL-conflict validation for KafkaTopic, KafkaQuota,
	// KafkaAccessPolicy, KafkaRoleBinding, and KafkaUser. Only registered when
	// enabled, so the operator works without serving certs by default.
	if opts.EnableWebhooks {
		if err := (&operatorwebhook.KafkaTopicValidator{
			Reader:           mgr.GetClient(),
			ClusterNamespace: opts.ClusterNamespace,
		}).SetupWithManager(mgr); err != nil {
			return fmt.Errorf("setting up KafkaTopic webhook: %w", err)
		}
		if err := (&operatorwebhook.KafkaQuotaValidator{
			Reader:           mgr.GetClient(),
			ClusterNamespace: opts.ClusterNamespace,
		}).SetupWithManager(mgr); err != nil {
			return fmt.Errorf("setting up KafkaQuota webhook: %w", err)
		}
		if err := (&operatorwebhook.KafkaAccessPolicyValidator{
			Reader:           mgr.GetClient(),
			ClusterNamespace: opts.ClusterNamespace,
		}).SetupWithManager(mgr); err != nil {
			return fmt.Errorf("setting up KafkaAccessPolicy webhook: %w", err)
		}
		if err := (&operatorwebhook.KafkaRoleBindingValidator{
			Reader:           mgr.GetClient(),
			ClusterNamespace: opts.ClusterNamespace,
		}).SetupWithManager(mgr); err != nil {
			return fmt.Errorf("setting up KafkaRoleBinding webhook: %w", err)
		}
		if err := (&operatorwebhook.KafkaUserValidator{
			Reader:           mgr.GetClient(),
			ClusterNamespace: opts.ClusterNamespace,
		}).SetupWithManager(mgr); err != nil {
			return fmt.Errorf("setting up KafkaUser webhook: %w", err)
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("adding healthz check: %w", err)
	}

	// Readiness: when webhooks are enabled, gate readiness on the webhook
	// server actually being able to serve (StartedChecker), not just on the
	// probe HTTP listener being up (healthz.Ping). Our webhooks are installed
	// with failurePolicy: Fail, so with a single replica a Service that routes
	// to a pod whose webhook listener/certs aren't serving yet would black-hole
	// every CR create/update until the pod becomes reachable. Requiring the
	// webhook server to be started before reporting Ready keeps the pod out of
	// the Service's endpoints during that window. Non-webhook mode keeps the
	// plain Ping, since there is no webhook listener to wait for.
	readyzCheck := healthz.Ping
	if opts.EnableWebhooks {
		readyzCheck = mgr.GetWebhookServer().StartedChecker()
	}
	if err := mgr.AddReadyzCheck("readyz", readyzCheck); err != nil {
		return fmt.Errorf("adding readyz check: %w", err)
	}

	if err := mgr.Start(ctx); err != nil {
		return fmt.Errorf("running manager: %w", err)
	}
	return nil
}
