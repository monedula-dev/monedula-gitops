package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/monedula-dev/monedula-gitops/internal/operator/controller"
	"github.com/monedula-dev/monedula-gitops/internal/operator/manager"
)

// minResyncInterval is the floor --resync-interval enforces: a shorter cadence
// risks the periodic resync becoming a self-inflicted load problem (each
// resync re-lists ACLs/quotas/role bindings per kind — see docs/operator.md's
// Scaling section) rather than the safety-net it is meant to be.
const minResyncInterval = 30 * time.Second

// newOperatorCmd builds the `operator` command, which runs the controller-runtime
// manager with the six reconcilers (KafkaCluster / KafkaTopic / KafkaAccessPolicy /
// KafkaQuota / KafkaRoleBinding / KafkaUser), the Prometheus metrics server, and
// health probes. It blocks until terminated by a signal.
func newOperatorCmd() *cobra.Command {
	var opts manager.Options
	cmd := &cobra.Command{
		Use:   "operator",
		Short: "Run the Monedula GitOps operator (controller-runtime manager)",
		Long: "Run the Monedula GitOps operator: a controller-runtime manager that " +
			"reconciles KafkaCluster, KafkaTopic, KafkaAccessPolicy, KafkaQuota, " +
			"KafkaRoleBinding, and KafkaUser resources, exposing Prometheus metrics " +
			"and health probes.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := validateOperatorOptions(opts); err != nil {
				return err
			}
			// Bridge controller-runtime's global logger (log.Log, and via it every
			// reconciler's log.FromContext logger) onto the slog handler setupLogging
			// configured from --log-level in the root PersistentPreRunE. Set exactly
			// once, before the manager is constructed — controller-runtime discards
			// all output until SetLogger is called.
			ctrl.SetLogger(controllerRuntimeLogger())
			ctx := cmd.Context()
			// When no cancellable context is wired by the caller (ctx is the plain
			// Background), install controller-runtime's signal handler so the manager
			// shuts down cleanly on SIGINT/SIGTERM.
			if ctx == nil || ctx == context.Background() {
				ctx = ctrl.SetupSignalHandler()
			}
			return manager.Run(ctx, opts)
		},
	}
	cmd.Flags().StringVar(&opts.MetricsAddr, "metrics-bind-address", ":8080",
		"address the Prometheus metrics endpoint binds to (\"0\" disables it)")
	cmd.Flags().BoolVar(&opts.MetricsSecure, "metrics-secure", false,
		"require authentication (TokenReview) and authorization (SubjectAccessReview) on the metrics endpoint; "+
			"default false serves plain HTTP metrics as in every prior release")
	cmd.Flags().StringVar(&opts.HealthAddr, "health-probe-bind-address", ":8081",
		"address the health/readiness probe endpoint binds to")
	cmd.Flags().BoolVar(&opts.LeaderElect, "leader-elect", false,
		"enable leader election so only one replica reconciles at a time")
	cmd.Flags().StringVar(&opts.ClusterNamespace, "cluster-namespace", "",
		"namespace to resolve KafkaCluster refs from (empty: each object's own namespace)")
	cmd.Flags().BoolVar(&opts.EnableWebhooks, "enable-webhooks", false,
		"enable the KafkaTopic identity validating admission webhook (requires serving certs; spec §20.3)")
	cmd.Flags().StringVar(&opts.WebhookCertDir, "webhook-cert-dir", "",
		"directory holding the webhook server's serving cert/key (empty: controller-runtime default; only used with --enable-webhooks)")
	cmd.Flags().DurationVar(&opts.ResyncInterval, "resync-interval", controller.DefaultResyncInterval,
		fmt.Sprintf("periodic resync cadence for every reconciler (minimum %s); a healthy resource is "+
			"re-checked on this cadence even without a spec change, so it also bounds duplicate-identity "+
			"loser recovery latency (see docs/operator.md Scaling)", minResyncInterval))
	cmd.Flags().IntVar(&opts.MaxConcurrentReconciles, "max-concurrent-reconciles", controller.DefaultMaxConcurrentReconciles,
		"reconcile concurrency per kind; >1 requires --leader-elect (in-process serialization "+
			"protects shared cluster state; see docs/operator.md Scaling)")
	return cmd
}

// validateOperatorOptions checks the flag combinations that must be refused
// BEFORE manager.Run is ever called (which would otherwise try to build a real
// client and block). Extracted from RunE so the accept/reject matrix is unit
// testable without starting a manager.
func validateOperatorOptions(opts manager.Options) error {
	if opts.ResyncInterval < minResyncInterval {
		return &ExitError{Code: 2, Msg: fmt.Sprintf(
			"--resync-interval must be at least %s, got %s", minResyncInterval, opts.ResyncInterval)}
	}
	// >1 relies on the operator's IN-PROCESS locking (internal/operator/locking)
	// to serialize shared-cluster-state writers; a second ACTIVE replica would
	// race the first cross-process, so concurrency is only safe when leader
	// election guarantees a single active replica.
	if opts.MaxConcurrentReconciles > 1 && !opts.LeaderElect {
		return &ExitError{Code: 2, Msg: fmt.Sprintf(
			"--max-concurrent-reconciles=%d requires --leader-elect: concurrent reconciles rely on "+
				"in-process locking to serialize shared cluster state, and a second active replica would "+
				"race it; enable --leader-elect or keep --max-concurrent-reconciles at 1",
			opts.MaxConcurrentReconciles)}
	}
	return nil
}
