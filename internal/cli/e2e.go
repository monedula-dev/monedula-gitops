package cli

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/cluster"
	"github.com/monedula-dev/monedula-gitops/internal/e2e"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	"github.com/monedula-dev/monedula-gitops/internal/loader"
	"github.com/monedula-dev/monedula-gitops/internal/operator/manager"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
	"github.com/monedula-dev/monedula-gitops/internal/secrets"
)

// registerClusterConfigFlag binds the e2e commands' cluster-config flag onto
// cmd, accepting both the canonical --cluster-config-file (matching every
// public command's spelling) and the legacy --cluster-config spelling that
// test/e2e/k8s/lib.bash and the scenario harness already invoke. Both flags
// write into the same *target; --cluster-config is kept working but marked
// Deprecated so cobra prints a nudge toward the canonical spelling without
// breaking existing callers. Whichever flag is actually passed on the command
// line wins; if both are passed, the one spelled LAST on the command line wins
// — pflag assigns values in command-line order, and both flags write the
// shared *target unconditionally (flag registration order is irrelevant).
func registerClusterConfigFlag(cmd *cobra.Command, target *string, usage string) {
	fl := cmd.Flags()
	fl.StringVar(target, "cluster-config", "", usage)
	_ = fl.MarkDeprecated("cluster-config", "use --cluster-config-file instead")
	fl.StringVar(target, "cluster-config-file", "", usage)
}

// newE2ECmd builds the hidden `e2e` command tree. It is test tooling for the
// scenarios/ suite (the shared assertion engine the Go + bats orchestrators
// invoke), not a user-facing command, so it is Hidden from help output.
func newE2ECmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "e2e",
		Short:  "Scenario test tooling (internal)",
		Hidden: true,
	}
	cmd.AddCommand(newE2ECheckCmd())
	cmd.AddCommand(newE2EMutateCmd())
	return cmd
}

func newE2ECheckCmd() *cobra.Command {
	var (
		scenarioDir   string
		mode          string
		kubeconfig    string
		clusterConfig string
		namespace     string
	)
	cmd := &cobra.Command{
		Use:   "check",
		Short: "Assert a scenario's expect.yaml against the live cluster",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if mode != "cli" && mode != "k8s" {
				return &ExitError{Code: 2, Msg: fmt.Sprintf("--mode must be cli or k8s, got %q", mode)}
			}
			exp, err := e2e.LoadExpect(filepath.Join(scenarioDir, "expect.yaml"))
			if err != nil {
				return &ExitError{Code: 2, Msg: err.Error()}
			}
			ctx := cmd.Context()
			var rep e2e.Report

			// liveState (both modes): probe real Kafka/SR via the cluster manifest.
			// In k8s mode the broker may be unreachable from the host depending on
			// the harness topology; when --cluster-config-file is absent liveState is
			// skipped with a warning. The bats harness (test/e2e/k8s) runs the
			// operator against the host compose broker and DOES pass --cluster-config-file,
			// so k8s-mode runs assert liveState too.
			needsKafka := len(exp.LiveState.Topics) > 0 || len(exp.LiveState.ACLs) > 0 || len(exp.LiveState.Quotas) > 0 ||
				len(exp.LiveState.Users) > 0 ||
				(exp.LiveState.Absent != nil && (len(exp.LiveState.Absent.Topics) > 0 || len(exp.LiveState.Absent.Users) > 0))
			needsSubjects := len(exp.LiveState.Subjects) > 0
			if needsKafka || needsSubjects {
				if clusterConfig == "" {
					if mode == "k8s" {
						// In k8s mode liveState needs a host-reachable broker; without
						// --cluster-config-file it cannot be probed, so skip with a warning
						// and rely on Conditions. The bats harness passes --cluster-config-file
						// (operator and host binary share the compose broker), so this
						// fallback only fires for ad-hoc condition-only checks.
						_, _ = fmt.Fprintf(cmd.ErrOrStderr(),
							"warning: --mode k8s: liveState assertions skipped (pass --cluster-config-file to enable broker probes)\n")
					} else {
						return &ExitError{Code: 2, Msg: "--cluster-config-file is required to assert liveState"}
					}
				} else {
					if needsKafka {
						admin, cleanup, berr := buildAdminClientForE2E(clusterConfig)
						if berr != nil {
							return &ExitError{Code: 2, Msg: berr.Error()}
						}
						defer cleanup()
						lr := e2e.CheckLiveState(ctx, e2e.NewKafkaProber(admin), exp.LiveState)
						rep.Results = append(rep.Results, lr.Results...)
					}
					if needsSubjects {
						sr, serr := buildSchemaClientForE2E(clusterConfig)
						if serr != nil {
							return &ExitError{Code: 2, Msg: serr.Error()}
						}
						var sp e2e.SubjectProber
						if sr != nil {
							sp = e2e.NewSchemaSubjectProber(sr)
						}
						cr := e2e.CheckSubjects(ctx, sp, exp.LiveState)
						rep.Results = append(rep.Results, cr.Results...)
					}
				}
			}

			// A scenario may declare k8s conditions but be invoked in cli mode
			// (conditions are k8s-only). Warn rather than silently skip, so a
			// mis-routed mode flag does not vacuously pass condition assertions.
			if mode == "cli" && exp.K8s != nil && len(exp.K8s.Conditions) > 0 {
				_, _ = fmt.Fprintf(cmd.ErrOrStderr(),
					"warning: --mode cli: %d k8s condition assertion(s) in expect.yaml are not checked in cli mode\n",
					len(exp.K8s.Conditions))
			}

			// conditions (k8s only): read CR status via a controller-runtime client.
			if mode == "k8s" && exp.K8s != nil && len(exp.K8s.Conditions) > 0 {
				cl, cerr := buildE2EK8sClient(kubeconfig)
				if cerr != nil {
					return &ExitError{Code: 2, Msg: cerr.Error()}
				}
				cr := e2e.CheckConditions(ctx, cl, namespace, exp.K8s.Conditions)
				rep.Results = append(rep.Results, cr.Results...)
			}

			_, _ = fmt.Fprint(cmd.OutOrStdout(), rep.String())
			if rep.Failed() {
				return &ExitError{Code: 1, Msg: "scenario assertions failed"}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&scenarioDir, "scenario", "", "path to the scenario directory")
	cmd.Flags().StringVar(&mode, "mode", "", "cli or k8s")
	cmd.Flags().StringVar(&kubeconfig, "kubeconfig", "", "kubeconfig path (k8s mode conditions)")
	registerClusterConfigFlag(cmd, &clusterConfig, "KafkaCluster manifest for liveState probes")
	cmd.Flags().StringVar(&namespace, "namespace", "default", "namespace for k8s condition lookups")
	_ = cmd.MarkFlagRequired("scenario")
	_ = cmd.MarkFlagRequired("mode")
	return cmd
}

// newE2EMutateCmd builds the hidden `e2e mutate` subcommand: it simulates an
// out-of-band broker change (drift) by setting topic config directly via the
// admin client. Test tooling for drift scenarios, not a user command.
func newE2EMutateCmd() *cobra.Command {
	var (
		clusterConfig string
		topic         string
		setSpec       string
	)
	cmd := &cobra.Command{
		Use:   "mutate",
		Short: "Apply an out-of-band broker change for drift scenarios (internal)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if clusterConfig == "" {
				return &ExitError{Code: 2, Msg: "--cluster-config-file is required"}
			}
			if topic == "" {
				return &ExitError{Code: 2, Msg: "--topic is required"}
			}
			set, err := parseSetSpec(setSpec)
			if err != nil {
				return &ExitError{Code: 2, Msg: err.Error()}
			}
			admin, cleanup, berr := buildAdminClientForE2E(clusterConfig)
			if berr != nil {
				return &ExitError{Code: 2, Msg: berr.Error()}
			}
			defer cleanup()
			if uerr := admin.UpdateTopicConfig(cmd.Context(), topic, set); uerr != nil {
				return &ExitError{Code: 2, Msg: uerr.Error()}
			}
			return nil
		},
	}
	registerClusterConfigFlag(cmd, &clusterConfig, "KafkaCluster manifest for the target cluster")
	cmd.Flags().StringVar(&topic, "topic", "", "topic whose config to set")
	cmd.Flags().StringVar(&setSpec, "set", "", "comma-separated key=value config entries")
	return cmd
}

// parseSetSpec parses "k1=v1,k2=v2" into a map. Errors on an empty spec or a
// pair lacking '='.
func parseSetSpec(spec string) (map[string]string, error) {
	if spec == "" {
		return nil, fmt.Errorf("--set is required (comma-separated key=value)")
	}
	out := map[string]string{}
	for _, pair := range strings.Split(spec, ",") {
		k, v, ok := strings.Cut(pair, "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("malformed --set entry %q: want key=value", pair)
		}
		out[k] = v
	}
	return out, nil
}

// findCluster returns the single KafkaCluster among loaded objects. It errors
// when none or more than one is present, matching the CLI's "one cluster per
// config" convention (a profile manifest declares exactly one cluster).
func findCluster(objs []loader.Object) (*v1alpha1.KafkaCluster, error) {
	var found *v1alpha1.KafkaCluster
	for _, o := range objs {
		if o.Cluster == nil {
			continue
		}
		if found != nil {
			return nil, fmt.Errorf("multiple KafkaClusters in cluster-config; expected exactly one")
		}
		found = o.Cluster
	}
	if found == nil {
		return nil, fmt.Errorf("no KafkaCluster found in cluster-config")
	}
	return found, nil
}

// buildAdminClientForE2E loads a KafkaCluster manifest and builds a live admin
// client. If the cluster carries the mock-state annotation it is seeded from
// that file (hermetic seam, mirroring buildLiveClient); otherwise a real client
// is built, resolving secrets from the environment/files relative to the
// manifest's directory.
func buildAdminClientForE2E(clusterConfigPath string) (kafka.AdminClient, func(), error) {
	noop := func() {}
	objs, err := loader.Load(loader.Options{Filenames: []string{clusterConfigPath}})
	if err != nil {
		return nil, noop, err
	}
	kc, err := findCluster(objs)
	if err != nil {
		return nil, noop, fmt.Errorf("%s: %w", clusterConfigPath, err)
	}
	dir := filepath.Dir(clusterConfigPath)
	if stateFile := kc.Annotations[mockStateAnnotation]; stateFile != "" {
		c, ferr := mock.FromFile(filepath.Join(dir, stateFile))
		if ferr != nil {
			return nil, noop, ferr
		}
		return c, noop, nil
	}
	admin, cleanup, berr := cluster.BuildKafkaClient(kc, secrets.FileEnvResolver{BaseDir: dir})
	if berr != nil {
		return nil, noop, fmt.Errorf("building kafka client for %s: %w", clusterConfigPath, berr)
	}
	return admin, cleanup, nil
}

// buildSchemaClientForE2E loads a KafkaCluster manifest and builds the Schema
// Registry client for subject liveState probing. Returns (nil, nil) when the
// cluster has no schemaRegistry configured (caller treats nil as "no SR").
func buildSchemaClientForE2E(clusterConfigPath string) (schemaregistry.Client, error) {
	objs, err := loader.Load(loader.Options{Filenames: []string{clusterConfigPath}})
	if err != nil {
		return nil, err
	}
	kc, err := findCluster(objs)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", clusterConfigPath, err)
	}
	dir := filepath.Dir(clusterConfigPath)
	return cluster.BuildSchemaClient(kc, secrets.FileEnvResolver{BaseDir: dir})
}

// buildE2EK8sClient constructs a controller-runtime client using the project
// scheme. An explicit kubeconfig path wins; otherwise the ambient config
// (KUBECONFIG / in-cluster) is used.
func buildE2EK8sClient(kubeconfig string) (client.Client, error) {
	scheme, err := manager.BuildScheme()
	if err != nil {
		return nil, err
	}
	var cfg *rest.Config
	if kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		cfg, err = ctrl.GetConfig()
	}
	if err != nil {
		return nil, err
	}
	return client.New(cfg, client.Options{Scheme: scheme})
}
