package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/defaulting"
	"github.com/monedula-dev/monedula-gitops/internal/importer"
	"github.com/monedula-dev/monedula-gitops/internal/loader"
	"github.com/monedula-dev/monedula-gitops/internal/pipeline"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
)

// importFlags holds the flags for `import cluster`. Unlike the other commands,
// import takes no -f manifests (it reads live state, not desired state), so it
// does NOT reuse sharedFlags (whose options() errors when no -f is given).
type importFlags struct {
	clusterConfigFiles    []string
	cluster               string
	outputDir             string
	overwrite             string
	output                string // summary format: human | yaml | json
	skipSchemas           bool   // when true, skip SR-client construction and schema collection
	skipUsers             bool   // when true, skip SCRAM credential listing/reconstruction entirely
	skipQuotas            bool   // when true, skip quota listing/reconstruction entirely
	includeConnectingUser bool   // when true, do not skip the connecting principal's own credential
	includeInternal       bool   // when true, import Kafka/Confluent housekeeping topics too

	nsStrategy      string // single | prefix | regex | mapping-file
	nsSingle        string
	prefixSeparator string
	nsRegex         string
	nsMappingFile   string
}

func newImportCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "import",
		Short: "Import live Kafka state into GitOps manifests",
	}
	cmd.AddCommand(newImportClusterCmd())
	return cmd
}

func newImportClusterCmd() *cobra.Command {
	var f importFlags
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "Read live cluster state and generate KafkaTopic/KafkaAccessPolicy manifests",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runImportCluster(cmd, &f)
		},
	}
	fl := cmd.Flags()
	// StringArray (not StringSlice): repeatable without comma-splitting, so
	// paths containing commas survive (same contract as the shared flags).
	fl.StringArrayVarP(&f.clusterConfigFiles, "cluster-config-file", "c", nil, "KafkaCluster config file or directory (repeatable)")
	fl.StringVar(&f.cluster, "cluster", "", "select a single cluster by name (required when more than one is loaded)")
	fl.StringVar(&f.outputDir, "output-dir", "", "write manifests to this directory tree instead of stdout")
	fl.StringVar(&f.overwrite, "overwrite", "never", "overwrite mode when writing to --output-dir: never, changed, or always")
	fl.StringVarP(&f.output, "output", "o", "human", "summary output format: human, yaml, or json")
	fl.BoolVar(&f.skipSchemas, "skip-schemas", false, "skip schema reconstruction entirely (no Schema Registry connection); use when schemas are application-managed")
	fl.BoolVar(&f.skipUsers, "skip-users", false, "skip SCRAM credential reconstruction entirely (no ListScramCredentials call); use when users are application-managed")
	fl.BoolVar(&f.skipQuotas, "skip-quotas", false, "skip quota reconstruction entirely (no DescribeClientQuotas call); use when quotas are unsupported (e.g. Confluent Cloud) or externally managed")
	fl.BoolVar(&f.includeConnectingUser, "include-connecting-user", false, "include the connecting principal's own SCRAM credential (skipped by default to avoid self-lockout risk)")
	fl.BoolVar(&f.includeInternal, "include-internal", false, "import Kafka/Confluent housekeeping topics too (__*, _schemas, _confluent*); skipped by default")

	fl.StringVar(&f.nsStrategy, "namespace-strategy", "single", "namespace strategy: single, prefix, regex, or mapping-file")
	fl.StringVar(&f.nsSingle, "namespace", "default", "namespace value for the single strategy")
	fl.StringVar(&f.prefixSeparator, "prefix-separator", ".", "separator for the prefix strategy")
	fl.StringVar(&f.nsRegex, "namespace-regex", "", "regex with one capture group for the regex strategy")
	fl.StringVar(&f.nsMappingFile, "namespace-mapping-file", "", "path to a YAML/JSON topic->namespace map for the mapping-file strategy")
	return cmd
}

func runImportCluster(cmd *cobra.Command, f *importFlags) error {
	ctx := cmd.Context()

	// 1. Require at least one cluster config.
	if len(f.clusterConfigFiles) == 0 {
		return &ExitError{Code: 2, Msg: "no cluster config provided (use --cluster-config-file)"}
	}

	// 2. Load cluster configs and collect KafkaClusters keyed by name.
	objs, err := loader.Load(loader.Options{Filenames: f.clusterConfigFiles})
	if err != nil {
		return &ExitError{Code: 2, Msg: err.Error()}
	}
	clusters := map[string]*v1alpha1.KafkaCluster{}
	for _, o := range objs {
		if o.Cluster != nil {
			defaulting.Cluster(o.Cluster)
			clusters[o.Cluster.Name] = o.Cluster
		}
	}
	if len(clusters) == 0 {
		return &ExitError{Code: 2, Msg: "no KafkaCluster found in cluster config"}
	}

	// 3. Resolve the selected cluster.
	selected := ""
	switch {
	case f.cluster != "":
		if _, ok := clusters[f.cluster]; !ok {
			return &ExitError{Code: 2, Msg: fmt.Sprintf("selected cluster %q not found", f.cluster)}
		}
		selected = f.cluster
	case len(clusters) == 1:
		for name := range clusters {
			selected = name
		}
	default:
		return &ExitError{Code: 2, Msg: "multiple clusters loaded; use --cluster to select one"}
	}

	// 4. Build the live client. buildLiveClient reads only Clusters +
	// SelectedCluster, so a minimal Plan is sufficient.
	plan := &pipeline.Plan{Clusters: clusters, SelectedCluster: selected}
	client, cleanup, err := buildLiveClient(plan, f.clusterConfigFiles)
	if err != nil {
		return err
	}
	defer cleanup()

	// 4b. Build the schema client (nil when the cluster has no Schema Registry
	// or when --skip-schemas is set).
	var srClient schemaregistry.Client
	if !f.skipSchemas {
		srClient, err = buildSchemaClient(plan, f.clusterConfigFiles)
		if err != nil {
			return &ExitError{Code: 2, Msg: err.Error()}
		}
	}

	// 4c. Build the MDS client (nil when the cluster has no authorization.mds).
	mdsClient, err := buildMDSClient(plan, f.clusterConfigFiles)
	if err != nil {
		return &ExitError{Code: 2, Msg: err.Error()}
	}
	cl := clusters[selected]
	var mdsCfg *v1alpha1.MDSConfig
	if cl.Spec.Authorization != nil {
		mdsCfg = cl.Spec.Authorization.MDS
	}
	mdsScopes := importer.ScopesFromMDSConfig(mdsCfg)

	// 4d. Resolve the connecting principal (bare username, no "User:" prefix)
	// so users import can skip it by default (self-lockout guard). Resolution
	// mirrors printLockoutWarnings/connectingPrincipal: it returns "" when the
	// cluster has no SASL auth (mTLS/OAuth/None) or the username reference
	// cannot be resolved — in either case there is nothing to skip, so users
	// import proceeds without special-casing any principal.
	connectingUser := strings.TrimPrefix(connectingPrincipal(plan, f.clusterConfigFiles), "User:")

	// 5. Read a deterministic snapshot of live state (read-only; never mutates).
	snap, err := importer.ReadSnapshot(ctx, client, srClient, mdsClient, mdsScopes,
		importer.SnapshotOptions{SkipUsers: f.skipUsers, SkipQuotas: f.skipQuotas, IncludeInternal: f.includeInternal})
	if err != nil {
		return &ExitError{Code: 2, Msg: err.Error()}
	}

	// 6. Reconstruct manifests.
	res := importer.Build(snap, selected, v1alpha1.EffectiveAccessBackends(cl), mdsCfg, importer.BuildOptions{
		ConnectingUser:        connectingUser,
		IncludeConnectingUser: f.includeConnectingUser,
	})

	// 7. Assign namespaces from the namespace flags.
	strategy, err := f.namespaceStrategy()
	if err != nil {
		return err
	}
	if err := importer.AssignNamespaces(&res, strategy); err != nil {
		return &ExitError{Code: 2, Msg: err.Error()}
	}

	// 8. Output.
	if f.outputDir != "" {
		return writeImportToDir(cmd, res, selected, f)
	}
	return writeImportToStdout(cmd, res, selected, f)
}

// namespaceStrategy builds an importer.NamespaceStrategy from the flags. For the
// mapping-file strategy it reads and unmarshals the map (relative to cwd).
func (f *importFlags) namespaceStrategy() (importer.NamespaceStrategy, error) {
	s := importer.NamespaceStrategy{
		Kind:      f.nsStrategy,
		Single:    f.nsSingle,
		Separator: f.prefixSeparator,
		Pattern:   f.nsRegex,
	}
	if f.nsStrategy == "mapping-file" {
		if f.nsMappingFile == "" {
			return importer.NamespaceStrategy{}, &ExitError{Code: 2, Msg: "namespace strategy mapping-file requires --namespace-mapping-file"}
		}
		data, err := os.ReadFile(f.nsMappingFile)
		if err != nil {
			return importer.NamespaceStrategy{}, &ExitError{Code: 2, Msg: fmt.Sprintf("reading namespace mapping file %q: %v", f.nsMappingFile, err)}
		}
		mapping := map[string]string{}
		if err := yaml.Unmarshal(data, &mapping); err != nil {
			return importer.NamespaceStrategy{}, &ExitError{Code: 2, Msg: fmt.Sprintf("parsing namespace mapping file %q: %v", f.nsMappingFile, err)}
		}
		s.Mapping = mapping
	}
	return s, nil
}

// writeImportToStdout writes the manifest stream to stdout (so it is cleanly
// pipeable) and the human-readable summary to stderr.
//
// Schema-bearing imports are refused: the rendered KafkaTopic manifests
// reference schema files via spec.schema.*.valueFrom.file, and stdout mode
// streams only manifests, so those files would never be written and a later
// apply/verify of the piped output would fail on missing files. Refusing beats
// silently emitting broken manifests (and stripping the schemas would violate
// the round-trip guarantee).
func writeImportToStdout(cmd *cobra.Command, res importer.Result, cluster string, f *importFlags) error {
	if n := len(res.SchemaFiles); n > 0 {
		return &ExitError{Code: 2, Msg: fmt.Sprintf(
			"import found %d schema subject(s); schema files cannot be emitted to stdout — use --output-dir", n)}
	}
	manifests, err := importer.RenderManifests(res)
	if err != nil {
		return &ExitError{Code: 2, Msg: err.Error()}
	}
	if _, err := cmd.OutOrStdout().Write(manifests); err != nil {
		return &ExitError{Code: 2, Msg: err.Error()}
	}
	// Summary goes to stderr so the manifest stream on stdout stays clean and
	// pipeable (e.g. `import cluster ... | kubectl apply -f -`).
	out := importer.Summarize(res, cluster)
	out.SchemasSkipped = f.skipSchemas
	out.UsersSkipped = f.skipUsers
	out.QuotasSkipped = f.skipQuotas
	summary, err := importer.RenderSummary(out, f.output)
	if err != nil {
		return &ExitError{Code: 2, Msg: err.Error()}
	}
	if _, err := cmd.ErrOrStderr().Write(summary); err != nil {
		return &ExitError{Code: 2, Msg: err.Error()}
	}
	return nil
}

// writeImportToDir writes the manifests to the directory tree and prints the
// summary to stdout in the requested -o format. The human-friendly
// "Wrote N files, skipped M" line is only appended for the human format, so a
// yaml/json summary stays a clean, parseable document.
func writeImportToDir(cmd *cobra.Command, res importer.Result, cluster string, f *importFlags) error {
	outcome, err := importer.WriteToDir(res, f.outputDir, f.overwrite)
	if err != nil {
		return &ExitError{Code: 2, Msg: err.Error()}
	}
	imp := importer.Summarize(res, cluster)
	imp.SchemasSkipped = f.skipSchemas
	imp.UsersSkipped = f.skipUsers
	imp.QuotasSkipped = f.skipQuotas
	summary, err := importer.RenderSummary(imp, f.output)
	if err != nil {
		return &ExitError{Code: 2, Msg: err.Error()}
	}
	out := cmd.OutOrStdout()
	if _, err := out.Write(summary); err != nil {
		return &ExitError{Code: 2, Msg: err.Error()}
	}
	if f.output == "human" {
		if _, err := fmt.Fprintf(out, "Wrote %d files, skipped %d\n", len(outcome.Written), len(outcome.Skipped)); err != nil {
			return &ExitError{Code: 2, Msg: err.Error()}
		}
	}
	return nil
}
