package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/defaulting"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/loader"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	"github.com/monedula-dev/monedula-gitops/internal/mirror"
	"github.com/monedula-dev/monedula-gitops/internal/pipeline"
)

// doctorFlags holds the flags for `doctor`. Like import, doctor takes no -f
// manifests (it probes the cluster, not desired state), so it does not reuse
// sharedFlags.
type doctorFlags struct {
	clusterConfigFiles []string
	cluster            string
	output             string // human | yaml | json
}

// Doctor check statuses.
const (
	doctorPass = "pass"
	doctorFail = "fail"
	doctorSkip = "skip"
	// doctorWarn flags a condition worth attention that does not make the run
	// unhealthy.
	doctorWarn = "warn"
)

// doctorCheck is one connectivity/readiness probe result.
type doctorCheck struct {
	Name    string `json:"name"`
	Status  string `json:"status"` // pass | fail | skip | warn
	Message string `json:"message"`
}

// doctorOutput is the serialized result of a doctor run.
type doctorOutput struct {
	Kind    string        `json:"kind"`
	Cluster string        `json:"cluster"`
	Checks  []doctorCheck `json:"checks"`
	Healthy bool          `json:"healthy"`
}

// newDoctorCmd builds the doctor command (spec §18): read-only connectivity and
// operational-readiness checks for one cluster. All checks run even when an
// earlier one fails, so a single run reports every problem it can see. Exit 0
// when every check passes (skips are fine), exit 2 when any check fails.
func newDoctorCmd() *cobra.Command {
	var f doctorFlags
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Check connectivity and operational readiness for a cluster",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runDoctor(cmd, &f)
		},
	}
	fl := cmd.Flags()
	// StringArray (not StringSlice): repeatable without comma-splitting, so
	// paths containing commas survive (same contract as the shared flags).
	fl.StringArrayVarP(&f.clusterConfigFiles, "cluster-config-file", "c", nil, "KafkaCluster config file or directory (repeatable)")
	fl.StringVar(&f.cluster, "cluster", "", "select a single cluster by name (required when more than one is loaded)")
	fl.StringVarP(&f.output, "output", "o", "human", "output format: human, yaml, or json")
	return cmd
}

func runDoctor(cmd *cobra.Command, f *doctorFlags) error {
	ctx := cmd.Context()

	// 1. Load and select the cluster (same contract as import: explicit
	// --cluster must exist; a sole cluster is auto-selected; multiple clusters
	// without a selector is a usage error).
	if len(f.clusterConfigFiles) == 0 {
		return &ExitError{Code: 2, Msg: "no cluster config provided (use --cluster-config-file)"}
	}
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
	cl := clusters[selected]

	// 2. Run the checks (read-only; never mutates). The check list mirrors the
	// operator's KafkaCluster readiness probe (ReconcileCluster) but reports
	// per-step results instead of folding everything into conditions.
	out := doctorOutput{Kind: "DoctorOutput", Cluster: selected, Healthy: true}
	add := func(name, status, msg string) {
		out.Checks = append(out.Checks, doctorCheck{Name: name, Status: status, Message: msg})
		if status == doctorFail {
			out.Healthy = false
		}
	}

	// config: parsing + selection already succeeded if we got here. Summarize
	// the connection parameters WITHOUT secrets (mechanism/endpoint only).
	add("config", doctorPass, configSummary(cl))

	// kafka-connect: build the client. Construction only parses configuration
	// and resolves secrets — it performs no I/O, so a bad TLS/auth/bootstrap
	// setting will NOT surface here; that connectivity proof happens at
	// kafka-admin below, which makes the first real call.
	plan := &pipeline.Plan{Clusters: clusters, SelectedCluster: selected}
	client, cleanup, err := buildLiveClient(plan, f.clusterConfigFiles)
	if err != nil {
		client = nil
		add("kafka-connect", doctorFail, err.Error())
	} else {
		defer cleanup()
		add("kafka-connect", doctorPass, "client constructed (configuration parsed; connectivity is proven by kafka-admin below)")
	}

	// kafka-admin: ListTopics proves Admin API reachability, authentication,
	// and topic describe/list permission in one cheap read-only call.
	if client == nil {
		add("kafka-admin", doctorFail, "not run: Kafka client unavailable")
	} else if topics, err := client.ListTopics(ctx); err != nil {
		add("kafka-admin", doctorFail, err.Error())
	} else {
		add("kafka-admin", doctorPass, fmt.Sprintf("%d topic(s)", len(topics)))
	}

	// acl-read: ListACLs proves the ACL describe path (authorizer access).
	if client == nil {
		add("acl-read", doctorFail, "not run: Kafka client unavailable")
	} else if acls, err := client.ListACLs(ctx); err != nil {
		add("acl-read", doctorFail, err.Error())
	} else {
		add("acl-read", doctorPass, fmt.Sprintf("%d ACL(s)", len(acls)))
	}

	// mds: only probed when authorization.mds is configured; skipped (not
	// failed) otherwise, mirroring the schema-registry check below. MDS is the
	// fiddliest connection to configure (its own endpoint/auth/TLS, independent
	// of the broker), so it gets its own preflight: authenticate and make one
	// cheap read (ListRoleBindings against the cluster's kafka scope, the same
	// call diff/apply use to discover live role bindings). An empty result is a
	// legitimate PASS (no bindings does not mean the scope/credentials are
	// wrong); ListRoleBindings only returns an error for auth/network/server
	// failures.
	if cl.Spec.Authorization == nil || cl.Spec.Authorization.MDS == nil {
		add("mds", doctorSkip, "not configured")
	} else if mdsClient, err := buildMDSClient(plan, f.clusterConfigFiles); err != nil {
		add("mds", doctorFail, err.Error())
	} else if mdsClient == nil {
		add("mds", doctorSkip, "not configured")
	} else if bindings, err := mdsClient.ListRoleBindings(ctx, mds.Scope{
		Type:         "kafka",
		KafkaCluster: cl.Spec.Authorization.MDS.Clusters.KafkaCluster,
	}); err != nil {
		add("mds", doctorFail, err.Error())
	} else {
		add("mds", doctorPass, fmt.Sprintf("%d role binding(s)", len(bindings)))
	}

	// schema-registry: only probed when configured; skipped (not failed)
	// otherwise.
	if cl.Spec.SchemaRegistry == nil {
		add("schema-registry", doctorSkip, "not configured")
	} else if srClient, err := buildSchemaClient(plan, f.clusterConfigFiles); err != nil {
		add("schema-registry", doctorFail, err.Error())
	} else if subjects, err := srClient.ListSubjects(ctx); err != nil {
		add("schema-registry", doctorFail, err.Error())
	} else {
		add("schema-registry", doctorPass, fmt.Sprintf("%d subject(s)", len(subjects)))
	}

	// rest / rest-cluster-binding / rest-mirrors / link:<name>: Kafka REST
	// (mirror topics). Skipped when spec.rest is not configured. All checks run
	// even when an earlier one fails.
	doctorREST(ctx, plan, f.clusterConfigFiles, cl, client, add)

	// 3. Render and map health to the exit code (0 healthy, 2 problems).
	rendered, err := renderDoctor(out, f.output)
	if err != nil {
		return &ExitError{Code: 2, Msg: err.Error()}
	}
	if _, err := cmd.OutOrStdout().Write(rendered); err != nil {
		return &ExitError{Code: 2, Msg: err.Error()}
	}
	if !out.Healthy {
		return &ExitError{Code: 2, Msg: ""}
	}
	return nil
}

// configSummary describes the cluster's connection parameters without leaking
// secrets: only the bootstrap servers, TLS on/off, auth mechanism name, and
// Schema Registry endpoint appear.
func configSummary(cl *v1alpha1.KafkaCluster) string {
	tls := "off"
	if cl.Spec.TLS != nil && cl.Spec.TLS.Enabled {
		tls = "on"
	}
	auth := "none"
	if cl.Spec.Auth != nil && cl.Spec.Auth.Mechanism != "" {
		auth = cl.Spec.Auth.Mechanism
	}
	sr := "not configured"
	if cl.Spec.SchemaRegistry != nil {
		sr = cl.Spec.SchemaRegistry.Endpoint
	}
	return fmt.Sprintf("bootstrapServers=%s tls=%s auth=%s schemaRegistry=%s",
		cl.Spec.BootstrapServers, tls, auth, sr)
}

// renderDoctor serializes a doctorOutput in the requested format. Output is
// deterministic (checks are appended in a fixed order).
func renderDoctor(out doctorOutput, format string) ([]byte, error) {
	switch format {
	case "json":
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.SetIndent("", "  ")
		if err := enc.Encode(out); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	case "yaml":
		return yaml.Marshal(out)
	case "human":
		return renderDoctorHuman(out), nil
	default:
		return nil, fmt.Errorf("unsupported output format %q (want human, yaml, or json)", format)
	}
}

func renderDoctorHuman(out doctorOutput) []byte {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "Cluster: %s\n", out.Cluster)
	failed := 0
	for _, c := range out.Checks {
		label := ""
		switch c.Status {
		case doctorPass:
			label = "PASS"
		case doctorFail:
			label = "FAIL"
			failed++
		case doctorSkip:
			label = "SKIP"
		case doctorWarn:
			label = "WARN"
		}
		fmt.Fprintf(&buf, "%s %s: %s\n", label, c.Name, c.Message)
	}
	if out.Healthy {
		buf.WriteString("Doctor: healthy\n")
	} else {
		fmt.Fprintf(&buf, "Doctor: %d check(s) failed\n", failed)
	}
	return buf.Bytes()
}

// doctorREST runs the Kafka REST checks (read-only). kafkaClient may be nil when
// the Kafka client could not be built; the binding check then fails.
func doctorREST(ctx context.Context, plan *pipeline.Plan, files []string, cl *v1alpha1.KafkaCluster,
	kafkaClient kafka.AdminClient, add func(name, status, msg string)) {
	if cl.Spec.REST == nil && cl.Annotations[mockRESTAnnotation] == "" {
		add("rest", doctorSkip, "Kafka REST not configured; mirror topics unavailable and KafkaTopic cannot detect out-of-band mirrors")
		return
	}
	rc, err := buildRESTClient(plan, files)
	if err != nil || rc == nil {
		msg := "Kafka REST client unavailable"
		if err != nil {
			msg = err.Error()
		}
		add("rest", doctorFail, msg)
		add("rest-cluster-binding", doctorFail, "not run: Kafka REST client unavailable")
		add("rest-mirrors", doctorFail, "not run: Kafka REST client unavailable")
		add("links", doctorFail, "not run: Kafka REST client unavailable")
		return
	}

	restID, restErr := rc.ClusterID(ctx)
	if restErr != nil {
		add("rest", doctorFail, restErr.Error())
	} else if cl.Spec.REST != nil && cl.Spec.REST.ClusterID != "" {
		// A configured clusterId is returned without any request, so this
		// check proves nothing about reachability; rest-mirrors does.
		add("rest", doctorPass, fmt.Sprintf("clusterId %s (configured; reachability checked by rest-mirrors)", restID))
	} else {
		add("rest", doctorPass, restID)
	}

	switch {
	case kafkaClient == nil:
		add("rest-cluster-binding", doctorFail, "not run: Kafka client unavailable")
	case restErr != nil:
		add("rest-cluster-binding", doctorFail, "not run: Kafka REST cluster ID unavailable")
	default:
		kafkaID, err := kafkaClient.ClusterID(ctx)
		switch {
		case err != nil:
			add("rest-cluster-binding", doctorFail, err.Error())
		case kafkaID == "" || restID == "" || kafkaID != restID:
			add("rest-cluster-binding", doctorFail, fmt.Sprintf("ClusterIdentityMismatch: kafka=%s rest=%s", kafkaID, restID))
		default:
			add("rest-cluster-binding", doctorPass, fmt.Sprintf("kafka=%s rest=%s", kafkaID, restID))
		}
	}

	if mirrors, err := rc.ListMirrors(ctx); err != nil {
		add("rest-mirrors", doctorFail, err.Error())
	} else {
		add("rest-mirrors", doctorPass, fmt.Sprintf("%d mirror topics (read-only checks; create/alter permissions not tested)", len(mirrors)))
	}

	links, err := rc.ListLinks(ctx)
	if err != nil {
		add("links", doctorFail, err.Error())
		return
	}
	for _, l := range links {
		name := "link:" + l.Name
		cfg, err := rc.LinkConfigs(ctx, l.Name)
		if err != nil {
			add(name, doctorFail, fmt.Sprintf("state=%s; link configs unreadable: %v", l.State, err))
			continue
		}
		prefix := cfg["cluster.link.prefix"]
		if prefix == "" {
			prefix = "none"
		}
		msg := fmt.Sprintf("state=%s prefix=%s", l.State, prefix)
		linkErr := mirror.LinkErrorSuffix(l)
		switch {
		case l.State == "PAUSED" || l.State == "LINK_PAUSED":
			add(name, doctorWarn, msg+" (link is paused)"+linkErr)
		case l.State != "ACTIVE":
			add(name, doctorFail, msg+linkErr)
		case cfg["auto.create.mirror.topics.enable"] == "true":
			add(name, doctorWarn, msg+" (auto.create.mirror.topics.enable=true; mirrors may be created outside Git)"+linkErr)
		default:
			add(name, doctorPass, msg)
		}
	}
}
