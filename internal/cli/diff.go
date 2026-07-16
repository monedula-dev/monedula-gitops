package cli

import (
	"context"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/monedula-dev/monedula-gitops/internal/diff"
	"github.com/monedula-dev/monedula-gitops/internal/executor"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	"github.com/monedula-dev/monedula-gitops/internal/operations"
	"github.com/monedula-dev/monedula-gitops/internal/output"
	"github.com/monedula-dev/monedula-gitops/internal/pipeline"
	"github.com/monedula-dev/monedula-gitops/internal/rbac"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
	"github.com/monedula-dev/monedula-gitops/internal/secrets"
)

func newDiffCmd() *cobra.Command {
	var f sharedFlags
	cmd := &cobra.Command{
		Use:   "diff",
		Short: "Show the operations needed to reconcile live state toward the manifests",
		RunE: func(cmd *cobra.Command, _ []string) error {
			// rotatePasswords is always false here: rotation is an apply-time
			// action (apply --rotate-passwords); diff reports drift only.
			_, _, err := planAndCompute(cmd, &f, true, output.KindDiff, false)
			return err
		},
	}
	f.register(cmd)
	return cmd
}

// computeOps runs the shared build -> live -> compute path used by diff, verify,
// and apply. It validates the output format, builds the plan (errors -> exit 2),
// resolves the live Kafka + Schema Registry clients (bundled in clients), and
// computes the operation list WITHOUT rendering. The caller MUST defer the
// returned cleanup (it closes a real Kafka client; for mocks it is a no-op);
// cleanup is always non-nil and safe to call even on error. apply reuses
// clients to execute; diff/verify render and only need ops.
//
// rotatePasswords threads `apply --rotate-passwords` into diff.Desired: only
// apply (real and --dry-run) ever passes true; diff and verify hard-code false
// because rotation is an apply-time action, not drift.
func computeOps(ctx context.Context, f *sharedFlags, requireClusters bool, rotatePasswords bool) (plan *pipeline.Plan, clients executor.Clients, cleanup func(), ops []operations.Operation, err error) {
	cleanup = func() {}
	if err = validateOutputFormat(f.output); err != nil {
		return nil, clients, cleanup, nil, err
	}
	opts, err := f.options(requireClusters)
	if err != nil {
		return nil, clients, cleanup, nil, err
	}
	logger.Debug("loading manifests", "filenames", f.filenames)
	logger.Debug("validating")
	plan, err = pipeline.Build(opts) // load -> default -> validate -> compile
	if err != nil {
		return nil, clients, cleanup, nil, &ExitError{Code: 2, Msg: err.Error()}
	}

	logger.Debug("connecting to cluster", "cluster", plan.SelectedCluster)
	client, cleanup, err := buildLiveClient(plan, f.clusterConfigFiles)
	if err != nil {
		return plan, clients, cleanup, nil, err
	}
	clients.Kafka = client

	// Topic/ACL/quota reads are gated on their desired-kind presence, exactly
	// like the SR/MDS/SCRAM fetches below: a least-privilege credential that can
	// only manage the kinds its manifests declare must not need describe rights
	// for the others. Confluent Cloud API keys cannot DescribeClientQuotas at
	// all — the previously unconditional ListQuotas failed EVERY apply there
	// (CLUSTER_AUTHORIZATION_FAILED), even for topic-only manifest sets.
	// Skipping a read cannot change the computed ops: each computeXxxOps walks
	// the desired set, and prune candidacy derives from a scope built from the
	// desired set too — empty desired means empty scope, so nil live state of
	// that kind yields zero operations of that kind.
	var liveTopics []kafka.TopicState
	if len(plan.DesiredTopics) > 0 {
		liveTopics, err = client.ListTopics(ctx)
		if err != nil {
			return plan, clients, cleanup, nil, &ExitError{Code: 2, Msg: err.Error()}
		}
	}
	var liveACLStates []kafka.ACLState
	if len(plan.DesiredACLs) > 0 {
		liveACLStates, err = client.ListACLs(ctx)
		if err != nil {
			return plan, clients, cleanup, nil, &ExitError{Code: 2, Msg: err.Error()}
		}
	}
	var liveQuotas []kafka.QuotaState
	if len(plan.DesiredQuotas) > 0 {
		liveQuotas, err = client.ListQuotas(ctx)
		if err != nil {
			return plan, clients, cleanup, nil, &ExitError{Code: 2, Msg: err.Error()}
		}
	}
	// Unknown-role warning: before live state is read, surface any role with an
	// unrecognised name so the operator is informed (spec §40). The
	// warning is informational (validation accepts unknown roles silently per
	// spec §40); it uses the CLI logger at slog.LevelWarn — visible at the
	// default --log-level=warn (or above); suppressed only at --log-level=error.
	for _, rb := range plan.RoleBindings {
		if _, known := rbac.ClassifyRole(rb.Spec.Role); !known {
			logger.Warn("unknown RBAC role",
				"role", rb.Spec.Role,
				"name", rb.Namespace+"/"+rb.Name)
		}
	}
	for _, w := range plan.RBACWarnings {
		logger.Warn("RBAC access coarsened", "detail", w)
	}

	// MDS / role-binding diff: only contact MDS when the manifests declare
	// role bindings. Mirrors the schema-client build pattern above.
	var liveRoleBindings []rbac.RoleBinding
	if len(plan.DesiredRoleBindings) > 0 {
		mdsClient, merr := buildMDSClient(plan, f.clusterConfigFiles)
		if merr != nil {
			return plan, clients, cleanup, nil, &ExitError{Code: 2, Msg: merr.Error()}
		}
		if mdsClient == nil {
			return plan, clients, cleanup, nil, &ExitError{Code: 2, Msg: "MDS not configured for cluster " + strconv.Quote(plan.SelectedCluster)}
		}
		clients.MDS = mdsClient

		// Collect distinct MDS scopes from the desired set and list each once,
		// accumulating all live bindings. The scope filter (KafkaCluster, Type,
		// SubCluster) is already enforced by the mock/real ListRoleBindings.
		type scopeKey struct{ typ, kafka, sub string }
		seenScopes := map[scopeKey]bool{}
		for _, b := range plan.DesiredRoleBindings {
			sk := scopeKey{b.Scope.Type, b.Scope.KafkaCluster, b.Scope.SubCluster}
			if seenScopes[sk] {
				continue
			}
			seenScopes[sk] = true
			listed, lerr := mdsClient.ListRoleBindings(ctx, mds.Scope{
				Type:         b.Scope.Type,
				KafkaCluster: b.Scope.KafkaCluster,
				SubCluster:   b.Scope.SubCluster,
			})
			if lerr != nil {
				return plan, clients, cleanup, nil, &ExitError{Code: 2, Msg: lerr.Error()}
			}
			liveRoleBindings = append(liveRoleBindings, fromMDSRoleBindings(listed)...)
		}
	}

	// SCRAM credential diff: only read credentials when the manifests declare
	// users (mirrors the conditional SR/MDS fetch pattern), and bound the read
	// to the declared usernames. The read returns identities only (user,
	// mechanism, iterations) — Kafka never exposes passwords. The password
	// resolution seam is wired here too: the executor resolves each op's
	// PasswordRef at execute time via clients.Passwords, using the same
	// FileEnvResolver base directory as every other CLI secret (file refs
	// resolve relative to the cluster-config directory).
	var liveScram []kafka.ScramCredential
	if len(plan.DesiredUsers) > 0 {
		usernames := make([]string, 0, len(plan.DesiredUsers))
		for _, du := range plan.DesiredUsers {
			usernames = append(usernames, du.Credential.Username)
		}
		liveScram, err = client.ListScramCredentials(ctx, usernames...)
		if err != nil {
			return plan, clients, cleanup, nil, &ExitError{Code: 2, Msg: err.Error()}
		}
		clients.Passwords = secrets.FileEnvResolver{BaseDir: baseDir(f.clusterConfigFiles)}
	}

	logger.Debug("reading live state",
		"topics", liveReadCount(len(plan.DesiredTopics) > 0, len(liveTopics)),
		"acls", liveReadCount(len(plan.DesiredACLs) > 0, len(liveACLStates)),
		"quotas", liveReadCount(len(plan.DesiredQuotas) > 0, len(liveQuotas)),
		"roleBindings", liveReadCount(len(plan.DesiredRoleBindings) > 0, len(liveRoleBindings)),
		"scramCredentials", liveReadCount(len(plan.DesiredUsers) > 0, len(liveScram)))

	// Schema diff: only query the registry when the manifests declare schemas.
	// Live subject queries are bounded to the desired subjects (no full listing).
	var liveSchemas []schemaregistry.SubjectState
	var superseded map[string]int
	var globalCompat string
	if len(plan.DesiredSchemas) > 0 {
		schemaClient, serr := buildSchemaClient(plan, f.clusterConfigFiles)
		if serr != nil {
			return plan, clients, cleanup, nil, &ExitError{Code: 2, Msg: serr.Error()}
		}
		if schemaClient == nil {
			return plan, clients, cleanup, nil, &ExitError{Code: 2, Msg: "schema registry not configured for cluster " + strconv.Quote(plan.SelectedCluster)}
		}
		// The schema client intentionally has no cleanup: the schemaregistry.Client
		// interface defines no Close, and the confluent implementation uses a stdlib
		// *http.Client with nothing to release. Only the Kafka client's cleanup is
		// returned above.
		clients.Schema = schemaClient
		// Global compatibility level, fetched ONCE per run: an unset subject's
		// effective level is this global default, so the diff uses it as the
		// baseline when classifying a first-time subject-level set (spec §17.1
		// — a level below the default is a gated Lower). A failure here (older
		// SR without GET /config) deliberately does NOT fail the run: the level
		// stays "" (unknown) and the diff falls back to the legacy
		// any-initial-set-is-a-Raise classification.
		// Mirror: operator observeTopicLive (internal/operator/reconcile/reconcile.go).
		if level, gerr := schemaClient.GetGlobalCompatibility(ctx); gerr == nil {
			globalCompat = level
		} else {
			logger.Warn("could not read Schema Registry global compatibility; first-time compatibility sets classify as Raise", "error", gerr)
		}
		for _, ds := range plan.DesiredSchemas {
			st, serr := schemaClient.GetSubject(ctx, ds.Subject)
			if serr != nil {
				return plan, clients, cleanup, nil, &ExitError{Code: 2, Msg: serr.Error()}
			}
			if st == nil {
				// Subject has no registered versions. In GOVERNANCE mode
				// (spec §12.2, empty Definition) monedula still manages the
				// subject compatibility level, which Confluent permits to exist
				// before any version (PUT/GET /config/{subject}). Synthesize a
				// live entry from GetCompatibility so the diff sees the current
				// subject-level config (an absent config reads back as ""). In
				// content mode an absent subject is left out -> RegisterSchema
				// drift, as before.
				// Mirror: operator observeTopicLive (internal/operator/reconcile/reconcile.go).
				if ds.Definition == "" {
					level, serr := schemaClient.GetCompatibility(ctx, ds.Subject)
					if serr != nil {
						return plan, clients, cleanup, nil, &ExitError{Code: 2, Msg: serr.Error()}
					}
					liveSchemas = append(liveSchemas, schemaregistry.SubjectState{
						Subject:       ds.Subject,
						Compatibility: level,
					})
				}
				continue
			}
			level, serr := schemaClient.GetCompatibility(ctx, ds.Subject)
			if serr != nil {
				return plan, clients, cleanup, nil, &ExitError{Code: 2, Msg: serr.Error()}
			}
			st.Compatibility = level
			liveSchemas = append(liveSchemas, *st)

			// Governance mode (empty Definition) never registers versions, so
			// skip the supersession probe — a producer-registered version is
			// not drift.
			if ds.Definition == "" {
				continue
			}

			// SchemaSuperseded probe (spec §12.1): the diff engine has no
			// registry client, so supersession is detected HERE, where live
			// state is read. For a desired subject diverging from the latest
			// version, LookupSchema tells whether the desired schema is
			// already registered as an older version — if so the diff emits
			// the terminal SchemaSuperseded instead of a RegisterSchema that
			// would dedupe and never converge.
			if !diff.SchemaEqual(ds.Type, ds.Definition, st.Schema.Definition) {
				v, serr := schemaClient.LookupSchema(ctx, ds.Subject, schemaregistry.Schema{
					Type:       schemaregistry.SchemaType(ds.Type),
					Definition: ds.Definition,
				})
				if serr != nil {
					return plan, clients, cleanup, nil, &ExitError{Code: 2, Msg: serr.Error()}
				}
				if v > 0 {
					if superseded == nil {
						superseded = map[string]int{}
					}
					superseded[ds.Subject] = v
				}
			}
		}
	}

	ops = diff.Compute(
		diff.Desired{Topics: plan.DesiredTopics, ACLs: plan.DesiredACLs, Schemas: plan.DesiredSchemas, Quotas: plan.DesiredQuotas, Users: plan.DesiredUsers, RotatePasswords: rotatePasswords, Scope: plan.Scope, RoleBindings: plan.DesiredRoleBindings, RoleBindingScope: plan.RoleBindingScope},
		diff.Live{Topics: liveTopics, ACLs: liveACLs(liveACLStates), Schemas: liveSchemas, Quotas: liveQuotas, ScramCredentials: liveScram, SupersededSchemas: superseded, RoleBindings: liveRoleBindings, GlobalCompatibility: globalCompat},
	)
	logger.Info("computed operations", "cluster", plan.SelectedCluster, "count", len(ops))
	return plan, clients, cleanup, ops, nil
}

// liveReadCount renders one kind's live-read result for the debug log: the
// item count when the read ran, "skipped" when the kind is absent from the
// manifests and the read was scoped away.
func liveReadCount(read bool, n int) any {
	if !read {
		return "skipped"
	}
	return n
}

// planAndCompute runs computeOps and renders the operations to the command's
// stdout under the command-specific document kind (diff -> DiffOutput, verify
// -> VerifyOutput, apply --dry-run -> ApplyDryRunOutput). The op list is
// returned so verify can act on drift; the plan so apply --dry-run can emit
// the spec §30.3 lockout warnings. rotatePasswords is forwarded to computeOps;
// only apply --dry-run passes true (the rotate ops are SHOWN, consistent with
// every other planned-but-not-applied op).
func planAndCompute(cmd *cobra.Command, f *sharedFlags, requireClusters bool, kind string, rotatePasswords bool) (*pipeline.Plan, []operations.Operation, error) {
	plan, _, cleanup, ops, err := computeOps(cmd.Context(), f, requireClusters, rotatePasswords) // clients.Schema unused: render-only path
	defer cleanup()
	if err != nil {
		return nil, nil, err
	}

	rendered, err := output.Render(ops, f.output, plan.SelectedCluster, kind)
	if err != nil {
		return nil, nil, &ExitError{Code: 2, Msg: err.Error()}
	}
	if _, err := cmd.OutOrStdout().Write(rendered); err != nil {
		return nil, nil, &ExitError{Code: 2, Msg: err.Error()}
	}
	return plan, ops, nil
}
