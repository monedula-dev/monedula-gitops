// Package executor applies a deterministic operation list (from internal/diff)
// against a kafka.AdminClient and a schemaregistry.Client. It is the heart of
// real apply: best-effort execution (a failure does not abort the run),
// prerequisite-skip (an ACL or schema op on a topic whose creation failed this
// run is skipped), and flag-gated approvals per the spec risk taxonomy
// (§17.1, §17.6, §17.7).
package executor

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	"github.com/monedula-dev/monedula-gitops/internal/operations"
	"github.com/monedula-dev/monedula-gitops/internal/rbac"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
	"github.com/monedula-dev/monedula-gitops/internal/secrets"
)

// Clients bundles the backends the executor dispatches against. Schema may be
// nil when no schema ops are present or the Schema Registry is not configured;
// a schema op encountered with a nil Schema client is recorded as Failed.
// MDS may be nil when no role-binding ops are present or MDS is not configured;
// a role-binding op encountered with a nil MDS client is recorded as Failed.
type Clients struct {
	Kafka  kafka.AdminClient
	Schema schemaregistry.Client
	MDS    mds.Client

	// Passwords resolves SCRAM password references (Operation.PasswordRef) at
	// EXECUTE time, immediately before UpsertScramCredential — the password
	// resolution seam for Create/Update/RotateScramCredential. It is passed in
	// like the clients (CLI: secrets.FileEnvResolver relative to the
	// cluster-config dir; operator: a Secret-backed resolver) so the executor
	// itself never chooses a source. May be nil when no user ops are present;
	// a user op encountered with a nil resolver is recorded as Failed.
	// Resolved plaintext is handed straight to the AdminClient and discarded:
	// it never appears in an Operation, an OpResult, or any error (resolver
	// and client errors identify field names/sources only, per the
	// kafka.ScramUpsert and secrets contracts).
	Passwords secrets.Resolver
}

// Approvals carry the operator's explicit consent for gated actions, set from
// the apply CLI flags.
type Approvals struct {
	Delete      bool // --allow-delete
	Destructive bool // --allow-destructive
	// Prune is the run-wide prune consent (spec §10.3): the CLI sets it from
	// `apply --prune`. A DeleteAcl executes iff Prune OR the operation's own
	// PruneAllowed (per-resource spec.prune consent, operator path) is set;
	// without either it is recorded as PruneDisabled, never attempted.
	Prune bool // --prune
}

// Status is the outcome of a single operation.
type Status string

const (
	Succeeded Status = "Succeeded"
	Failed    Status = "Failed"
	Skipped   Status = "Skipped"
	Blocked   Status = "Blocked"
	Rejected  Status = "Rejected"
	// ReportOnly marks an operation owned by a DetectOnly/ObserveOnly resource
	// (spec §16): it is reported but never executed, and is NOT a failure —
	// Result.OK treats it like Succeeded.
	ReportOnly Status = "ReportOnly"
	// PruneDisabled marks a DeleteAcl prune candidate without prune consent
	// (spec §10.3): neither the run-wide --prune approval nor the operation's
	// own PruneAllowed was set. It is reported, never executed, and is NOT a
	// failure — an accidentally truncated manifest must not fail the run (or
	// cut access). Result.OK treats it like Succeeded.
	PruneDisabled Status = "PruneDisabled"
	// Unsupported marks an approved operation the tool has no client method
	// for (today: UpdateReplicationFactor). It IS a failure for Result.OK —
	// the user asked to enforce a change that stands unresolved — but it is
	// TERMINAL: unlike Failed, retrying cannot resolve it (only a spec change
	// can), so the operator's retry signal must not treat it as transient.
	Unsupported Status = "Unsupported"
)

// OpResult pairs an operation with its outcome. Err is the verbatim client
// error string (empty unless Status is Failed or Unsupported).
//
// Redaction contract (spec §30.2): the EXECUTOR guarantees Err never contains
// a secret VALUE — only field names and sources (env var name, file path,
// Secret key, username, mechanism, op target). This is enforced at every
// error-construction site that touches secret material (see upsertScram's
// doc and the internal/secrets package contract, which never logs or embeds
// a resolved value in an error) and pinned by
// internal/executor.TestApplyScramPasswordNeverInResults. Callers (internal/
// output and any logger) do NOT need to apply their own redaction before
// rendering or logging Err — they may print it verbatim, and
// internal/output does exactly that (see
// TestRenderApplyResultFailedOpErrRendersVerbatim). Wrapped errors from
// underlying clients (kafka.AdminClient, mds.Client, schemaregistry.Client)
// are included verbatim in Err on the same assumption: those clients' own
// contracts must not echo secret values either (true today — SCRAM upsert
// errors from internal/kafka/franz name user/mechanism only, never a
// password).
type OpResult struct {
	Op     operations.Operation
	Status Status
	Err    string `json:",omitempty"`
}

// Result is the full set of per-operation outcomes, in execution order.
type Result struct{ Results []OpResult }

// Counts tallies results by status.
func (r Result) Counts() map[Status]int {
	out := make(map[Status]int)
	for _, res := range r.Results {
		out[res.Status]++
	}
	return out
}

// OK reports whether every operation succeeded (vacuously true when empty).
// ReportOnly counts as OK: a non-Enforce resource's drift is informational by
// definition and must not fail an apply run. PruneDisabled counts as OK for
// the same reason (spec §10.3): an unconsented prune candidate is reported
// divergence, not an apply failure.
func (r Result) OK() bool {
	for _, res := range r.Results {
		if res.Status != Succeeded && res.Status != ReportOnly && res.Status != PruneDisabled {
			return false
		}
	}
	return true
}

// Apply executes ops in order against c, honoring approvals and recording every
// outcome. It is best-effort: a failure is recorded and execution continues. A
// CreateAcl or schema op whose owning topic failed to be created this run is
// skipped rather than attempted.
func Apply(ctx context.Context, c Clients, ops []operations.Operation, ap Approvals) Result {
	var res Result
	failedTopics := make(map[string]bool)

	// diff.Compute emits ops in RENDER order, sorted by (Action, Target, Field).
	// Alphabetically "CreateAcl" < "CreateTopic", so a topic's CreateAcl is
	// listed BEFORE its CreateTopic. Executing in that order would attempt the
	// ACL before failedTopics is populated, so the prerequisite-skip could never
	// fire. We therefore reorder for EXECUTION into dependency order — all topic
	// (non-ACL) ops first, then ACL ops, then schema ops — using a stable
	// partition that preserves relative order within each group. Topics must
	// exist before their ACLs and schemas, so they run first; schemas run last.
	// This keeps execution deterministic without altering diff's rendering
	// order. Result.Results follows this execution order.
	ordered := executionOrder(ops)

	for _, op := range ordered {
		res.Results = append(res.Results, applyOne(ctx, c, op, ap, failedTopics))
	}
	return res
}

// executionOrder stable-partitions ops into four groups, run in order: topic
// (non-ACL, non-schema, non-role-binding) ops, then ACL ops, then schema ops,
// then role-binding ops. Relative order within each group is preserved. A
// topic's CreateTopic thus always runs before a CreateAcl or schema op
// referencing it. Role-binding ops run last (no ordering dependency on topics).
func executionOrder(ops []operations.Operation) []operations.Operation {
	var topics, acls, schemas, roleBindings []operations.Operation
	for _, op := range ops {
		switch {
		case isACLOp(op.Action):
			acls = append(acls, op)
		case isSchemaOp(op.Action):
			schemas = append(schemas, op)
		case isRoleBindingOp(op.Action):
			roleBindings = append(roleBindings, op)
		default:
			topics = append(topics, op)
		}
	}
	ordered := make([]operations.Operation, 0, len(ops))
	ordered = append(ordered, topics...)
	ordered = append(ordered, acls...)
	ordered = append(ordered, schemas...)
	ordered = append(ordered, roleBindings...)
	return ordered
}

func isACLOp(a operations.Action) bool {
	return a == operations.CreateAcl || a == operations.DeleteAcl
}

func isSchemaOp(a operations.Action) bool {
	return a == operations.RegisterSchema ||
		a == operations.RaiseSchemaCompatibility ||
		a == operations.LowerSchemaCompatibility ||
		a == operations.DeleteSubject ||
		a == operations.SchemaSuperseded
}

func isRoleBindingOp(a operations.Action) bool {
	return a == operations.AddRoleBinding || a == operations.RemoveRoleBinding
}

func applyOne(ctx context.Context, c Clients, op operations.Operation, ap Approvals, failedTopics map[string]bool) OpResult {
	// Mode gate (spec §16), checked before EVERYTHING else (Rejected included):
	// an op owned by a DetectOnly/ObserveOnly resource is purely informational —
	// it is recorded as ReportOnly, never executed, never blocked, never failed.
	// Empty mode means unattributed (operator path) and executes normally; the
	// operator decides modes per resource and only calls Apply for Enforce.
	if op.ReportOnly() {
		return OpResult{Op: op, Status: ReportOnly}
	}

	// Rejected ops are never executed.
	if op.Action == operations.Rejected {
		return OpResult{Op: op, Status: Rejected}
	}

	// SchemaSuperseded (spec §12.1) is terminal divergence: the manifest schema
	// is an older registered version of the subject, so re-registering can
	// never converge (the registry dedupes to the old version). Like
	// UpdateReplicationFactor it is recorded as Unsupported — a failure for
	// Result.OK, but one retrying cannot resolve — and never dispatched.
	if op.Action == operations.SchemaSuperseded {
		err := op.Message
		if err == "" {
			err = fmt.Sprintf("manifest schema is an older version of subject %s; update the manifest or roll the registry forward", op.Subject)
		}
		return OpResult{Op: op, Status: Unsupported, Err: err}
	}

	// Gate check: a gated action without its approval is blocked (not attempted).
	// This runs BEFORE the unsupported-op check (review I10) so a destructive
	// op the tool cannot even perform still honors its §17.1 gate: without
	// --allow-destructive an UpdateReplicationFactor is Blocked like any other
	// destructive op, not silently classified past the gate.
	switch operations.GateOf(op.Action) {
	case operations.GateDelete:
		if !ap.Delete {
			return OpResult{Op: op, Status: Blocked}
		}
	case operations.GateDestructive:
		if !ap.Destructive {
			return OpResult{Op: op, Status: Blocked}
		}
	case operations.GatePrune:
		// Prune consent (spec §10.3): run-wide (CLI --prune) OR per-op (the
		// covering resources' spec.prune, stamped by the diff). Unlike the
		// other gates this is not Blocked (a failure): the candidate is
		// reported as PruneDisabled and the run stays OK.
		if !ap.Prune && !op.PruneAllowed {
			return OpResult{Op: op, Status: PruneDisabled}
		}
	}

	// Replication factor changes have no client method: an approved one is the
	// terminal Unsupported (not Failed, which callers retry as transient).
	if op.Action == operations.UpdateReplicationFactor {
		return OpResult{Op: op, Status: Unsupported,
			Err: "replication factor changes are not supported; recreate the topic or use kafka-reassign-partitions"}
	}

	// Prerequisite-skip: an ACL on a topic whose creation failed this run.
	if op.Action == operations.CreateAcl && op.ACL != nil &&
		op.ACL.ResourceType == "topic" && failedTopics[op.ACL.ResourceName] {
		return OpResult{Op: op, Status: Skipped}
	}

	// Prerequisite-skip: a schema op whose owning topic failed to be created
	// this run. The subject would be orphaned, so do not touch the registry.
	if isSchemaOp(op.Action) && op.Topic != "" && failedTopics[op.Topic] {
		return OpResult{Op: op, Status: Skipped}
	}

	if err := dispatch(ctx, c, op, failedTopics); err != nil {
		return OpResult{Op: op, Status: Failed, Err: err.Error()}
	}
	return OpResult{Op: op, Status: Succeeded}
}

func dispatch(ctx context.Context, c Clients, op operations.Operation, failedTopics map[string]bool) error {
	if isSchemaOp(op.Action) {
		return dispatchSchema(ctx, c.Schema, op)
	}
	if isRoleBindingOp(op.Action) {
		return dispatchRoleBinding(ctx, c.MDS, op)
	}
	client := c.Kafka
	switch op.Action {
	case operations.CreateTopic:
		err := client.CreateTopic(ctx, kafka.TopicSpec{
			Name:              op.Target,
			Partitions:        op.Partitions,
			ReplicationFactor: op.ReplicationFactor,
			Config:            op.Config,
		})
		if err != nil {
			failedTopics[op.Target] = true
		}
		return err
	case operations.UpdateTopicConfig:
		return client.UpdateTopicConfig(ctx, op.Target, map[string]string{op.Field: op.To})
	case operations.IncreasePartitions:
		return client.CreatePartitions(ctx, op.Target, op.Partitions)
	case operations.DeleteTopic:
		return client.DeleteTopic(ctx, op.Target)
	case operations.CreateAcl:
		if op.ACL == nil {
			return fmt.Errorf("CreateAcl operation %q has no ACL", op.Target)
		}
		return client.CreateACLs(ctx, []kafka.ACLState{*op.ACL})
	case operations.DeleteAcl:
		if op.ACL == nil {
			return fmt.Errorf("DeleteAcl operation %q has no ACL", op.Target)
		}
		return client.DeleteACLs(ctx, []kafka.ACLState{*op.ACL})
	case operations.SetQuota, operations.UpdateQuota:
		return client.SetQuota(ctx, op.QuotaEntity, op.QuotaLimits)
	case operations.RemoveQuota:
		keys := make([]string, 0, len(op.QuotaLimits))
		for k := range op.QuotaLimits {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		return client.DeleteQuota(ctx, op.QuotaEntity, keys)
	case operations.CreateScramCredential, operations.UpdateScramCredential, operations.RotateScramCredential:
		return upsertScram(ctx, c, op)
	case operations.DeleteScramCredential:
		// Standalone credential deletion (operator finalizer path, T5; the CLI
		// diff never emits it — see diff.computeUserOps). The destructive gate
		// has already been honored by applyOne.
		return client.DeleteScramCredential(ctx, op.ScramUser, op.ScramMechanism)
	default:
		return fmt.Errorf("unsupported operation %s", op.Action)
	}
}

// dispatchRoleBinding maps a role-binding op to an mds.Client call. A nil
// client (MDS not configured) yields an error so the op is recorded as Failed
// rather than panicking, mirroring dispatchSchema's nil-client handling.
func dispatchRoleBinding(ctx context.Context, client mds.Client, op operations.Operation) error {
	if client == nil {
		return errors.New("MDS not configured")
	}
	if op.RoleBinding == nil {
		return fmt.Errorf("%s operation %q has no RoleBinding payload", op.Action, op.Target)
	}
	rb := toMDS(*op.RoleBinding)
	switch op.Action {
	case operations.AddRoleBinding:
		return client.AddRoleBinding(ctx, rb)
	case operations.RemoveRoleBinding:
		return client.RemoveRoleBinding(ctx, rb)
	default:
		return fmt.Errorf("unsupported role-binding operation %s", op.Action)
	}
}

// toMDS converts an rbac.RoleBinding (engine type) to an mds.RoleBinding (wire
// type). This is the rbac→mds seam: the executor converts at dispatch time,
// keeping mds decoupled from the engine.
func toMDS(rb rbac.RoleBinding) mds.RoleBinding {
	out := mds.RoleBinding{
		Principal: rb.Principal,
		Role:      rb.Role,
		Scope: mds.Scope{
			Type:         rb.Scope.Type,
			KafkaCluster: rb.Scope.KafkaCluster,
			SubCluster:   rb.Scope.SubCluster,
		},
	}
	if rb.Resource != nil {
		out.Resource = &mds.ResourcePattern{
			Type:        rb.Resource.Type,
			Name:        rb.Resource.Name,
			PatternType: rb.Resource.PatternType,
		}
	}
	return out
}

// upsertScram executes Create/Update/RotateScramCredential: it resolves the
// op's password REFERENCE via Clients.Passwords at this moment — the latest
// possible point, so the plaintext exists only for the duration of the upsert
// call and never rides on the op through diff/render/report — then upserts the
// declared (user, mechanism, iterations) credential. For a mechanism-change
// UpdateScramCredential (ScramDeleteMechanism set) it deletes the user's old
// mechanism ONLY after the upsert succeeded, so the principal can authenticate
// throughout; if the upsert fails the old credential is left untouched.
//
// Every error path here names fields/sources only (spec §30.2): the op target,
// the username, and whatever the resolver reports (env var name / file path /
// secret key) — never a password value, which this function never puts into an
// error and the AdminClient contract (kafka.ScramUpsert) forbids echoing.
func upsertScram(ctx context.Context, c Clients, op operations.Operation) error {
	if op.PasswordRef == nil {
		return fmt.Errorf("%s operation %q has no password reference", op.Action, op.Target)
	}
	if c.Passwords == nil {
		return fmt.Errorf("%s operation %q: no password resolver configured", op.Action, op.Target)
	}
	password, err := c.Passwords.Resolve(*op.PasswordRef)
	if err != nil {
		// Resolver errors identify the source (env var name, file path), never
		// a resolved value — see the secrets package contract.
		return fmt.Errorf("resolving password for user %q: %w", op.ScramUser, err)
	}
	if err := c.Kafka.UpsertScramCredential(ctx, kafka.ScramUpsert{
		User:       op.ScramUser,
		Mechanism:  op.ScramMechanism,
		Iterations: op.ScramIterations,
		Password:   password,
	}); err != nil {
		return err
	}
	if op.ScramDeleteMechanism != "" {
		if err := c.Kafka.DeleteScramCredential(ctx, op.ScramUser, op.ScramDeleteMechanism); err != nil {
			// The new credential is already live at this point — the upsert above
			// succeeded — so the principal now authenticates under BOTH mechanisms.
			// Re-applying will NOT retry this delete: computeUserOps only emits a
			// mechanism-change op when the declared mechanism is ABSENT live: once
			// the upsert lands, the declared mechanism is present+in-sync and the
			// stale one becomes an EXTRA live mechanism, which is out of diff scope
			// by design (see computeUserOps) and stays invisible to every future
			// diff. Say so plainly rather than suggesting a retry that cannot work.
			return fmt.Errorf("upserted %s credential for user %q but failed to delete the old %s credential (the principal now has BOTH mechanisms; this cannot be retried via re-apply — delete the old credential manually or via the operator): %w", op.ScramMechanism, op.ScramUser, op.ScramDeleteMechanism, err)
		}
	}
	return nil
}

// dispatchSchema maps a schema op to a schemaregistry.Client call. A nil client
// (Schema Registry not configured) yields an error so the op is recorded as
// Failed rather than panicking.
func dispatchSchema(ctx context.Context, client schemaregistry.Client, op operations.Operation) error {
	if client == nil {
		return errors.New("schema registry not configured")
	}
	switch op.Action {
	case operations.RegisterSchema:
		_, err := client.RegisterSchema(ctx, op.Subject, schemaregistry.Schema{
			Type:       schemaregistry.SchemaType(op.SchemaType),
			Definition: op.SchemaDef,
		})
		return err
	case operations.RaiseSchemaCompatibility, operations.LowerSchemaCompatibility:
		return client.SetCompatibility(ctx, op.Subject, op.Compatibility)
	case operations.DeleteSubject:
		return client.DeleteSubject(ctx, op.Subject)
	default:
		return fmt.Errorf("unsupported schema operation %s", op.Action)
	}
}
