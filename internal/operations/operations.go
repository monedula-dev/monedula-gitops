package operations

import (
	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/rbac"
)

type Action string

// Reconciliation modes (spec §16), carried on Operation.Mode. An empty mode
// means "unattributed" and is treated like Enforce by the executor: the
// operator's reconcile core decides modes per resource BEFORE calling the
// executor (it only applies in Enforce mode), so its ops legitimately carry no
// mode. The CLI pipeline stamps every op with its owning resource's mode.
const (
	ModeEnforce     = "Enforce"
	ModeDetectOnly  = "DetectOnly"
	ModeObserveOnly = "ObserveOnly"
)

const (
	NoOp                    Action = "NoOp"
	CreateTopic             Action = "CreateTopic"
	UpdateTopicConfig       Action = "UpdateTopicConfig"
	IncreasePartitions      Action = "IncreasePartitions"
	UpdateReplicationFactor Action = "UpdateReplicationFactor"
	DeleteTopic             Action = "DeleteTopic"
	CreateAcl               Action = "CreateAcl"
	DeleteAcl               Action = "DeleteAcl"
	Rejected                Action = "Rejected"

	RegisterSchema           Action = "RegisterSchema"
	RaiseSchemaCompatibility Action = "RaiseSchemaCompatibility"
	LowerSchemaCompatibility Action = "LowerSchemaCompatibility"
	DeleteSubject            Action = "DeleteSubject"

	SetQuota    Action = "SetQuota"
	UpdateQuota Action = "UpdateQuota"
	RemoveQuota Action = "RemoveQuota"

	// RBAC role binding ops (spec §40).
	AddRoleBinding    Action = "AddRoleBinding"
	RemoveRoleBinding Action = "RemoveRoleBinding"

	// SCRAM credential ops (v0.35 KafkaUser, spec §2-§4). The diff compares
	// ONLY the observable identity (username, mechanism, iterations) —
	// passwords are write-only in Kafka and never drift-detected.
	//
	// CreateScramCredential: the declared (username, mechanism) is absent live.
	// UpdateScramCredential: the declared mechanism exists but iterations
	// mismatch (when the spec sets them), OR a mechanism change (live has only
	// the OTHER mechanism for the user): ONE op whose apply upserts the
	// declared mechanism and then deletes the old one (see
	// Operation.ScramDeleteMechanism).
	// RotateScramCredential: emitted ONLY under `apply --rotate-passwords` for
	// declared users whose identity is otherwise in sync — an event-driven
	// re-upsert of the password from its configured source, never plain drift.
	// DeleteScramCredential: standalone credential deletion. The CLI diff
	// NEVER emits it (an undeclared live user is out of scope, and the
	// mechanism-change delete rides inside UpdateScramCredential); it exists
	// for the executor/operator finalizer path (T5) and stays gated.
	CreateScramCredential Action = "CreateScramCredential"
	UpdateScramCredential Action = "UpdateScramCredential"
	RotateScramCredential Action = "RotateScramCredential"
	DeleteScramCredential Action = "DeleteScramCredential"

	// SchemaSuperseded marks non-convergeable schema drift (spec §12.1): the
	// manifest schema differs from the subject's LATEST version but is
	// registered as an OLDER one, so re-registering would dedupe to the old
	// version and never become latest. Like Rejected it is carried for
	// reporting but never executed; absent from the taxonomy by design (no
	// risk, no gate). The executor records it as the terminal Unsupported.
	SchemaSuperseded Action = "SchemaSuperseded"
)

type Risk string

const (
	RiskNone     Risk = ""
	RiskLow      Risk = "Low"
	RiskMedium   Risk = "Medium"
	RiskHigh     Risk = "High"
	RiskCritical Risk = "Critical"
)

type Gate string

const (
	GateNone        Gate = ""
	GateDestructive Gate = "allow-destructive"
	GateDelete      Gate = "allow-delete"
	// GatePrune is the opt-in prune consent (spec §10.3, Flux-style): it is
	// satisfied by the run-wide CLI flag `apply --prune` (executor
	// Approvals.Prune) OR by per-operation consent (Operation.PruneAllowed,
	// stamped by the diff from the covering scope's spec.prune). It supersedes
	// the former GateDestructive gating of DeleteAcl.
	GatePrune Gate = "prune"
)

// taxonomy from spec §17.1
var taxonomy = map[Action]struct {
	Risk Risk
	Gate Gate
}{
	NoOp:                    {RiskNone, GateNone},
	CreateTopic:             {RiskLow, GateNone},
	UpdateTopicConfig:       {RiskLow, GateNone},
	CreateAcl:               {RiskLow, GateNone},
	IncreasePartitions:      {RiskMedium, GateDestructive},
	UpdateReplicationFactor: {RiskHigh, GateDestructive},
	DeleteAcl:               {RiskMedium, GatePrune},
	DeleteTopic:             {RiskCritical, GateDelete},

	RegisterSchema:           {RiskLow, GateNone},
	RaiseSchemaCompatibility: {RiskLow, GateNone},
	LowerSchemaCompatibility: {RiskHigh, GateDestructive},
	DeleteSubject:            {RiskHigh, GateDestructive},

	SetQuota:    {RiskLow, GateNone},
	UpdateQuota: {RiskLow, GateNone},
	// RemoveQuota authoritatively DELETES a live limit (unthrottling a client —
	// operationally riskier than DeleteAcl), so unlike the reversible
	// Set/UpdateQuota it is gated like the other destructive ops.
	RemoveQuota: {RiskMedium, GateDestructive},

	AddRoleBinding:    {RiskLow, GateNone},
	RemoveRoleBinding: {RiskMedium, GatePrune},

	// CreateScramCredential establishes a credential that did not exist (no
	// live client's access can break) and RotateScramCredential re-upserts a
	// password for an in-sync user under the explicit --rotate-passwords flag
	// — both Low, ungated. UpdateScramCredential is Medium (it re-writes a
	// credential live clients authenticate with; a mechanism change also drops
	// the old mechanism once the new one is in place) but ungated: it
	// converges the DECLARED credential, like UpdateQuota. DeleteScramCredential
	// removes a principal's ability to authenticate entirely — following the
	// RemoveQuota precedent (authoritative deletion of live state), it is
	// destructive-gated.
	CreateScramCredential: {RiskLow, GateNone},
	UpdateScramCredential: {RiskMedium, GateNone},
	RotateScramCredential: {RiskLow, GateNone},
	DeleteScramCredential: {RiskMedium, GateDestructive},
}

// RiskOf and GateOf return the risk/gate for an Action. An Action not present
// in the taxonomy (e.g. Rejected) returns the zero value — RiskNone / GateNone
// — by design. This is relied upon by Rejected: a rejected op is never applied,
// so it carries no risk and requires no approval (New maps GateNone -> false).
func RiskOf(a Action) Risk { return taxonomy[a].Risk }
func GateOf(a Action) Gate { return taxonomy[a].Gate }

type Operation struct {
	Action           Action
	Kind             string
	Namespace        string
	Name             string
	Target           string
	Field            string `json:",omitempty"`
	From             string `json:",omitempty"`
	To               string `json:",omitempty"`
	Risk             Risk
	RequiresApproval bool
	Message          string `json:",omitempty"`

	// Mode is the reconciliation mode (spec §16) of the resource that owns this
	// operation: Enforce, DetectOnly, or ObserveOnly. Empty means unattributed
	// (operator path; treated as Enforce by the executor). json:"-" like the
	// payload fields: the output package decides how modes render.
	Mode string `json:"-"`

	// PruneAllowed is per-operation prune consent (spec §10.3), meaningful for
	// DeleteAcl only. The diff stamps it from the covering managed-scope entry:
	// true iff EVERY resource whose scope covers the tuple set spec.prune. The
	// executor prunes iff (Approvals.Prune || PruneAllowed) — the CLI's --prune
	// is run-wide consent, this field is the operator's per-resource consent.
	// json:"-" like Mode: the output package decides how prune consent renders.
	PruneAllowed bool `json:"-"`

	// Executable payload (internal; never rendered — json:"-").
	Partitions        int               `json:"-"` // CreateTopic / IncreasePartitions target count
	ReplicationFactor int               `json:"-"` // CreateTopic
	Config            map[string]string `json:"-"` // CreateTopic full config
	ACL               *kafka.ACLState   `json:"-"` // CreateAcl / DeleteAcl

	// Schema-op payload (internal; never rendered — json:"-").
	Subject       string `json:"-"` // RegisterSchema / *SchemaCompatibility / DeleteSubject
	SchemaType    string `json:"-"` // RegisterSchema: AVRO|JSON|PROTOBUF
	SchemaDef     string `json:"-"` // RegisterSchema: schema body
	Compatibility string `json:"-"` // Raise/LowerSchemaCompatibility: target level
	Topic         string `json:"-"` // owning topic name, for executor prerequisite-skip

	// Quota-op payload (internal; never rendered — json:"-").
	QuotaEntity []kafka.QuotaEntityComponent `json:"-"` // SetQuota / UpdateQuota / RemoveQuota: the entity components
	QuotaLimits map[string]float64           `json:"-"` // Set/Update: limit keys to SET; RemoveQuota: keys to REMOVE (values ignored)

	// RBAC role-binding payload (internal; never rendered — json:"-").
	// Carries the rbac type; the executor and CLI convert internal/rbac types to these wire types at dispatch.
	RoleBinding *rbac.RoleBinding `json:"-"` // AddRoleBinding / RemoveRoleBinding

	// SCRAM credential op payload (internal; never rendered — json:"-").
	ScramUser       string `json:"-"` // username (Kafka principal, without "User:" prefix)
	ScramMechanism  string `json:"-"` // Create/Update/Rotate: declared mechanism to upsert; DeleteScramCredential: mechanism to delete
	ScramIterations int32  `json:"-"` // Create/Update/Rotate: iteration count; 0 = broker/adapter default, never drift-compared
	// ScramDeleteMechanism is set ONLY on a mechanism-change UpdateScramCredential:
	// the user's old live mechanism, deleted by the executor AFTER the declared
	// mechanism has been upserted (upsert-then-delete keeps the principal
	// authenticable throughout). Empty on every other op.
	ScramDeleteMechanism string `json:"-"`
	// PasswordRef is the password *reference* (spec.password.valueFrom) for
	// Create/Update/RotateScramCredential. The executor resolves it via the
	// run's secrets.Resolver immediately before UpsertScramCredential — the
	// plaintext never flows through the pipeline, the diff, this struct, or
	// any rendered output, and resolver errors identify the source (env var
	// name / file path) only, never a value.
	PasswordRef *v1alpha1.ValueFrom `json:"-"`
}

// New builds an Operation with risk/approval filled from the taxonomy.
func New(a Action) Operation {
	return Operation{Action: a, Risk: RiskOf(a), RequiresApproval: GateOf(a) != GateNone}
}

// ReportOnly reports whether the operation belongs to a resource whose
// reconciliation mode forbids mutation (DetectOnly/ObserveOnly, spec §16).
// Such operations are rendered/reported but never executed.
func (o Operation) ReportOnly() bool {
	return o.Mode == ModeDetectOnly || o.Mode == ModeObserveOnly
}
