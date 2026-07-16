package output

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"sigs.k8s.io/yaml"

	"github.com/monedula-dev/monedula-gitops/internal/executor"
	"github.com/monedula-dev/monedula-gitops/internal/operations"
)

// Change is the serialized form of one operation (stable field order).
// Mode is set only for non-Enforce operations (spec §16): Enforce is the
// default and stays invisible, matching the §17.5 example output.
type Change struct {
	Action           operations.Action `json:"action"`
	Kind             string            `json:"kind,omitempty"`
	Namespace        string            `json:"namespace,omitempty"`
	Name             string            `json:"name,omitempty"`
	Target           string            `json:"target,omitempty"`
	Field            string            `json:"field,omitempty"`
	From             string            `json:"from,omitempty"`
	To               string            `json:"to,omitempty"`
	Mode             string            `json:"mode,omitempty"`
	Risk             operations.Risk   `json:"risk,omitempty"`
	RequiresApproval bool              `json:"requiresApproval"`
	Message          string            `json:"message,omitempty"`
}

// Document kinds for the yaml/json operation-list output: each command stamps
// the kind that names it, so a consumer can tell an apply --dry-run plan from
// a diff or a verify report. ApplyDryRunOutput is the spec §17.5 shape.
const (
	KindApplyDryRun = "ApplyDryRunOutput"
	KindDiff        = "DiffOutput"
	KindVerify      = "VerifyOutput"
)

// Document is the operation-list shape (spec §17.5) used for yaml/json.
type Document struct {
	Kind    string   `json:"kind"`
	Cluster string   `json:"cluster"`
	Changes []Change `json:"changes"`
}

func toDocument(ops []operations.Operation, cluster, kind string) Document {
	changes := make([]Change, 0, len(ops))
	for _, o := range ops {
		changes = append(changes, Change{
			Action: o.Action, Kind: o.Kind, Namespace: o.Namespace, Name: o.Name,
			Target: o.Target, Field: o.Field, From: o.From, To: o.To,
			Mode: renderedMode(o.Mode),
			Risk: o.Risk, RequiresApproval: o.RequiresApproval, Message: o.Message,
		})
	}
	return Document{Kind: kind, Cluster: cluster, Changes: changes}
}

// RenderDocument is THE json/yaml/human format switch shared by every CLI
// output surface (Render, RenderApplyResult, the importer's RenderSummary):
// doc is serialized as indented JSON or as YAML (sigs.k8s.io/yaml — JSON tag
// order), or, for the human format, human() renders it. Any other format is
// rejected with the canonical unsupported-format error. Determinism is the
// caller's property: RenderDocument adds no ordering of its own.
func RenderDocument(format string, doc any, human func() []byte) ([]byte, error) {
	switch format {
	case "json":
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.SetIndent("", "  ")
		if err := enc.Encode(doc); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	case "yaml":
		return yaml.Marshal(doc)
	case "human":
		return human(), nil
	default:
		return nil, fmt.Errorf("unsupported output format %q (want human, yaml, or json)", format)
	}
}

// Render serializes operations in the requested format under the given document
// kind (one of the Kind* constants; only yaml/json carry it). Output is
// deterministic: operations arrive already deterministically ordered and Render
// preserves that order (it does NOT sort).
func Render(ops []operations.Operation, format, cluster, kind string) ([]byte, error) {
	return RenderDocument(format, toDocument(ops, cluster, kind), func() []byte { return renderHuman(ops, cluster) })
}

// ResultEntry is the serialized form of one applied operation's outcome (stable
// field order). It is built from explicit Operation fields only — never from the
// json:"-" executable payload (Partitions/Config/ACL), which must not render.
//
// Namespace is intentionally omitted (unlike Change): Target already carries the
// fully-qualified topic name, which is the identity that matters for an applied result.
// Mode mirrors Change.Mode: set only for non-Enforce operations (spec §16).
type ResultEntry struct {
	Action operations.Action `json:"action"`
	Kind   string            `json:"kind,omitempty"`
	Name   string            `json:"name,omitempty"`
	Target string            `json:"target,omitempty"`
	Field  string            `json:"field,omitempty"`
	From   string            `json:"from,omitempty"`
	To     string            `json:"to,omitempty"`
	Mode   string            `json:"mode,omitempty"`
	Risk   operations.Risk   `json:"risk,omitempty"`
	Status executor.Status   `json:"status"`
	Error  string            `json:"error,omitempty"`
}

// ResultDocument is the ApplyResult shape used for yaml/json. summary maps each
// observed status to its count; results preserves the executor's order.
type ResultDocument struct {
	Kind    string                  `json:"kind"`
	Cluster string                  `json:"cluster"`
	Summary map[executor.Status]int `json:"summary"`
	Results []ResultEntry           `json:"results"`
}

func toResultDocument(res executor.Result, cluster string) ResultDocument {
	entries := make([]ResultEntry, 0, len(res.Results))
	for _, r := range res.Results {
		o := r.Op
		entries = append(entries, ResultEntry{
			Action: o.Action, Kind: o.Kind, Name: o.Name, Target: o.Target,
			Field: o.Field, From: o.From, To: o.To,
			Mode: renderedMode(o.Mode), Risk: o.Risk,
			Status: r.Status, Error: r.Err,
		})
	}
	summary := make(map[executor.Status]int)
	for s, c := range res.Counts() {
		summary[s] = c
	}
	return ResultDocument{Kind: "ApplyResult", Cluster: cluster, Summary: summary, Results: entries}
}

// RenderApplyResult serializes an executor.Result in the requested format. The
// document is deterministic: results follow the executor's order and the human
// summary sorts status keys alphabetically. JSON/YAML map keys serialize sorted.
func RenderApplyResult(res executor.Result, format, cluster string) ([]byte, error) {
	return RenderDocument(format, toResultDocument(res, cluster), func() []byte { return renderApplyResultHuman(res, cluster) })
}

func renderApplyResultHuman(res executor.Result, cluster string) []byte {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "Cluster: %s\n", cluster)
	for _, r := range res.Results {
		o := r.Op
		fmt.Fprintf(&buf, "%s %s %s/%s %s", r.Status, o.Action, o.Kind, o.Name, o.Target)
		if o.Field != "" {
			fmt.Fprintf(&buf, " [field=%s %s -> %s]", o.Field, o.From, o.To)
		}
		fmt.Fprintf(&buf, " (risk=%s)", o.Risk)
		rotateAnnotation(&buf, o)
		if m := renderedMode(o.Mode); m != "" {
			fmt.Fprintf(&buf, " (mode=%s, report-only)", m)
		}
		if r.Err != "" {
			fmt.Fprintf(&buf, " error=%s", r.Err)
		}
		buf.WriteByte('\n')
	}

	counts := res.Counts()
	statuses := make([]string, 0, len(counts))
	for s := range counts {
		statuses = append(statuses, string(s))
	}
	sort.Strings(statuses)
	parts := make([]string, 0, len(statuses))
	for _, s := range statuses {
		parts = append(parts, fmt.Sprintf("%d %s", counts[executor.Status(s)], s))
	}
	fmt.Fprintf(&buf, "Applied: %s\n", strings.Join(parts, ", "))
	return buf.Bytes()
}

func renderHuman(ops []operations.Operation, cluster string) []byte {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "Cluster: %s\n", cluster)
	if len(ops) == 0 {
		buf.WriteString("No changes.\n")
		return buf.Bytes()
	}
	for _, o := range ops {
		fmt.Fprintf(&buf, "%s %s/%s %s", o.Action, o.Kind, o.Name, o.Target)
		if o.Field != "" {
			fmt.Fprintf(&buf, " [field=%s %s -> %s]", o.Field, o.From, o.To)
		}
		fmt.Fprintf(&buf, " (risk=%s approval=%t)", o.Risk, o.RequiresApproval)
		if m := renderedMode(o.Mode); m != "" {
			fmt.Fprintf(&buf, " (mode=%s, report-only)", m)
		}
		if pruneCandidate(o) {
			buf.WriteString(" (prune candidate; enable with --prune)")
		}
		rotateAnnotation(&buf, o)
		if o.Message != "" {
			fmt.Fprintf(&buf, " message=%s", o.Message)
		}
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

// pruneCandidate reports whether o is a DeleteAcl or RemoveRoleBinding without
// prune consent (spec §10.3): it always renders for visibility, but apply will
// record it as PruneDisabled rather than deleting unless --prune (or, in the
// operator, the covering resources' spec.prune) supplies consent.
func pruneCandidate(o operations.Operation) bool {
	switch o.Action {
	case operations.DeleteAcl, operations.RemoveRoleBinding:
		return !o.PruneAllowed
	}
	return false
}

// rotateAnnotation appends the "(--rotate-passwords)" marker to buf when o is
// a RotateScramCredential: rotation is event-driven, never drift, so a plan/
// result reader must be told the op exists only because the flag was set,
// not because anything was out of sync. Shared by renderHuman and
// renderApplyResultHuman (mirrors the renderedMode/pruneCandidate precedent).
func rotateAnnotation(buf *bytes.Buffer, o operations.Operation) {
	if o.Action == operations.RotateScramCredential {
		buf.WriteString(" (--rotate-passwords)")
	}
}

// renderedMode maps an operation's reconciliation mode to its rendered form:
// only the non-Enforce modes (DetectOnly/ObserveOnly) are surfaced. Enforce —
// and the empty unattributed mode — is the default and stays invisible,
// matching the spec §17.5 example output.
func renderedMode(mode string) string {
	if mode == "" || mode == operations.ModeEnforce {
		return ""
	}
	return mode
}
