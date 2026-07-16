package importer

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"sigs.k8s.io/yaml"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/output"
)

// sortedTopics returns the Result's topics ordered by namespace then name.
func sortedTopics(res Result) []*v1alpha1.KafkaTopic {
	ts := make([]*v1alpha1.KafkaTopic, len(res.Topics))
	copy(ts, res.Topics)
	sort.Slice(ts, func(i, j int) bool {
		a, b := ts[i].ObjectMeta, ts[j].ObjectMeta
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Name < b.Name
	})
	return ts
}

// sortedPolicies returns the Result's policies ordered by namespace then name.
func sortedPolicies(res Result) []*v1alpha1.KafkaAccessPolicy {
	ps := make([]*v1alpha1.KafkaAccessPolicy, len(res.Policies))
	copy(ps, res.Policies)
	sort.Slice(ps, func(i, j int) bool {
		a, b := ps[i].ObjectMeta, ps[j].ObjectMeta
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Name < b.Name
	})
	return ps
}

// sortedQuotas returns the Result's quotas ordered by namespace then name.
func sortedQuotas(res Result) []*v1alpha1.KafkaQuota {
	qs := make([]*v1alpha1.KafkaQuota, len(res.Quotas))
	copy(qs, res.Quotas)
	sort.Slice(qs, func(i, j int) bool {
		a, b := qs[i].ObjectMeta, qs[j].ObjectMeta
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Name < b.Name
	})
	return qs
}

// sortedRoleBindings returns the Result's role bindings ordered by namespace then name.
func sortedRoleBindings(res Result) []*v1alpha1.KafkaRoleBinding {
	rbs := make([]*v1alpha1.KafkaRoleBinding, len(res.RoleBindings))
	copy(rbs, res.RoleBindings)
	sort.Slice(rbs, func(i, j int) bool {
		a, b := rbs[i].ObjectMeta, rbs[j].ObjectMeta
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Name < b.Name
	})
	return rbs
}

// sortedUsers returns the Result's users ordered by namespace then name.
func sortedUsers(res Result) []*v1alpha1.KafkaUser {
	us := make([]*v1alpha1.KafkaUser, len(res.Users))
	copy(us, res.Users)
	sort.Slice(us, func(i, j int) bool {
		a, b := us[i].ObjectMeta, us[j].ObjectMeta
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		return a.Name < b.Name
	})
	return us
}

// RenderManifests produces a deterministic multi-document YAML stream: all
// Topics first (sorted by namespace then name), then all Policies (same order),
// then all Quotas, then all RoleBindings, then all Users (same order). Each
// document is serialized via sigs.k8s.io/yaml (JSON tag order, omitempty),
// separated by a "---\n" line, with a trailing newline.
func RenderManifests(res Result) ([]byte, error) {
	var buf bytes.Buffer
	for _, tp := range sortedTopics(res) {
		if err := writeDoc(&buf, tp); err != nil {
			return nil, err
		}
	}
	for _, pol := range sortedPolicies(res) {
		if err := writeDoc(&buf, pol); err != nil {
			return nil, err
		}
	}
	for _, q := range sortedQuotas(res) {
		if err := writeDoc(&buf, q); err != nil {
			return nil, err
		}
	}
	for _, rb := range sortedRoleBindings(res) {
		if err := writeDoc(&buf, rb); err != nil {
			return nil, err
		}
	}
	for _, u := range sortedUsers(res) {
		if err := writeDoc(&buf, u); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// writeDoc appends one YAML document (with leading "---\n" separator) to buf.
func writeDoc(buf *bytes.Buffer, obj any) error {
	data, err := marshalClean(obj)
	if err != nil {
		return err
	}
	buf.WriteString("---\n")
	buf.Write(data) // marshalClean output ends with a newline
	return nil
}

// marshalClean serializes a manifest object to YAML, stripping zero-value noise
// that the shared v1alpha1 types cannot omit via struct tags. Specifically, a
// KafkaTopic with no producer/consumer access still serializes "access: {}"
// because Go's omitempty does not apply to non-pointer struct fields; that empty
// map is dropped here so the rendered manifests stay clean.
func marshalClean(obj any) ([]byte, error) {
	data, err := yaml.Marshal(obj)
	if err != nil {
		return nil, err
	}
	if _, ok := obj.(*v1alpha1.KafkaTopic); !ok {
		return data, nil
	}
	var doc map[string]any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	spec, ok := doc["spec"].(map[string]any)
	if !ok {
		return data, nil
	}
	if access, ok := spec["access"].(map[string]any); ok && len(access) == 0 {
		delete(spec, "access")
		return yaml.Marshal(doc)
	}
	return data, nil
}

// ImportOutput is the serialized summary of an import run.
type ImportOutput struct {
	Kind    string        `json:"kind"`
	Cluster string        `json:"cluster"`
	Summary ImportSummary `json:"summary"`
	// SchemasSkipped is true when --skip-schemas was passed, meaning no Schema
	// Registry connection was attempted and schema fields were not reconstructed.
	// Distinguishes "intentionally skipped" from "cluster has no Schema Registry".
	SchemasSkipped bool `json:"schemasSkipped,omitempty"`
	// UsersSkipped is true when --skip-users was passed, meaning SCRAM
	// credentials were not listed/reconstructed at all. Distinguishes
	// "intentionally skipped" from "cluster has no SCRAM credentials".
	UsersSkipped bool `json:"usersSkipped,omitempty"`
	// QuotasSkipped is true when --skip-quotas was passed, meaning client
	// quotas were not listed/reconstructed at all. Distinguishes
	// "intentionally skipped" from "cluster has no quotas".
	QuotasSkipped bool     `json:"quotasSkipped,omitempty"`
	Warnings      []string `json:"warnings,omitempty"`
	// RecordNameSubjects holds subjects recognized as RecordName-strategy subjects
	// (subject name == the schema's record full name). They cannot be attributed to
	// an owning topic from cluster state alone and are reported for manual
	// attribution (spec §24.1).
	RecordNameSubjects []RecordNameSubject `json:"recordNameSubjects,omitempty"`
}

// ImportSummary holds the headline counts of an import run.
type ImportSummary struct {
	Topics             int `json:"topics"`
	TopicAccessRules   int `json:"topicAccessRules"` // producer+consumer entries across topics
	AccessPolicies     int `json:"accessPolicies"`
	PolicyRules        int `json:"policyRules"`        // total rules across all policies
	Schemas            int `json:"schemas"`            // schema files written (value+key across topics)
	Quotas             int `json:"quotas"`             // reconstructed KafkaQuota manifests
	RoleBindings       int `json:"roleBindings"`       // reconstructed KafkaRoleBinding manifests
	Users              int `json:"users"`              // reconstructed KafkaUser manifests
	RecordNameSubjects int `json:"recordNameSubjects"` // RecordName-strategy subjects needing manual attribution (spec §24.1)
}

// Summarize computes the ImportOutput for a Result.
func Summarize(res Result, cluster string) ImportOutput {
	var topicAccess int
	for _, tp := range res.Topics {
		topicAccess += len(tp.Spec.Access.Producers) + len(tp.Spec.Access.Consumers)
	}
	var policyRules int
	for _, pol := range res.Policies {
		policyRules += len(pol.Spec.Rules)
	}
	return ImportOutput{
		Kind:    "ImportOutput",
		Cluster: cluster,
		Summary: ImportSummary{
			Topics:             len(res.Topics),
			TopicAccessRules:   topicAccess,
			AccessPolicies:     len(res.Policies),
			PolicyRules:        policyRules,
			Schemas:            len(res.SchemaFiles),
			Quotas:             len(res.Quotas),
			RoleBindings:       len(res.RoleBindings),
			Users:              len(res.Users),
			RecordNameSubjects: len(res.RecordNameSubjects),
		},
		Warnings:           res.Warnings,
		RecordNameSubjects: res.RecordNameSubjects,
	}
}

// RenderSummary serializes an ImportOutput in the requested format: human, yaml,
// or json (output.RenderDocument, the shared format switch). Output is
// deterministic.
func RenderSummary(out ImportOutput, format string) ([]byte, error) {
	return output.RenderDocument(format, out, func() []byte { return renderSummaryHuman(out) })
}

func renderSummaryHuman(out ImportOutput) []byte {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "Cluster: %s\n", out.Cluster)
	fmt.Fprintf(&buf, "Topics: %d\n", out.Summary.Topics)
	fmt.Fprintf(&buf, "Topic access rules: %d\n", out.Summary.TopicAccessRules)
	fmt.Fprintf(&buf, "Access policies: %d\n", out.Summary.AccessPolicies)
	fmt.Fprintf(&buf, "Policy rules: %d\n", out.Summary.PolicyRules)
	if out.SchemasSkipped {
		buf.WriteString("Schemas: skipped (--skip-schemas)\n")
	} else {
		fmt.Fprintf(&buf, "Schemas: %d\n", out.Summary.Schemas)
	}
	if out.QuotasSkipped {
		buf.WriteString("Quotas: skipped (--skip-quotas)\n")
	} else {
		fmt.Fprintf(&buf, "Quotas: %d\n", out.Summary.Quotas)
	}
	fmt.Fprintf(&buf, "Role bindings: %d\n", out.Summary.RoleBindings)
	if out.UsersSkipped {
		buf.WriteString("Users: skipped (--skip-users)\n")
	} else {
		fmt.Fprintf(&buf, "Users: %d\n", out.Summary.Users)
	}
	fmt.Fprintf(&buf, "RecordName subjects: %d\n", out.Summary.RecordNameSubjects)
	if len(out.Warnings) > 0 {
		buf.WriteString("Warnings:\n")
		for _, w := range out.Warnings {
			fmt.Fprintf(&buf, "  %s\n", w)
		}
	}
	if len(out.RecordNameSubjects) > 0 {
		fmt.Fprintf(&buf, "RecordName subjects needing manual attribution (%d):\n", len(out.RecordNameSubjects))
		for _, rns := range out.RecordNameSubjects {
			fmt.Fprintf(&buf, "  - %s  (record %s, %s)\n", rns.Subject, rns.RecordName, rns.SchemaType)
		}
	}
	return buf.Bytes()
}

// WriteOutcome reports which files WriteToDir wrote and which it skipped.
type WriteOutcome struct {
	Written []string // paths written
	Skipped []string // paths skipped
}

// WriteToDir writes each object as a single-document YAML file under dir:
// topics at <dir>/<namespace>/topics/<name>.yaml, policies at
// <dir>/<namespace>/access/<name>.yaml, and users at
// <dir>/<namespace>/users/<name>.yaml. Parent directories are created as
// needed. Objects are processed in sorted order (topics, policies, quotas,
// rolebindings, users, then schemas) so the Written/Skipped slices are
// deterministic.
//
// overwrite controls behavior when a target file already exists:
//   - "never":   skip existing files; write only new ones.
//   - "changed": skip when current content equals the new content; else write.
//   - "always":  always write.
//
// Any other value returns an error.
func WriteToDir(res Result, dir string, overwrite string) (WriteOutcome, error) {
	switch overwrite {
	case "never", "changed", "always":
	default:
		return WriteOutcome{}, fmt.Errorf("unknown overwrite mode %q (want never, changed, or always)", overwrite)
	}

	var outcome WriteOutcome
	for _, tp := range sortedTopics(res) {
		path := filepath.Join(dir, tp.Namespace, "topics", tp.Name+".yaml")
		if err := writeOne(&outcome, path, tp, overwrite); err != nil {
			return outcome, err
		}
	}
	for _, pol := range sortedPolicies(res) {
		path := filepath.Join(dir, pol.Namespace, "access", pol.Name+".yaml")
		if err := writeOne(&outcome, path, pol, overwrite); err != nil {
			return outcome, err
		}
	}
	for _, q := range sortedQuotas(res) {
		path := filepath.Join(dir, q.Namespace, "quotas", q.Name+".yaml")
		if err := writeOne(&outcome, path, q, overwrite); err != nil {
			return outcome, err
		}
	}
	for _, rb := range sortedRoleBindings(res) {
		path := filepath.Join(dir, rb.Namespace, "rolebindings", rb.Name+".yaml")
		if err := writeOne(&outcome, path, rb, overwrite); err != nil {
			return outcome, err
		}
	}
	for _, u := range sortedUsers(res) {
		path := filepath.Join(dir, u.Namespace, "users", u.Name+".yaml")
		if err := writeOne(&outcome, path, u, overwrite); err != nil {
			return outcome, err
		}
	}
	for _, sf := range sortedSchemaFiles(res) {
		path := filepath.Join(dir, sf.Namespace, "schemas", sf.BaseName+"."+sf.Ext)
		if err := writeBytes(&outcome, path, []byte(sf.Content), overwrite); err != nil {
			return outcome, err
		}
	}
	return outcome, nil
}

// sortedSchemaFiles returns the Result's schema files ordered by namespace then
// base name.
func sortedSchemaFiles(res Result) []SchemaFile {
	sf := make([]SchemaFile, len(res.SchemaFiles))
	copy(sf, res.SchemaFiles)
	sort.Slice(sf, func(i, j int) bool {
		if sf[i].Namespace != sf[j].Namespace {
			return sf[i].Namespace < sf[j].Namespace
		}
		return sf[i].BaseName < sf[j].BaseName
	})
	return sf
}

// writeOne serializes obj and writes it to path according to the overwrite mode,
// recording the path in outcome.Written or outcome.Skipped.
func writeOne(outcome *WriteOutcome, path string, obj any, overwrite string) error {
	data, err := marshalClean(obj)
	if err != nil {
		return err
	}

	switch overwrite {
	case "never":
		if exists(path) {
			outcome.Skipped = append(outcome.Skipped, path)
			return nil
		}
	case "changed":
		if cur, err := os.ReadFile(path); err == nil {
			if bytes.Equal(cur, data) {
				outcome.Skipped = append(outcome.Skipped, path)
				return nil
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	case "always":
		// fall through to write
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return err
	}
	outcome.Written = append(outcome.Written, path)
	return nil
}

// writeBytes writes raw content to path according to the overwrite mode,
// recording the path in outcome.Written or outcome.Skipped. It mirrors writeOne
// but for verbatim (non-YAML-marshaled) payloads such as schema files.
func writeBytes(outcome *WriteOutcome, path string, data []byte, overwrite string) error {
	switch overwrite {
	case "never":
		if exists(path) {
			outcome.Skipped = append(outcome.Skipped, path)
			return nil
		}
	case "changed":
		if cur, err := os.ReadFile(path); err == nil {
			if bytes.Equal(cur, data) {
				outcome.Skipped = append(outcome.Skipped, path)
				return nil
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	case "always":
		// fall through to write
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return err
	}
	outcome.Written = append(outcome.Written, path)
	return nil
}

// exists reports whether path refers to an existing file or directory.
func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
