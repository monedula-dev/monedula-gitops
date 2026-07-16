// Package recordname is the single source of truth for Schema Registry subject
// names. It derives the record full name from a schema body (Extract) and maps
// a (subjectStrategy, topic, schema) tuple to the concrete value/key subjects
// (Subjects). Both the CLI pipeline and the operator reconciler call Subjects so
// the two never drift on naming.
//
// Spec §11 subject strategies:
//   - TopicName       -> <topic>-value / <topic>-key
//   - RecordName      -> <recordFullName> (per value/key schema body)
//   - TopicRecordName -> <topic>-<recordFullName> (per value/key schema body)
//   - Custom          -> spec.schema.valueSubject / spec.schema.keySubject verbatim
//
// The importer (internal/importer) deliberately reconstructs ONLY TopicName-
// strategy subjects; it uses this package solely to VERIFY reconstruction
// (recomputing a generated manifest's apply-time subjects and comparing them
// to the attributed live subjects) — the other strategies cannot be reversed
// from a live subject name back to a topic.
package recordname

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

// protoMessageRe matches a top-level `message Name` declaration, allowing
// leading whitespace and an optional `{` on the same line. The first match in a
// definition wins (see Extract's PROTOBUF case).
var protoMessageRe = regexp.MustCompile(`^\s*message\s+([A-Za-z_][A-Za-z0-9_]*)\b`)

// protoPackageRe matches a `package x.y.z;` declaration (leading whitespace ok).
var protoPackageRe = regexp.MustCompile(`^\s*package\s+([A-Za-z_][A-Za-z0-9_.]*)\s*;`)

// Extract returns the record full name carried by a schema definition, used by
// the RecordName and TopicRecordName subject strategies (spec §11).
//
//   - AVRO: parse the JSON object. The full name is namespace + "." + name when
//     a namespace is set and name is not already dotted; otherwise name verbatim
//     (a dotted name already carries its namespace, and a name without a
//     namespace has none to prepend). Errors on invalid JSON or a missing name.
//   - JSON: the top-level "title" string (JSON Schema's conventional name).
//     Errors when title is absent, empty, or not a string.
//   - PROTOBUF: a deliberately simple line scanner (no proto dependency): an
//     optional `package x.y;` plus the FIRST top-level `message Name`, joined as
//     `x.y.Name` (or `Name` without a package). It does not parse nested
//     messages, options, or comments — only the leading declarations a generated
//     `.proto` schema body carries. Errors when no message is found.
func Extract(format, definition string) (string, error) {
	switch format {
	case "AVRO":
		return extractAvro(definition)
	case "JSON":
		return extractJSON(definition)
	case "PROTOBUF":
		return extractProto(definition)
	default:
		return "", fmt.Errorf("cannot extract record name: unsupported schema format %q", format)
	}
}

func extractAvro(definition string) (string, error) {
	var doc struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	}
	if err := json.Unmarshal([]byte(definition), &doc); err != nil {
		return "", fmt.Errorf("invalid AVRO schema JSON: %w", err)
	}
	if doc.Name == "" {
		return "", fmt.Errorf("AVRO schema has no top-level \"name\"")
	}
	// A dotted name already includes its namespace; a name without a namespace
	// has none to prepend.
	if doc.Namespace == "" || strings.Contains(doc.Name, ".") {
		return doc.Name, nil
	}
	return doc.Namespace + "." + doc.Name, nil
}

func extractJSON(definition string) (string, error) {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal([]byte(definition), &doc); err != nil {
		return "", fmt.Errorf("invalid JSON schema: %w", err)
	}
	raw, ok := doc["title"]
	if !ok {
		return "", fmt.Errorf("JSON schema has no top-level \"title\"")
	}
	var title string
	if err := json.Unmarshal(raw, &title); err != nil {
		return "", fmt.Errorf("JSON schema \"title\" must be a string")
	}
	if title == "" {
		return "", fmt.Errorf("JSON schema \"title\" must be non-empty")
	}
	return title, nil
}

func extractProto(definition string) (string, error) {
	var pkg, message string
	for _, line := range strings.Split(definition, "\n") {
		if pkg == "" {
			if m := protoPackageRe.FindStringSubmatch(line); m != nil {
				pkg = m[1]
				continue
			}
		}
		if m := protoMessageRe.FindStringSubmatch(line); m != nil {
			message = m[1]
			break // first top-level message wins
		}
	}
	if message == "" {
		return "", fmt.Errorf("PROTOBUF schema has no top-level message declaration")
	}
	if pkg == "" {
		return message, nil
	}
	return pkg + "." + message, nil
}

// Subjects maps a subject strategy (spec §11) to the concrete value and key
// subject names. It is the single computation site shared by the CLI pipeline
// and the operator reconciler.
//
// valueDef/keyDef are the resolved schema bodies (empty when that schema is not
// present — e.g. governance mode, or a value-only topic). For RecordName and
// TopicRecordName the body is required to extract the record name, so those
// strategies only produce a subject for a non-empty def; an empty strategy is
// treated as TopicName (defaulting does not stamp a strategy).
//
// keySubject is "" when there is no key schema and, for Custom, no keySubject is
// configured.
func Subjects(strategy, topicName string, schema *v1alpha1.TopicSchema, valueDef, keyDef string) (valueSubject, keySubject string, err error) {
	switch strategy {
	case "", "TopicName":
		// Value subject is always named, even in governance mode (no body): the
		// subject's compatibility is still managed under <topic>-value. Key
		// subject only when a key body is present.
		valueSubject = topicName + "-value"
		if keyDef != "" {
			keySubject = topicName + "-key"
		}
		return valueSubject, keySubject, nil

	case "RecordName", "TopicRecordName":
		if valueDef != "" {
			rn, e := Extract(schema.Format, valueDef)
			if e != nil {
				return "", "", fmt.Errorf("value schema: %w", e)
			}
			valueSubject = recordSubject(strategy, topicName, rn)
		}
		if keyDef != "" {
			rn, e := Extract(schema.Format, keyDef)
			if e != nil {
				return "", "", fmt.Errorf("key schema: %w", e)
			}
			keySubject = recordSubject(strategy, topicName, rn)
		}
		if valueSubject != "" && keySubject != "" && valueSubject == keySubject {
			return "", "", fmt.Errorf("value and key schemas resolve to the same subject %q (strategy %s): rename one record to avoid clobbering the subject", valueSubject, strategy)
		}
		return valueSubject, keySubject, nil

	case "Custom":
		// Subjects are named verbatim; this is what makes governance of an
		// arbitrary subject possible (no body needed). keySubject stays "" when
		// unset.
		vs, ks := schema.ValueSubject, schema.KeySubject
		if vs != "" && ks != "" && vs == ks {
			return "", "", fmt.Errorf("value and key schemas resolve to the same subject %q (strategy %s): valueSubject and keySubject must differ", vs, strategy)
		}
		return vs, ks, nil

	default:
		return "", "", fmt.Errorf("unsupported subjectStrategy %q", strategy)
	}
}

// recordSubject applies the record-name-based strategies to a full record name.
func recordSubject(strategy, topicName, recordFullName string) string {
	if strategy == "TopicRecordName" {
		return topicName + "-" + recordFullName
	}
	return recordFullName
}
