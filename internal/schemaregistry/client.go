// Package schemaregistry defines the client interface and value types for
// interacting with a Confluent-style Schema Registry. The interface is
// implemented by both an in-memory mock (internal/schemaregistry/mock) for
// unit tests and a real HTTP client against a live registry.
package schemaregistry

import "context"

// SchemaType identifies the schema format. One of AVRO, JSON, PROTOBUF.
type SchemaType string

const (
	AVRO     SchemaType = "AVRO"
	JSON     SchemaType = "JSON"
	PROTOBUF SchemaType = "PROTOBUF"
)

// Schema is a single schema definition: its format and verbatim body.
type Schema struct {
	Type       SchemaType
	Definition string // schema body, verbatim
}

// SubjectState is the latest registered version of a subject.
type SubjectState struct {
	Subject       string
	ID            int
	Version       int
	Schema        Schema
	Compatibility string // subject-level; "" means unset (inherits global)
}

// Client is the set of Schema Registry operations used by monedula-gitops.
type Client interface {
	// ListSubjects returns all registered subject names.
	ListSubjects(ctx context.Context) ([]string, error)
	// GetSubject returns the latest version of subject, or (nil, nil) if absent.
	GetSubject(ctx context.Context, subject string) (*SubjectState, error)
	// CheckCompatibility reports whether s is compatible with subject.
	CheckCompatibility(ctx context.Context, subject string, s Schema) (bool, error)
	// RegisterSchema registers s under subject and returns the schema id.
	RegisterSchema(ctx context.Context, subject string, s Schema) (int, error)
	// LookupSchema returns the version under which this exact schema is
	// already registered for subject, or 0 (with a nil error) when it is not
	// registered at all. It backs SchemaSuperseded detection (spec §12.1): a
	// desired schema that differs from the LATEST version but exists as an
	// OLDER one would dedupe on re-registration and never converge.
	LookupSchema(ctx context.Context, subject string, s Schema) (int, error)
	// GetCompatibility returns the subject-level compatibility level, or "" if
	// unset (inherits global).
	GetCompatibility(ctx context.Context, subject string) (string, error)
	// GetGlobalCompatibility returns the registry's GLOBAL compatibility level
	// (GET /config) — the EFFECTIVE level of every subject without a
	// subject-level override. It returns "" (nil error) when the registry has
	// no global config; callers treat "" or an error as "unknown" and fall
	// back to legacy classification rather than failing the run (spec §17.1
	// first-set risk classification).
	GetGlobalCompatibility(ctx context.Context) (string, error)
	// SetCompatibility sets the subject-level compatibility level.
	SetCompatibility(ctx context.Context, subject, level string) error
	// DeleteSubject removes subject and all its versions.
	DeleteSubject(ctx context.Context, subject string) error
}
