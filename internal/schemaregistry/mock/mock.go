// Package mock provides an in-memory schemaregistry.Client seeded from a YAML
// state fixture, so the schema diff and apply executor can be unit-tested
// without a live Schema Registry. Reads are deterministically ordered to keep
// tests stable.
package mock

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	"sigs.k8s.io/yaml"

	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
)

var _ schemaregistry.Client = (*Client)(nil)

// subjectEntry is the stored state for one subject. versions and ids grow
// together (one id per version); the last element is the latest version.
type subjectEntry struct {
	versions      []schemaregistry.Schema
	ids           []int
	compatibility string
}

// Client is an in-memory implementation of schemaregistry.Client.
//
// Every mutating call (RegisterSchema, SetCompatibility, DeleteSubject) is
// recorded (see Calls) so the apply executor can be unit-tested without a live
// registry. Failures can be injected per call via FailOn; an injected failure
// is recorded but does NOT mutate state. CheckCompatibility returns a
// configurable result (default true) set via SetCheckResult.
type Client struct {
	subjects map[string]*subjectEntry

	// globalCompatibility is the registry-wide default level (GET /config).
	// "" means "unknown/unset" — live-state readers then fall back to legacy
	// risk classification, so existing fixtures keep their behavior.
	globalCompatibility string

	// nextID is the global, monotonically increasing schema id sequence.
	nextID int

	// checkResult is what CheckCompatibility returns; defaults to true.
	checkResult bool

	// calls records each mutating call in invocation order, e.g.
	// "RegisterSchema payments.orders-value", "SetCompatibility subj FULL".
	calls []string

	// failures maps a call signature ("<method> <target>") to the error that
	// should be returned for that call. Populated via FailOn.
	failures map[string]error
}

// fileState mirrors the on-disk YAML fixture. JSON tags drive the lowercase
// field mapping via sigs.k8s.io/yaml (YAML -> JSON -> struct).
type fileState struct {
	Subjects []fileSubject `json:"subjects"`
	// GlobalCompatibility seeds the registry-wide default level (GET /config).
	// Omitted -> "" (unknown), which live-state readers treat as "fall back to
	// legacy classification".
	GlobalCompatibility string `json:"globalCompatibility"`
}

type fileSubject struct {
	Subject       string `json:"subject"`
	Type          string `json:"type"`
	Definition    string `json:"definition"`
	Compatibility string `json:"compatibility"`
	// Versions, when non-empty, seeds the subject with multiple versions
	// (definition bodies, oldest first; the last is the latest version) and
	// takes precedence over Definition. All versions share Type.
	Versions []string `json:"versions"`
}

// New constructs an empty Client.
func New() *Client {
	return &Client{
		subjects:    make(map[string]*subjectEntry),
		checkResult: true,
	}
}

// FromFile loads registry state from a YAML fixture at path. Each seeded
// subject gets version 1 and an incrementing schema id.
func FromFile(path string) (*Client, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read state file %q: %w", path, err)
	}
	var fs fileState
	if err := yaml.Unmarshal(data, &fs); err != nil {
		return nil, fmt.Errorf("parse state file %q: %w", path, err)
	}

	c := New()
	c.globalCompatibility = fs.GlobalCompatibility
	for _, s := range fs.Subjects {
		defs := s.Versions
		if len(defs) == 0 {
			defs = []string{s.Definition}
		}
		e := &subjectEntry{compatibility: s.Compatibility}
		for _, def := range defs {
			c.nextID++
			e.versions = append(e.versions, schemaregistry.Schema{
				Type:       schemaregistry.SchemaType(s.Type),
				Definition: def,
			})
			e.ids = append(e.ids, c.nextID)
		}
		c.subjects[s.Subject] = e
	}
	return c, nil
}

// SetCheckResult configures the boolean returned by CheckCompatibility.
func (c *Client) SetCheckResult(ok bool) {
	c.checkResult = ok
}

// SetGlobalCompatibility seeds the registry-wide default level returned by
// GetGlobalCompatibility. Tests use it to exercise first-set risk
// classification against a known global baseline; "" (the default) means
// unknown.
func (c *Client) SetGlobalCompatibility(level string) {
	c.globalCompatibility = level
}

// Seed pre-populates subject with versions (oldest first; the last is the
// latest) and a subject-level compatibility, WITHOUT recording any calls.
// Tests use it to simulate state a producer's pipeline registered out-of-band
// (spec §12.2 governance mode), so a subsequent reconcile that touches only
// compatibility can assert ZERO RegisterSchema calls. Passing no versions seeds
// only the compatibility, mirroring config-before-any-version. IDs are drawn
// from the shared monotonic sequence, so the first seeded version is id 1 only
// on a fresh client.
func (c *Client) Seed(subject, typ, compatibility string, versions ...string) {
	e := &subjectEntry{compatibility: compatibility}
	for _, def := range versions {
		c.nextID++
		e.versions = append(e.versions, schemaregistry.Schema{
			Type:       schemaregistry.SchemaType(typ),
			Definition: def,
		})
		e.ids = append(e.ids, c.nextID)
	}
	c.subjects[subject] = e
}

// Calls returns the recorded mutating calls in invocation order. Each entry is
// a stable string of the form "<Method> <target...>". Reads are not recorded.
// A call is recorded even if it fails (including via FailOn).
func (c *Client) Calls() []string {
	out := make([]string, len(c.calls))
	copy(out, c.calls)
	return out
}

// FailOn configures the mock to return err for the next and subsequent calls to
// method against subject. The failing call is still recorded, but state is NOT
// mutated.
func (c *Client) FailOn(method, subject string, err error) {
	if c.failures == nil {
		c.failures = make(map[string]error)
	}
	c.failures[method+" "+subject] = err
}

// record appends sig to the call log and returns any configured failure keyed
// by "<method> <subject>".
func (c *Client) record(sig, method, subject string) error {
	c.calls = append(c.calls, sig)
	return c.failures[method+" "+subject]
}

// ListSubjects returns all registered subject names, sorted.
func (c *Client) ListSubjects(_ context.Context) ([]string, error) {
	out := make([]string, 0, len(c.subjects))
	for name := range c.subjects {
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

// GetSubject returns the latest version of subject, or (nil, nil) if absent.
func (c *Client) GetSubject(_ context.Context, subject string) (*schemaregistry.SubjectState, error) {
	e := c.subjects[subject]
	if e == nil || len(e.versions) == 0 {
		return nil, nil
	}
	last := len(e.versions) - 1
	return &schemaregistry.SubjectState{
		Subject:       subject,
		ID:            e.ids[last],
		Version:       last + 1,
		Schema:        e.versions[last],
		Compatibility: e.compatibility,
	}, nil
}

// LookupSchema returns the version under which s is registered for subject,
// or 0 (nil error) when it is not registered. The real registry compares
// canonically; the mock approximates with an exact (or whitespace-trimmed)
// type+definition match, which is sufficient for fixtures. Reads are not
// recorded.
func (c *Client) LookupSchema(_ context.Context, subject string, s schemaregistry.Schema) (int, error) {
	e := c.subjects[subject]
	if e == nil {
		return 0, nil
	}
	want := strings.TrimSpace(s.Definition)
	for i, v := range e.versions {
		if v.Type == s.Type && strings.TrimSpace(v.Definition) == want {
			return i + 1, nil
		}
	}
	return 0, nil
}

// CheckCompatibility returns the configured result (default true).
func (c *Client) CheckCompatibility(_ context.Context, _ string, _ schemaregistry.Schema) (bool, error) {
	return c.checkResult, nil
}

// RegisterSchema registers s under subject and returns the schema id.
//
// If subject is absent, it creates version 1 with a new id. If subject exists
// and s matches the latest version (same type and definition), the existing id
// is returned without adding a version (idempotent). Otherwise a new version
// with a new id is appended.
func (c *Client) RegisterSchema(_ context.Context, subject string, s schemaregistry.Schema) (int, error) {
	if err := c.record("RegisterSchema "+subject, "RegisterSchema", subject); err != nil {
		return 0, err
	}

	e := c.subjects[subject]
	if e == nil {
		c.nextID++
		c.subjects[subject] = &subjectEntry{
			versions: []schemaregistry.Schema{s},
			ids:      []int{c.nextID},
		}
		return c.nextID, nil
	}

	if len(e.versions) > 0 {
		last := len(e.versions) - 1
		if e.versions[last] == s {
			return e.ids[last], nil // idempotent: no new version
		}
	}

	c.nextID++
	e.versions = append(e.versions, s)
	e.ids = append(e.ids, c.nextID)
	return c.nextID, nil
}

// GetCompatibility returns the stored subject-level compatibility, or "" if
// unset or absent.
func (c *Client) GetCompatibility(_ context.Context, subject string) (string, error) {
	e := c.subjects[subject]
	if e == nil {
		return "", nil
	}
	return e.compatibility, nil
}

// GetGlobalCompatibility returns the seeded registry-wide default level, or
// "" when none was seeded (unknown — legacy classification). Reads are not
// recorded.
func (c *Client) GetGlobalCompatibility(_ context.Context) (string, error) {
	return c.globalCompatibility, nil
}

// SetCompatibility sets the subject-level compatibility level. The level is
// recorded even for a not-yet-existing subject (mirroring real Schema Registry
// behavior), so GetCompatibility returns it. Setting config on an absent
// subject does NOT make it appear in ListSubjects/GetSubject.
func (c *Client) SetCompatibility(_ context.Context, subject, level string) error {
	if err := c.record("SetCompatibility "+subject+" "+level, "SetCompatibility", subject); err != nil {
		return err
	}
	e := c.subjects[subject]
	if e == nil {
		e = &subjectEntry{}
		c.subjects[subject] = e
	}
	e.compatibility = level
	return nil
}

// DeleteSubject removes subject and all its versions. Deleting an absent
// subject is a no-op (idempotent), mirroring the kafka mock's DeleteTopic.
func (c *Client) DeleteSubject(_ context.Context, subject string) error {
	if err := c.record("DeleteSubject "+subject, "DeleteSubject", subject); err != nil {
		return err
	}
	delete(c.subjects, subject)
	return nil
}
