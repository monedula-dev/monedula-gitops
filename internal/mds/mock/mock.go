// Package mock provides an in-memory mds.Client seeded from a YAML state
// fixture, so the RBAC executor can be unit-tested without a live MDS endpoint.
// Reads are deterministically ordered to keep tests stable. Mirrors the shape
// of internal/schemaregistry/mock and internal/kafka/mock.
package mock

import (
	"context"
	"fmt"
	"os"
	"sort"

	"sigs.k8s.io/yaml"

	"github.com/monedula-dev/monedula-gitops/internal/mds"
)

var _ mds.Client = (*Mock)(nil)

// Mock is an in-memory implementation of mds.Client.
//
// Every mutating call (AddRoleBinding, RemoveRoleBinding) is recorded (see
// Calls) so the RBAC executor can be unit-tested without a live MDS endpoint.
// Failures can be injected per call via FailOn; an injected failure is recorded
// but does NOT mutate state.
type Mock struct {
	// bindings is the internal set, keyed by the role binding's stable key().
	bindings map[string]mds.RoleBinding

	// calls records each mutating call in invocation order, e.g.
	// "AddRoleBinding User:alice|DeveloperRead|kafka|lkc-abc||",
	// "RemoveRoleBinding User:alice|DeveloperRead|kafka|lkc-abc||".
	calls []string

	// failures maps a call signature ("<Method> <key>") to the error that
	// should be returned for that call. Populated via FailOn.
	failures map[string]error
}

// fileState mirrors the on-disk YAML fixture. JSON tags drive the lowercase
// field mapping via sigs.k8s.io/yaml (YAML -> JSON -> struct).
type fileState struct {
	RoleBindings []fileRoleBinding `json:"roleBindings"`
}

type fileRoleBinding struct {
	Principal    string               `json:"principal"`
	Role         string               `json:"role"`
	ScopeType    string               `json:"scopeType"`
	KafkaCluster string               `json:"kafkaCluster"`
	SubCluster   string               `json:"subCluster,omitempty"`
	Resource     *fileResourcePattern `json:"resource,omitempty"`
}

type fileResourcePattern struct {
	Type        string `json:"type"`
	Name        string `json:"name"`
	PatternType string `json:"patternType"`
}

// New constructs a Mock pre-seeded with the given role bindings. Passing no
// arguments constructs an empty mock. Seeded bindings are not recorded as calls —
// only subsequent AddRoleBinding/RemoveRoleBinding calls are.
func New(initial ...mds.RoleBinding) *Mock {
	m := &Mock{
		bindings: make(map[string]mds.RoleBinding),
	}
	for _, rb := range initial {
		m.bindings[rb.Key()] = rb
	}
	return m
}

// FromFile loads MDS role-binding state from a YAML fixture at path. The file
// must contain a top-level "roleBindings" list. Mirrors
// internal/schemaregistry/mock.FromFile and internal/kafka/mock.FromFile.
//
// Example fixture:
//
//	roleBindings:
//	  - principal: User:alice
//	    role: DeveloperRead
//	    scopeType: kafka
//	    kafkaCluster: lkc-abc123
//	    resource:
//	      type: Topic
//	      name: payments.orders
//	      patternType: literal
func FromFile(path string) (*Mock, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read state file %q: %w", path, err)
	}
	var fs fileState
	if err := yaml.Unmarshal(data, &fs); err != nil {
		return nil, fmt.Errorf("parse state file %q: %w", path, err)
	}

	m := New()
	for _, frb := range fs.RoleBindings {
		rb := mds.RoleBinding{
			Principal: frb.Principal,
			Role:      frb.Role,
			Scope: mds.Scope{
				Type:         frb.ScopeType,
				KafkaCluster: frb.KafkaCluster,
				SubCluster:   frb.SubCluster,
			},
		}
		if frb.Resource != nil {
			rb.Resource = &mds.ResourcePattern{
				Type:        frb.Resource.Type,
				Name:        frb.Resource.Name,
				PatternType: frb.Resource.PatternType,
			}
		}
		m.bindings[rb.Key()] = rb
	}
	return m, nil
}

// Calls returns the recorded mutating calls in invocation order. Each entry is
// a stable string of the form "<Method> <key>". Reads are not recorded. A call
// is recorded even if it fails (including via FailOn).
func (m *Mock) Calls() []string {
	out := make([]string, len(m.calls))
	copy(out, m.calls)
	return out
}

// FailOn configures the mock to return err for the next and subsequent calls to
// method against a role binding whose key matches key. The failing call is
// still recorded, but state is NOT mutated.
func (m *Mock) FailOn(method, key string, err error) {
	if m.failures == nil {
		m.failures = make(map[string]error)
	}
	m.failures[method+" "+key] = err
}

// record appends sig to the call log and returns any configured failure for it.
func (m *Mock) record(method, key string) error {
	sig := method + " " + key
	m.calls = append(m.calls, sig)
	return m.failures[sig]
}

// ListRoleBindings returns all stored role bindings that match the given scope
// (same KafkaCluster, Type, and SubCluster), sorted deterministically by key.
// Reads are not recorded.
func (m *Mock) ListRoleBindings(_ context.Context, scope mds.Scope) ([]mds.RoleBinding, error) {
	var out []mds.RoleBinding
	for _, rb := range m.bindings {
		if rb.Scope.KafkaCluster == scope.KafkaCluster &&
			rb.Scope.Type == scope.Type &&
			rb.Scope.SubCluster == scope.SubCluster {
			rb := rb // value copy (Principal/Role/Scope are value fields)
			if rb.Resource != nil {
				r := *rb.Resource
				rb.Resource = &r
			}
			out = append(out, rb)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Key() < out[j].Key()
	})
	return out, nil
}

// AddRoleBinding inserts a role binding. Idempotent: adding a binding whose
// key already exists is a no-op (matching MDS increment semantics). The call
// is always recorded.
func (m *Mock) AddRoleBinding(_ context.Context, rb mds.RoleBinding) error {
	key := rb.Key()
	if err := m.record("AddRoleBinding", key); err != nil {
		return err
	}
	// Idempotent: if key already present, do not overwrite.
	if _, exists := m.bindings[key]; !exists {
		m.bindings[key] = rb
	}
	return nil
}

// RemoveRoleBinding deletes a role binding. Idempotent: removing an absent
// binding is a no-op (mirroring kafka/mock.DeleteTopic and SR
// mock.DeleteSubject). The call is always recorded.
func (m *Mock) RemoveRoleBinding(_ context.Context, rb mds.RoleBinding) error {
	key := rb.Key()
	if err := m.record("RemoveRoleBinding", key); err != nil {
		return err
	}
	delete(m.bindings, key)
	return nil
}
