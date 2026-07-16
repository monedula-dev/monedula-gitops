// Package mds is the client for the Confluent Metadata Service (RBAC role
// bindings). Mirrors internal/schemaregistry: an interface implemented by an
// in-memory mock (tests, internal/mds/mock) and a real REST client
// (internal/mds/confluent).
//
// Wire-type strategy: mds defines its own minimal value types (RoleBinding,
// Scope, ResourcePattern) rather than reusing internal/rbac types directly.
// This mirrors how internal/schemaregistry defines SubjectState/Schema rather
// than reusing operator API types. The executor and CLI convert
// rbac.RoleBinding → mds.RoleBinding at the dispatch seam, keeping this
// package decoupled from the engine.
package mds

import (
	"context"
	"fmt"
)

// Scope is the MDS scope for a role binding: always a Kafka cluster id, plus
// an optional sub-cluster id for non-kafka scope types (schema-registry,
// connect, ksql).
type Scope struct {
	Type         string // kafka | schema-registry | connect | ksql
	KafkaCluster string
	SubCluster   string // SR/connect/ksql cluster id; "" for kafka scope
}

// ResourcePattern is one MDS resource pattern. Nil on the parent RoleBinding
// means the binding is cluster-scoped.
type ResourcePattern struct {
	Type        string
	Name        string
	PatternType string // literal | prefixed
}

// RoleBinding is one MDS role binding (wire type). Resource is nil for
// cluster-scoped bindings.
type RoleBinding struct {
	Principal string
	Role      string
	Scope     Scope
	Resource  *ResourcePattern // nil for cluster-scoped
}

// Key returns a stable identity string over (Principal, Role, Scope, Resource).
// Mirrors rbac.RoleBinding.FullKey's field set and separator (NUL, \x00) byte
// for byte. Used by the mock's internal set and by Add/Remove idempotence.
// Layout MUST stay byte-identical to rbac.RoleBinding.FullKey —
// TestRoleBindingKeyLayoutInvariant in internal/importer pins this.
func (rb RoleBinding) Key() string {
	rk := ""
	if rb.Resource != nil {
		rk = fmt.Sprintf("%s\x00%s\x00%s", rb.Resource.Type, rb.Resource.Name, rb.Resource.PatternType)
	}
	return fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s\x00%s",
		rb.Principal,
		rb.Role,
		rb.Scope.Type,
		rb.Scope.KafkaCluster,
		rb.Scope.SubCluster,
		rk,
	)
}

// Client is the set of MDS RBAC operations used by monedula-gitops.
type Client interface {
	// ListRoleBindings returns the role bindings visible within scope (matched
	// on KafkaCluster, Type, and SubCluster).
	ListRoleBindings(ctx context.Context, scope Scope) ([]RoleBinding, error)
	// AddRoleBinding adds a role binding. Idempotent: adding an existing
	// binding (same key) is a no-op, matching MDS increment semantics.
	AddRoleBinding(ctx context.Context, rb RoleBinding) error
	// RemoveRoleBinding removes a role binding. Idempotent: removing an absent
	// binding is a no-op.
	RemoveRoleBinding(ctx context.Context, rb RoleBinding) error
}
