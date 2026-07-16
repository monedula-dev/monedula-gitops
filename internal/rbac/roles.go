package rbac

// RoleKind classifies a Confluent RBAC role by whether it binds to specific
// resources or to a whole cluster scope.
type RoleKind int

const (
	RoleUnknown        RoleKind = iota
	RoleClusterScoped           // binds to a scope only; resources forbidden (SystemAdmin, Operator, …)
	RoleResourceScoped          // binds to resource patterns; resources required (DeveloperRead, …)
)

// knownRoles is the package-level map of Confluent canonical role names to their kind.
var knownRoles = map[string]RoleKind{
	// Cluster-scoped roles: bind to a scope only; no resource patterns.
	"SystemAdmin":   RoleClusterScoped,
	"ClusterAdmin":  RoleClusterScoped,
	"UserAdmin":     RoleClusterScoped,
	"Operator":      RoleClusterScoped,
	"SecurityAdmin": RoleClusterScoped,
	"AuditAdmin":    RoleClusterScoped,

	// Resource-scoped roles: require at least one resource pattern.
	"ResourceOwner":   RoleResourceScoped,
	"DeveloperRead":   RoleResourceScoped,
	"DeveloperWrite":  RoleResourceScoped,
	"DeveloperManage": RoleResourceScoped,
}

// ClassifyRole returns the kind of a known Confluent role and whether it is
// known. Unknown roles return (RoleUnknown, false) — callers accept them with a
// warning and skip resource-presence enforcement (spec §40).
func ClassifyRole(role string) (RoleKind, bool) {
	k, ok := knownRoles[role]
	if !ok {
		return RoleUnknown, false
	}
	return k, true
}
