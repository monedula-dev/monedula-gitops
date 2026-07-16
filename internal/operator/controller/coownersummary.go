package controller

import (
	"fmt"
	"sort"
	"strings"

	"github.com/monedula-dev/monedula-gitops/internal/access"
	"github.com/monedula-dev/monedula-gitops/internal/rbac"
)

// coOwnerNamesLimit is how many distinct co-owners the SharedACLsRetained /
// SharedRoleBindingsRetained events name explicitly before collapsing the rest
// into an "and N more" overflow.
const coOwnerNamesLimit = 3

// aclCoOwnerSummary formats the distinct co-owners of retained (Kind/namespace/
// name, deduped + sorted) into a human-readable summary for the
// SharedACLsRetained event message, e.g. "KafkaTopic/ns/a, KafkaAccessPolicy/
// ns/b, and 2 more". retained must be the ACTUALLY RETAINED tuples (see
// retainedACLs) — not the whole protect set, which may name owners unrelated
// to this deletion. Returns "" when retained is empty.
func aclCoOwnerSummary(retained []access.ACL, limit int) string {
	names := make([]string, 0, len(retained))
	for _, a := range retained {
		names = append(names, coOwnerName(a.SourceKind, a.SourceNamespace, a.SourceName))
	}
	return coOwnerSummary(names, limit)
}

// roleBindingCoOwnerSummary is the role-binding analogue of aclCoOwnerSummary
// for the SharedRoleBindingsRetained event message. retained must be the
// ACTUALLY RETAINED bindings (see retainedRoleBindings).
func roleBindingCoOwnerSummary(retained []rbac.RoleBinding, limit int) string {
	names := make([]string, 0, len(retained))
	for _, b := range retained {
		names = append(names, coOwnerName(b.SourceKind, b.SourceNamespace, b.SourceName))
	}
	return coOwnerSummary(names, limit)
}

// coOwnerName formats one owner's attribution as "Kind/namespace/name".
func coOwnerName(kind, namespace, name string) string {
	return kind + "/" + namespace + "/" + name
}

// coOwnerSummary dedupes and sorts names, then joins up to limit of them,
// appending "and N more" for any overflow. Returns "" for an empty input.
func coOwnerSummary(names []string, limit int) string {
	if len(names) == 0 {
		return ""
	}
	seen := make(map[string]struct{}, len(names))
	uniq := make([]string, 0, len(names))
	for _, n := range names {
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		uniq = append(uniq, n)
	}
	sort.Strings(uniq)

	if len(uniq) <= limit {
		return strings.Join(uniq, ", ")
	}
	overflow := len(uniq) - limit
	return fmt.Sprintf("%s, and %d more", strings.Join(uniq[:limit], ", "), overflow)
}
