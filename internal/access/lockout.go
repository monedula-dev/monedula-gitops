package access

import (
	"fmt"
	"sort"
)

// LockoutWarnings implements the self-lockout guard of spec §30.3: applying
// ACLs to a resource that previously had none (with the common
// `allow.everyone.if.no.acl.found=true` broker setting) closes the resource to
// every principal NOT listed — including the connecting principal Monedula
// itself uses. For each resource the desired set touches, a warning is
// returned when connectingPrincipal is not among that resource's listed
// principals.
//
// This is a HEURISTIC, deliberately conservative in both directions:
//   - super-users (`super.users`) bypass ACLs entirely but cannot be detected
//     client-side, so the warning may be a false positive (the message says
//     so);
//   - it groups by (resourceType, resourceName, patternType) and only checks
//     principal membership — it does not model Deny precedence, host scoping,
//     prefix overlap, or ACLs that already exist live.
//
// Warnings are deduplicated and sorted for deterministic output. An empty
// connectingPrincipal (auth mechanism None / unresolvable) returns nil.
func LockoutWarnings(desiredACLs []ACL, connectingPrincipal string) []string {
	if connectingPrincipal == "" || len(desiredACLs) == 0 {
		return nil
	}

	type groupKey struct{ resourceType, resourceName, patternType string }
	principals := map[groupKey]map[string]bool{}
	for _, a := range desiredACLs {
		k := groupKey{a.ResourceType, a.ResourceName, a.PatternType}
		if principals[k] == nil {
			principals[k] = map[string]bool{}
		}
		principals[k][a.Principal] = true
	}

	seen := map[string]bool{}
	var out []string
	for k, ps := range principals {
		if ps[connectingPrincipal] {
			continue
		}
		w := fmt.Sprintf("applying ACLs to %s %q will restrict access to the listed principals; "+
			"the connecting principal %s is not among them (ignore if it is a super.user)",
			k.resourceType, k.resourceName, connectingPrincipal)
		if seen[w] {
			continue // same resource under two pattern types: one line
		}
		seen[w] = true
		out = append(out, w)
	}
	sort.Strings(out)
	return out
}
