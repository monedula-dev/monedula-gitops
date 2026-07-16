package access

import (
	"fmt"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/managedset"
)

func hostOrStar(h string) string {
	if h == "" {
		return "*"
	}
	return h
}

func orDefault(ops, def []string) []string {
	if len(ops) > 0 {
		return ops
	}
	return def
}

// CompileTopic compiles topic-local access into ACLs (all Allow, literal patterns).
// The host for each ACL is taken from the per-entry Host field; an empty Host defaults
// to "*" (all hosts), making it fully backwards-compatible. See spec §8.4.
func CompileTopic(tp *v1alpha1.KafkaTopic) []ACL {
	topicName := tp.ResolvedTopicName()
	var out []ACL
	for _, p := range tp.Spec.Access.Producers {
		for _, op := range orDefault(p.Operations, []string{"Write", "Describe"}) {
			out = append(out, ACL{Principal: p.Principal, Host: hostOrStar(p.Host), ResourceType: "topic", ResourceName: topicName, PatternType: "literal", Operation: op, Permission: "Allow"})
		}
	}
	for _, c := range tp.Spec.Access.Consumers {
		for _, op := range orDefault(c.TopicOperations, []string{"Read", "Describe"}) {
			out = append(out, ACL{Principal: c.Principal, Host: hostOrStar(c.Host), ResourceType: "topic", ResourceName: topicName, PatternType: "literal", Operation: op, Permission: "Allow"})
		}
		for _, op := range orDefault(c.GroupOperations, []string{"Read"}) {
			out = append(out, ACL{Principal: c.Principal, Host: hostOrStar(c.Host), ResourceType: "group", ResourceName: c.Group, PatternType: "literal", Operation: op, Permission: "Allow"})
		}
	}
	return out
}

// CompilePolicy compiles a KafkaAccessPolicy's rules into ACLs, applying rule-level defaults
// (permission Allow, host "*", patternType literal) so compilation is self-contained.
func CompilePolicy(pol *v1alpha1.KafkaAccessPolicy) []ACL {
	var out []ACL
	for _, r := range pol.Spec.Rules {
		host := r.Host
		if host == "" {
			host = "*"
		}
		perm := r.Permission
		if perm == "" {
			perm = "Allow"
		}
		pt := r.Resource.PatternType
		if pt == "" {
			pt = "literal"
		}
		for _, op := range r.Operations {
			out = append(out, ACL{Principal: r.Principal, Host: host, ResourceType: r.Resource.Type, ResourceName: r.Resource.Name, PatternType: pt, Operation: op, Permission: perm})
		}
	}
	return out
}

// BuildDesiredSet dedupes identical tuples and reports Allow/Deny conflicts on the same subject.
//
// Ordering contract: when two tuples share a subject but disagree on permission
// (one Allow, one Deny), the FIRST-seen permission is retained and every subsequent
// conflicting tuple is dropped from the output (and reported as an error). Because the
// surviving permission therefore depends on input order, callers MUST supply ACLs in a
// deterministic order to get reproducible results.
//
// Attribution on dedupe: when the same tuple is desired by multiple resources,
// the MOST ENFORCING reconciliation mode wins (Enforce > DetectOnly >
// ObserveOnly — see managedset.ModeRank), so a tuple is only ever report-only
// if EVERY contributor is non-Enforce. Prune consent merges the OPPOSITE way
// (spec §10.3): the survivor keeps Prune=true only if EVERY contributor opted
// in (AND-merge — one non-consenting owner vetoes deletion), so a scope built
// from the deduped set still honors the veto. Owner attribution (Source*)
// stays with the FIRST contributor, which is deterministic given ordered
// input.
//
// The walk is managedset.BuildDesiredSet; the ACL-specific part is the
// Allow/Deny subject conflict, which spans two DIFFERENT full keys (the
// permissions differ) and therefore enters as the stateful reject hook rather
// than the same-identity conflict hook rbac uses.
func BuildDesiredSet(acls []ACL) ([]ACL, []error) {
	bySubject := map[string]string{} // subjectKey -> permission
	rejectConflicting := func(a ACL) error {
		if prev, ok := bySubject[a.subjectKey()]; ok && prev != a.Permission {
			return fmt.Errorf("ACL conflict: %s requested as both Allow and Deny", a.subjectKey())
		}
		bySubject[a.subjectKey()] = a.Permission
		return nil
	}
	return managedset.BuildDesiredSet(acls, ACL.fullKey, aclAttribution, rejectConflicting, nil)
}
