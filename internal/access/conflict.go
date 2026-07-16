package access

// Conflict is a cross-resource Allow/Deny disagreement on one ACL subject
// (the identity tuple minus permission). A and B are two ACLs with opposite
// permissions on that subject, carrying Source* attribution so callers can name
// both parties.
type Conflict struct {
	Subject string // human-readable subjectKey of the contested tuple
	A, B    ACL    // opposite-permission ACLs (A first-seen, B the conflicting one)
}

// Conflicts returns every Allow/Deny disagreement in acls, keyed by subject.
// The first ACL seen for a subject is the baseline; each later ACL with the
// opposite permission yields one Conflict{baseline, later}. Identical-permission
// duplicates are not conflicts. Deterministic for a deterministically-ordered
// input (callers sort upstream).
func Conflicts(acls []ACL) []Conflict {
	type seen struct {
		perm string
		acl  ACL
	}
	first := map[string]seen{}
	var out []Conflict
	for _, a := range acls {
		k := a.subjectKey()
		s, ok := first[k]
		if !ok {
			first[k] = seen{perm: a.Permission, acl: a}
			continue
		}
		if s.perm != a.Permission {
			out = append(out, Conflict{Subject: k, A: s.acl, B: a})
		}
	}
	return out
}
