package controller

// Unit tests for the co-owner naming helpers used by the SharedACLsRetained /
// SharedRoleBindingsRetained events: the retained set must be computed from
// the ACTUALLY RETAINED tuples (the intersection of the deleting CR's own set
// with the surviving co-owners' desired union), named as Kind/namespace/name,
// deduped + sorted, with an "and N more" overflow past the limit.

import (
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/access"
	"github.com/monedula-dev/monedula-gitops/internal/rbac"
)

func TestRetainedACLs(t *testing.T) {
	shared := access.ACL{Principal: "User:svc", Host: "*", ResourceType: "topic", ResourceName: "t1", PatternType: "literal", Operation: "Read", Permission: "Allow"}
	own := access.ACL{Principal: "User:svc", Host: "*", ResourceType: "group", ResourceName: "g1", PatternType: "literal", Operation: "Read", Permission: "Allow"}

	protecting := shared
	protecting.SourceKind = "KafkaAccessPolicy"
	protecting.SourceName = "other-owner"

	got := retainedACLs([]access.ACL{shared, own}, []access.ACL{protecting})
	if len(got) != 1 || got[0].FullKey() != shared.FullKey() {
		t.Fatalf("retainedACLs = %+v, want only the shared tuple %q", got, shared.FullKey())
	}
	// Must carry the PROTECT-side (co-owner's) attribution, not toDelete's
	// (which is unattributed here) — this is what makes co-owner naming correct.
	if got[0].SourceKind != "KafkaAccessPolicy" || got[0].SourceName != "other-owner" {
		t.Fatalf("retainedACLs attribution = %+v, want the protect-side owner KafkaAccessPolicy/other-owner", got[0])
	}

	if got := retainedACLs([]access.ACL{shared, own}, nil); len(got) != 0 {
		t.Fatalf("empty protect set: nothing retained, got %+v", got)
	}
	if got := retainedACLs(nil, []access.ACL{protecting}); len(got) != 0 {
		t.Fatalf("empty to-delete set: nothing retained, got %+v", got)
	}
}

func TestRetainedRoleBindings(t *testing.T) {
	scope := rbac.Scope{Type: "kafka", KafkaCluster: "lkc-1"}
	shared := rbac.RoleBinding{Principal: "User:svc", Role: "DeveloperWrite", Scope: scope,
		Resource: &rbac.ResourcePattern{Type: "Topic", Name: "t1", PatternType: "literal"}}
	own := rbac.RoleBinding{Principal: "User:svc", Role: "DeveloperWrite", Scope: scope,
		Resource: &rbac.ResourcePattern{Type: "Topic", Name: "t2", PatternType: "literal"}}

	protecting := shared
	protecting.SourceKind = "KafkaTopic"
	protecting.SourceName = "other-owner"

	got := retainedRoleBindings([]rbac.RoleBinding{shared, own}, []rbac.RoleBinding{protecting})
	if len(got) != 1 || got[0].FullKey() != shared.FullKey() {
		t.Fatalf("retainedRoleBindings = %+v, want only the shared binding %q", got, shared.FullKey())
	}
	if got[0].SourceKind != "KafkaTopic" || got[0].SourceName != "other-owner" {
		t.Fatalf("retainedRoleBindings attribution = %+v, want the protect-side owner KafkaTopic/other-owner", got[0])
	}
}

func aclOwner(kind, ns, name string) access.ACL {
	return access.ACL{
		Principal: "User:x", Host: "*", ResourceType: "topic", ResourceName: "t", PatternType: "literal",
		Operation: "Read", Permission: "Allow",
		SourceKind: kind, SourceNamespace: ns, SourceName: name,
	}
}

func TestACLCoOwnerSummary_DedupSortAndLimit(t *testing.T) {
	acls := []access.ACL{
		aclOwner("KafkaAccessPolicy", "ns2", "beta"),
		aclOwner("KafkaAccessPolicy", "ns2", "beta"), // same owner (Source triple) again: aclCoOwnerSummary keys only on Source*, so this must dedupe to one name regardless of the ACL's other fields
		aclOwner("KafkaTopic", "ns1", "alpha"),
		aclOwner("KafkaAccessPolicy", "ns3", "gamma"),
		aclOwner("KafkaTopic", "ns4", "delta"),
	}
	got := aclCoOwnerSummary(acls, 3)
	// Sorted alphabetically: "KafkaAccessPolicy/..." < "KafkaTopic/...".
	want := "KafkaAccessPolicy/ns2/beta, KafkaAccessPolicy/ns3/gamma, KafkaTopic/ns1/alpha, and 1 more"
	if got != want {
		t.Fatalf("aclCoOwnerSummary = %q, want %q", got, want)
	}
}

func TestACLCoOwnerSummary_NoOverflowUnderLimit(t *testing.T) {
	acls := []access.ACL{
		aclOwner("KafkaTopic", "ns1", "alpha"),
		aclOwner("KafkaAccessPolicy", "ns2", "beta"),
	}
	got := aclCoOwnerSummary(acls, 3)
	want := "KafkaAccessPolicy/ns2/beta, KafkaTopic/ns1/alpha"
	if got != want {
		t.Fatalf("aclCoOwnerSummary = %q, want %q", got, want)
	}
}

func TestACLCoOwnerSummary_Empty(t *testing.T) {
	if got := aclCoOwnerSummary(nil, 3); got != "" {
		t.Fatalf("aclCoOwnerSummary(nil) = %q, want empty", got)
	}
}

func roleBindingOwner(kind, ns, name string) rbac.RoleBinding {
	return rbac.RoleBinding{
		Principal: "User:x", Role: "DeveloperWrite", Scope: rbac.Scope{Type: "kafka", KafkaCluster: "lkc-1"},
		SourceKind: kind, SourceNamespace: ns, SourceName: name,
	}
}

func TestRoleBindingCoOwnerSummary_DedupSortAndLimit(t *testing.T) {
	bindings := []rbac.RoleBinding{
		roleBindingOwner("KafkaRoleBinding", "ns2", "beta"),
		roleBindingOwner("KafkaTopic", "ns1", "alpha"),
		roleBindingOwner("KafkaRoleBinding", "ns3", "gamma"),
		roleBindingOwner("KafkaTopic", "ns4", "delta"),
	}
	got := roleBindingCoOwnerSummary(bindings, 3)
	// Sorted alphabetically: "KafkaRoleBinding/..." < "KafkaTopic/...".
	want := "KafkaRoleBinding/ns2/beta, KafkaRoleBinding/ns3/gamma, KafkaTopic/ns1/alpha, and 1 more"
	if got != want {
		t.Fatalf("roleBindingCoOwnerSummary = %q, want %q", got, want)
	}
}

func TestRoleBindingCoOwnerSummary_DedupSameOwnerMultipleBindings(t *testing.T) {
	// Two DIFFERENT bindings from the SAME owner CR must collapse to one name.
	bindings := []rbac.RoleBinding{
		roleBindingOwner("KafkaTopic", "ns1", "alpha"),
		func() rbac.RoleBinding {
			b := roleBindingOwner("KafkaTopic", "ns1", "alpha")
			b.Role = "DeveloperRead"
			return b
		}(),
	}
	got := roleBindingCoOwnerSummary(bindings, 3)
	want := "KafkaTopic/ns1/alpha"
	if got != want {
		t.Fatalf("roleBindingCoOwnerSummary = %q, want %q (deduped)", got, want)
	}
}
