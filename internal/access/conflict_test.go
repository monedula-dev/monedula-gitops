package access

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func acl(principal, perm, kind, name string) ACL {
	return ACL{Principal: principal, Host: "*", ResourceType: "topic", ResourceName: "t",
		PatternType: "literal", Operation: "Write", Permission: perm,
		SourceKind: kind, SourceName: name}
}

func TestConflictsDetectsOpposite(t *testing.T) {
	cs := Conflicts([]ACL{
		acl("User:a", "Allow", "KafkaAccessPolicy", "p1"),
		acl("User:a", "Deny", "KafkaAccessPolicy", "p2"),
	})
	require.Len(t, cs, 1)
	require.Equal(t, "p1", cs[0].A.SourceName)
	require.Equal(t, "p2", cs[0].B.SourceName)
	require.NotEmpty(t, cs[0].Subject)
}

func TestConflictsNoneWhenSamePermission(t *testing.T) {
	cs := Conflicts([]ACL{
		acl("User:a", "Allow", "KafkaAccessPolicy", "p1"),
		acl("User:a", "Allow", "KafkaTopic", "t1"), // dup, not a conflict
	})
	require.Empty(t, cs)
}

func TestConflictsDifferentTuplesNoConflict(t *testing.T) {
	a := acl("User:a", "Allow", "KafkaAccessPolicy", "p1")
	b := acl("User:b", "Deny", "KafkaAccessPolicy", "p2") // different principal
	require.Empty(t, Conflicts([]ACL{a, b}))
}

func TestStampSource(t *testing.T) {
	in := []ACL{{Principal: "User:a"}}
	StampSource(in, "KafkaTopic", "ns", "t1")
	require.Equal(t, "KafkaTopic", in[0].SourceKind)
	require.Equal(t, "ns", in[0].SourceNamespace)
	require.Equal(t, "t1", in[0].SourceName)
}
