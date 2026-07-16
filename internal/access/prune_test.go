package access

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func pruneACL(op string, prune bool) ACL {
	return ACL{
		Principal: "User:a", Host: "*", ResourceType: "topic", ResourceName: "t",
		PatternType: "literal", Operation: op, Permission: "Allow",
		Mode: "Enforce", Prune: prune,
	}
}

func TestBuildScopePruneAndMergeAcrossContributors(t *testing.T) {
	// A scope entry consents to pruning only if EVERY contributor opted in
	// (AND-merge — the opposite of the most-enforcing OR-merge used for Mode).
	cases := []struct {
		name string
		a, b bool
		want bool
	}{
		{"both opted in", true, true, true},
		{"first opted out", false, true, false},
		{"second opted out", true, false, false},
		{"neither opted in", false, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scope := BuildScope([]ACL{pruneACL("Read", tc.a), pruneACL("Write", tc.b)})
			require.Len(t, scope, 1, "both ACLs share one scope key")
			info, ok := scope.Info(pruneACL("Describe", false))
			require.True(t, ok)
			require.Equal(t, tc.want, info.Prune)
		})
	}
}

func TestBuildDesiredSetPruneAndMergeOnDedupe(t *testing.T) {
	// When the same tuple is desired by several resources, the deduped survivor
	// keeps Prune=true only if EVERY contributor opted in, so the scope built
	// from the deduped set still reflects the AND-merge.
	set, errs := BuildDesiredSet([]ACL{pruneACL("Read", true), pruneACL("Read", false)})
	require.Empty(t, errs)
	require.Len(t, set, 1)
	require.False(t, set[0].Prune)

	set, errs = BuildDesiredSet([]ACL{pruneACL("Read", true), pruneACL("Read", true)})
	require.Empty(t, errs)
	require.Len(t, set, 1)
	require.True(t, set[0].Prune)
}

func TestPruneExcludedFromIdentity(t *testing.T) {
	a := pruneACL("Read", true)
	b := pruneACL("Read", false)
	require.Equal(t, a.FullKey(), b.FullKey(), "Prune must not be part of ACL identity")
}
