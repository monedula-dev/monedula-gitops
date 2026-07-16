package access

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func modeACL(op, mode, srcKind, srcNS, srcName string) ACL {
	return ACL{
		Principal: "User:x", Host: "*", ResourceType: "topic", ResourceName: "t",
		PatternType: "literal", Operation: op, Permission: "Allow",
		Mode: mode, SourceKind: srcKind, SourceNamespace: srcNS, SourceName: srcName,
	}
}

func TestModeExcludedFromIdentity(t *testing.T) {
	a := modeACL("Read", "Enforce", "KafkaTopic", "payments", "orders")
	b := modeACL("Read", "ObserveOnly", "KafkaAccessPolicy", "billing", "policy")
	require.Equal(t, a.FullKey(), b.FullKey(), "Mode/Source* must not be part of ACL identity")
}

func TestDesiredSetDedupeMostEnforcingModeWins(t *testing.T) {
	cases := []struct {
		name  string
		modes []string
		want  string
	}{
		{"enforce-beats-observe", []string{"ObserveOnly", "Enforce"}, "Enforce"},
		{"enforce-beats-observe-reversed", []string{"Enforce", "ObserveOnly"}, "Enforce"},
		{"detect-beats-observe", []string{"ObserveOnly", "DetectOnly"}, "DetectOnly"},
		{"enforce-beats-detect", []string{"DetectOnly", "Enforce"}, "Enforce"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var in []ACL
			for _, m := range tc.modes {
				in = append(in, modeACL("Read", m, "KafkaTopic", "ns", "n"))
			}
			set, errs := BuildDesiredSet(in)
			require.Empty(t, errs)
			require.Len(t, set, 1)
			require.Equal(t, tc.want, set[0].Mode)
		})
	}
}

func TestDesiredSetDedupeFirstOwnerWins(t *testing.T) {
	first := modeACL("Read", "ObserveOnly", "KafkaTopic", "payments", "orders")
	second := modeACL("Read", "Enforce", "KafkaAccessPolicy", "billing", "policy")
	set, errs := BuildDesiredSet([]ACL{first, second})
	require.Empty(t, errs)
	require.Len(t, set, 1)
	// Mode is upgraded to the most enforcing contributor, but owner attribution
	// stays with the FIRST contributor (deterministic).
	require.Equal(t, "Enforce", set[0].Mode)
	require.Equal(t, "KafkaTopic", set[0].SourceKind)
	require.Equal(t, "payments", set[0].SourceNamespace)
	require.Equal(t, "orders", set[0].SourceName)
}

func TestBuildScopeInfoMostEnforcingAndFirstOwner(t *testing.T) {
	// Two ACLs share a scope key (same principal/resource pattern, different
	// operation) but disagree on mode: the scope entry carries the most
	// enforcing mode and the first contributor's owner.
	a := modeACL("Read", "ObserveOnly", "KafkaTopic", "payments", "orders")
	b := modeACL("Write", "Enforce", "KafkaAccessPolicy", "billing", "policy")
	scope := BuildScope([]ACL{a, b})

	info, ok := scope.Info(a)
	require.True(t, ok)
	require.Equal(t, "Enforce", info.Mode)
	require.Equal(t, "KafkaTopic", info.SourceKind)
	require.Equal(t, "payments", info.SourceNamespace)
	require.Equal(t, "orders", info.SourceName)

	_, ok = scope.Info(ACL{Principal: "User:other", ResourceType: "topic", ResourceName: "t", PatternType: "literal"})
	require.False(t, ok)
}
