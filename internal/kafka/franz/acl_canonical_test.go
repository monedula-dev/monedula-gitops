package franz

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// The tool's canonical ACL vocabulary (what manifests use, what access.ACL
// compares by FullKey). ListACLs MUST return exactly these forms, or the diff
// never converges: it would re-create every ACL on every apply.

func TestCanonicalRoundTripResourceTypes(t *testing.T) {
	for _, want := range []string{"topic", "group", "cluster", "transactionalId", "delegationToken"} {
		enum, err := kmsg.ParseACLResourceType(want)
		require.NoError(t, err, want)
		require.Equal(t, want, canonicalResourceType(enum), "resource type must round-trip")
	}
}

func TestCanonicalRoundTripPatternTypes(t *testing.T) {
	for _, want := range []string{"literal", "prefixed"} {
		enum, err := kmsg.ParseACLResourcePatternType(want)
		require.NoError(t, err, want)
		require.Equal(t, want, canonicalPatternType(enum), "pattern type must round-trip")
	}
}

func TestCanonicalRoundTripOperations(t *testing.T) {
	for _, want := range []string{
		"Read", "Write", "Create", "Delete", "Alter", "Describe",
		"ClusterAction", "DescribeConfigs", "AlterConfigs", "IdempotentWrite", "All",
	} {
		enum, err := kmsg.ParseACLOperation(want)
		require.NoError(t, err, want)
		require.Equal(t, want, canonicalOperation(enum), "operation must round-trip")
	}
}

func TestCanonicalRoundTripPermissions(t *testing.T) {
	for _, want := range []string{"Allow", "Deny"} {
		enum, err := kmsg.ParseACLPermissionType(want)
		require.NoError(t, err, want)
		require.Equal(t, want, canonicalPermission(enum), "permission must round-trip")
	}
}

// Regression guard for the original bug: the raw kmsg .String() forms are NOT
// the canonical vocabulary, so the converters must never fall through to them
// for known values.
func TestCanonicalIsNotKmsgString(t *testing.T) {
	require.NotEqual(t, kmsg.ACLResourceTypeTopic.String(), canonicalResourceType(kmsg.ACLResourceTypeTopic))
	require.NotEqual(t, kmsg.ACLOperationDescribeConfigs.String(), canonicalOperation(kmsg.ACLOperationDescribeConfigs))
	require.NotEqual(t, kmsg.ACLPermissionTypeAllow.String(), canonicalPermission(kmsg.ACLPermissionTypeAllow))
	require.NotEqual(t, kmsg.ACLResourcePatternTypeLiteral.String(), canonicalPatternType(kmsg.ACLResourcePatternTypeLiteral))
}
