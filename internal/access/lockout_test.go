package access

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func lockoutACL(principal, resourceType, resourceName, patternType string) ACL {
	return ACL{
		Principal: principal, Host: "*",
		ResourceType: resourceType, ResourceName: resourceName, PatternType: patternType,
		Operation: "Write", Permission: "Allow",
	}
}

func TestLockoutWarningsPrincipalMissing(t *testing.T) {
	desired := []ACL{
		lockoutACL("User:svc-a", "topic", "payments.orders", "literal"),
		lockoutACL("User:svc-b", "topic", "payments.orders", "literal"),
	}
	got := LockoutWarnings(desired, "User:admin")
	require.Len(t, got, 1)
	require.Contains(t, got[0], `topic "payments.orders"`)
	require.Contains(t, got[0], "User:admin")
	require.Contains(t, got[0], "super.user")
}

func TestLockoutWarningsPrincipalListed(t *testing.T) {
	desired := []ACL{
		lockoutACL("User:svc-a", "topic", "payments.orders", "literal"),
		lockoutACL("User:admin", "topic", "payments.orders", "literal"),
	}
	require.Empty(t, LockoutWarnings(desired, "User:admin"))
}

func TestLockoutWarningsPerResourceGrouping(t *testing.T) {
	// admin is listed on orders but not on refunds or the group: one warning
	// per uncovered resource, deterministically sorted.
	desired := []ACL{
		lockoutACL("User:admin", "topic", "payments.orders", "literal"),
		lockoutACL("User:svc-a", "topic", "payments.refunds", "literal"),
		lockoutACL("User:svc-a", "group", "payments-cg", "literal"),
	}
	got := LockoutWarnings(desired, "User:admin")
	require.Len(t, got, 2)
	require.Contains(t, got[0], `group "payments-cg"`)
	require.Contains(t, got[1], `topic "payments.refunds"`)
}

func TestLockoutWarningsDedupedAcrossPatternTypes(t *testing.T) {
	// Same resource name under two pattern types produces one warning line.
	desired := []ACL{
		lockoutACL("User:svc-a", "topic", "payments.", "prefixed"),
		lockoutACL("User:svc-b", "topic", "payments.", "literal"),
	}
	got := LockoutWarnings(desired, "User:admin")
	require.Len(t, got, 1)
}

func TestLockoutWarningsEmptyInputs(t *testing.T) {
	require.Empty(t, LockoutWarnings(nil, "User:admin"))
	require.Empty(t, LockoutWarnings([]ACL{lockoutACL("User:a", "topic", "t", "literal")}, ""))
}
