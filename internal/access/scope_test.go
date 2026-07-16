package access

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScopeContainsInScopeAndExcludesOthers(t *testing.T) {
	desired := []ACL{{Principal: "User:a", Host: "*", ResourceType: "topic", ResourceName: "t1", PatternType: "literal", Operation: "Read", Permission: "Allow"}}
	scope := BuildScope(desired)
	inScope := ACL{Principal: "User:a", ResourceType: "topic", ResourceName: "t1", PatternType: "literal", Operation: "Write"}
	outOfScope := ACL{Principal: "User:b", ResourceType: "topic", ResourceName: "t2", PatternType: "literal", Operation: "Read"}
	require.True(t, scope.Contains(inScope))
	require.False(t, scope.Contains(outOfScope))
}
