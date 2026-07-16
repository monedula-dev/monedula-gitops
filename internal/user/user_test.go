package user

import (
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/stretchr/testify/require"
)

func i32(v int32) *int32 { return &v }

func TestCompileBasic(t *testing.T) {
	u := &v1alpha1.KafkaUser{}
	u.Spec.Username = "svc-checkout"
	u.Spec.Mechanism = "SCRAM-SHA-512"
	c := Compile(u)
	require.Equal(t, Credential{Username: "svc-checkout", Mechanism: "SCRAM-SHA-512"}, c)
	require.Equal(t, int32(0), c.Iterations)
}

func TestCompileIterationsNilIsZero(t *testing.T) {
	u := &v1alpha1.KafkaUser{}
	u.Spec.Username = "svc"
	u.Spec.Mechanism = "SCRAM-SHA-256"
	u.Spec.Iterations = nil
	c := Compile(u)
	require.Equal(t, int32(0), c.Iterations)
}

func TestCompileIterationsSet(t *testing.T) {
	u := &v1alpha1.KafkaUser{}
	u.Spec.Username = "svc"
	u.Spec.Mechanism = "SCRAM-SHA-256"
	u.Spec.Iterations = i32(8192)
	c := Compile(u)
	require.Equal(t, int32(8192), c.Iterations)
}

func TestCompileAssumesDefaultedInput(t *testing.T) {
	// Compile does no defaulting itself: an undefaulted CR compiles verbatim
	// (empty username/mechanism), mirroring quota.Compile's documented
	// assumption that defaulting has already run upstream.
	u := &v1alpha1.KafkaUser{}
	c := Compile(u)
	require.Equal(t, Credential{}, c)
}

func TestKeySameUsernameSameClusterCollide(t *testing.T) {
	require.Equal(t, Key("prod", "svc"), Key("prod", "svc"))
}

func TestKeyDifferentMechanismStillCollides(t *testing.T) {
	// Identity is (cluster, username) only: mechanism is not part of the key,
	// so two CRs for the same username with different mechanisms still
	// collide (they'd fight over the same credential set).
	u1 := &v1alpha1.KafkaUser{}
	u1.Spec.Username = "svc"
	u1.Spec.Mechanism = "SCRAM-SHA-256"
	u2 := &v1alpha1.KafkaUser{}
	u2.Spec.Username = "svc"
	u2.Spec.Mechanism = "SCRAM-SHA-512"
	c1 := Compile(u1)
	c2 := Compile(u2)
	require.Equal(t, Key("prod", c1.Username), Key("prod", c2.Username))
}

func TestKeyDifferentClusterNoCollision(t *testing.T) {
	require.NotEqual(t, Key("prod", "svc"), Key("staging", "svc"))
}

func TestKeyDifferentUsernameNoCollision(t *testing.T) {
	require.NotEqual(t, Key("prod", "svc-a"), Key("prod", "svc-b"))
}
