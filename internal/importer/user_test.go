package importer_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/importer"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
)

// makeUserSnap builds a Snapshot with no topics/ACLs/quotas and the given
// SCRAM credentials.
func makeUserSnap(creds []kafka.ScramCredential) importer.Snapshot {
	return importer.Snapshot{ScramCredentials: creds}
}

// TestBuildUsersBasic verifies a single-mechanism credential reconstructs a
// KafkaUser with an explicit username, the live mechanism, no iterations
// (default), and a placeholder env-var password reference.
func TestBuildUsersBasic(t *testing.T) {
	snap := makeUserSnap([]kafka.ScramCredential{
		{User: "svc-checkout", Mechanism: "SCRAM-SHA-512", Iterations: 4096},
	})

	r := importer.Build(snap, "prod", nil, nil)

	require.Len(t, r.Users, 1)
	u := r.Users[0]
	require.Equal(t, "KafkaUser", u.Kind)
	require.Equal(t, "svc-checkout", u.Spec.Username)
	require.Equal(t, "SCRAM-SHA-512", u.Spec.Mechanism)
	require.Nil(t, u.Spec.Iterations, "default iterations (4096) must not be emitted")
	require.Equal(t, "prod", u.Spec.ClusterRef.Name)
	require.Equal(t, "Orphan", u.Spec.DeletionPolicy)
	require.NotNil(t, u.Spec.Password)
	require.NotNil(t, u.Spec.Password.ValueFrom)
	require.Equal(t, "KAFKA_USER_SVC_CHECKOUT_PASSWORD", u.Spec.Password.ValueFrom.Env)
	require.Nil(t, u.Spec.Password.Generate)

	// Aggregate password-unrecoverability warning must be present.
	found := false
	for _, w := range r.Warnings {
		if w == passwordWarning {
			found = true
		}
	}
	require.True(t, found, "want aggregate password warning, got warnings: %v", r.Warnings)
}

const passwordWarning = "imported KafkaUser passwords are placeholders (env var references) — Kafka never exposes SCRAM passwords, so these are NOT recoverable from the cluster; set the referenced env vars (or switch to secretKeyRef/generate) before applying, or apply will fail to resolve a missing credential"

// TestBuildUsersNonDefaultIterations verifies spec.iterations IS emitted when
// the live value differs from Kafka's default (4096).
func TestBuildUsersNonDefaultIterations(t *testing.T) {
	snap := makeUserSnap([]kafka.ScramCredential{
		{User: "svc-checkout", Mechanism: "SCRAM-SHA-512", Iterations: 8192},
	})
	r := importer.Build(snap, "prod", nil, nil)
	require.Len(t, r.Users, 1)
	require.NotNil(t, r.Users[0].Spec.Iterations)
	require.EqualValues(t, 8192, *r.Users[0].Spec.Iterations)
}

// TestBuildUsersBothMechanismsPrefersStrongerAndWarns verifies that a user
// with both SCRAM-SHA-256 and SCRAM-SHA-512 live gets exactly ONE manifest
// carrying SCRAM-SHA-512, plus a warning naming the dropped mechanism.
func TestBuildUsersBothMechanismsPrefersStrongerAndWarns(t *testing.T) {
	snap := makeUserSnap([]kafka.ScramCredential{
		{User: "svc-checkout", Mechanism: "SCRAM-SHA-256", Iterations: 4096},
		{User: "svc-checkout", Mechanism: "SCRAM-SHA-512", Iterations: 4096},
	})
	r := importer.Build(snap, "prod", nil, nil)

	require.Len(t, r.Users, 1)
	require.Equal(t, "SCRAM-SHA-512", r.Users[0].Spec.Mechanism)

	wantWarning := `user "svc-checkout" also has a SCRAM-SHA-256 credential; only the SCRAM-SHA-512 one is captured — manage the other manually or add a second manifest with a different metadata.name`
	found := false
	for _, w := range r.Warnings {
		if w == wantWarning {
			found = true
		}
	}
	require.True(t, found, "want both-mechanisms warning, got: %v", r.Warnings)
}

// TestBuildUsersUsernameSlugAndDisambiguation verifies adversarial usernames
// (dots, uppercase) slug via the topicMetaName-style helper — which, unlike
// plain slug(), preserves '.' as a meaningful label separator — and that
// distinct users colliding on the slugged name get disambiguated.
func TestBuildUsersUsernameSlugAndDisambiguation(t *testing.T) {
	snap := makeUserSnap([]kafka.ScramCredential{
		// "Service.Checkout" slugs (dots preserved) to "service.checkout" —
		// distinct from "service-checkout" below, so no collision here; this
		// exercises the dotted-username path without expecting one.
		{User: "Service.Checkout", Mechanism: "SCRAM-SHA-512", Iterations: 4096},
		// "Service_Checkout" (underscore, uppercase) slugs to "service-checkout",
		// which collides with the literal "service-checkout" username below.
		{User: "Service_Checkout", Mechanism: "SCRAM-SHA-512", Iterations: 4096},
		{User: "service-checkout", Mechanism: "SCRAM-SHA-512", Iterations: 4096},
	})
	r := importer.Build(snap, "prod", nil, nil)
	require.Len(t, r.Users, 3)

	names := map[string]bool{}
	usernameByName := map[string]string{}
	for _, u := range r.Users {
		names[u.Name] = true
		usernameByName[u.Name] = u.Spec.Username
		// spec.username always carries the live principal verbatim regardless
		// of slugging.
	}
	require.Len(t, names, 3, "names must be distinct: %v", names)
	require.True(t, names["service.checkout"], "want dotted slug present: %v", names)
	require.True(t, names["service-checkout"], "want first literal slug present: %v", names)
	require.True(t, names["service-checkout-2"], "want disambiguated slug present: %v", names)

	sawCollisionWarning := false
	for _, w := range r.Warnings {
		if strings.Contains(w, "user name collision") && strings.Contains(w, "service-checkout-2") {
			sawCollisionWarning = true
		}
	}
	require.True(t, sawCollisionWarning, "want name collision warning, got: %v", r.Warnings)
}

// TestBuildUsersEnvVarSanitizeAndDisambiguation verifies the password env var
// name is derived from the username (uppercased, non-alnum -> '_') and that
// colliding sanitized env var names get a numeric suffix.
func TestBuildUsersEnvVarSanitizeAndDisambiguation(t *testing.T) {
	snap := makeUserSnap([]kafka.ScramCredential{
		{User: "svc.checkout", Mechanism: "SCRAM-SHA-512", Iterations: 4096},
		{User: "svc-checkout", Mechanism: "SCRAM-SHA-512", Iterations: 4096},
	})
	r := importer.Build(snap, "prod", nil, nil)
	require.Len(t, r.Users, 2)

	envNames := map[string]bool{}
	for _, u := range r.Users {
		require.NotNil(t, u.Spec.Password)
		require.NotNil(t, u.Spec.Password.ValueFrom)
		envNames[u.Spec.Password.ValueFrom.Env] = true
	}
	require.Len(t, envNames, 2, "env var names must be distinct: %v", envNames)
	require.True(t, envNames["KAFKA_USER_SVC_CHECKOUT_PASSWORD"])
	require.True(t, envNames["KAFKA_USER_SVC_CHECKOUT_PASSWORD_2"])
}

// TestBuildUsersSkipsConnectingPrincipal verifies the connecting principal is
// omitted by default and a warning is recorded naming it.
func TestBuildUsersSkipsConnectingPrincipal(t *testing.T) {
	snap := makeUserSnap([]kafka.ScramCredential{
		{User: "svc-checkout", Mechanism: "SCRAM-SHA-512", Iterations: 4096},
		{User: "cli-user", Mechanism: "SCRAM-SHA-512", Iterations: 4096},
	})
	r := importer.Build(snap, "prod", nil, nil, importer.BuildOptions{ConnectingUser: "cli-user"})

	require.Len(t, r.Users, 1)
	require.Equal(t, "svc-checkout", r.Users[0].Spec.Username)

	wantWarning := `skipped connecting principal "cli-user" (managing your own credential risks self-lockout); use --include-connecting-user to include it`
	found := false
	for _, w := range r.Warnings {
		if w == wantWarning {
			found = true
		}
	}
	require.True(t, found, "want skipped-connecting-principal warning, got: %v", r.Warnings)
}

// TestBuildUsersIncludeConnectingPrincipal verifies --include-connecting-user
// (BuildOptions.IncludeConnectingUser) captures the connecting principal
// instead of skipping it, and no skip warning is recorded.
func TestBuildUsersIncludeConnectingPrincipal(t *testing.T) {
	snap := makeUserSnap([]kafka.ScramCredential{
		{User: "cli-user", Mechanism: "SCRAM-SHA-512", Iterations: 4096},
	})
	r := importer.Build(snap, "prod", nil, nil, importer.BuildOptions{
		ConnectingUser:        "cli-user",
		IncludeConnectingUser: true,
	})

	require.Len(t, r.Users, 1)
	require.Equal(t, "cli-user", r.Users[0].Spec.Username)
	for _, w := range r.Warnings {
		require.NotContains(t, w, "skipped connecting principal")
	}
}

// TestBuildUsersNoConnectingUserConfigured verifies that when
// BuildOptions.ConnectingUser is "" (unresolvable — e.g. mTLS/OAuth clusters),
// nothing is skipped and no principal is treated specially.
func TestBuildUsersNoConnectingUserConfigured(t *testing.T) {
	snap := makeUserSnap([]kafka.ScramCredential{
		{User: "svc-checkout", Mechanism: "SCRAM-SHA-512", Iterations: 4096},
	})
	r := importer.Build(snap, "prod", nil, nil, importer.BuildOptions{})
	require.Len(t, r.Users, 1)
	require.Equal(t, "svc-checkout", r.Users[0].Spec.Username)
}

// TestBuildUsersEmptySnapshot verifies no ScramCredentials produces no Users
// and no user-related warnings.
func TestBuildUsersEmptySnapshot(t *testing.T) {
	r := importer.Build(importer.Snapshot{}, "prod", nil, nil)
	require.Empty(t, r.Users)
}

// TestBuildUsersDeterministic verifies Build is deterministic for a fixed
// snapshot (name ordering, mechanism selection, env var assignment all stable).
func TestBuildUsersDeterministic(t *testing.T) {
	snap := makeUserSnap([]kafka.ScramCredential{
		{User: "zzz-user", Mechanism: "SCRAM-SHA-512", Iterations: 4096},
		{User: "aaa-user", Mechanism: "SCRAM-SHA-256", Iterations: 4096},
		{User: "aaa-user", Mechanism: "SCRAM-SHA-512", Iterations: 4096},
	})
	r1 := importer.Build(snap, "prod", nil, nil)
	r2 := importer.Build(snap, "prod", nil, nil)
	require.Len(t, r1.Users, 2)
	require.Len(t, r2.Users, 2)
	for i := range r1.Users {
		require.Equal(t, r1.Users[i].Name, r2.Users[i].Name)
		require.Equal(t, r1.Users[i].Spec.Username, r2.Users[i].Spec.Username)
	}
	// Sorted by metadata.name: "aaa-user" < "zzz-user".
	require.Equal(t, "aaa-user", r1.Users[0].Name)
	require.Equal(t, "zzz-user", r1.Users[1].Name)
}
