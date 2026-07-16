package cli

import (
	"fmt"
	"io"

	"github.com/monedula-dev/monedula-gitops/internal/access"
	"github.com/monedula-dev/monedula-gitops/internal/pipeline"
	"github.com/monedula-dev/monedula-gitops/internal/secrets"
)

// connectingPrincipal resolves the SASL principal the CLI connects to the
// selected cluster as, in Kafka's "User:<username>" form. The username is a
// secret reference (auth.scram.username), resolved through the same
// FileEnvResolver the live client uses. It returns "" — meaning "no lockout
// check possible" — when:
//   - no auth is configured, or the mechanism is None (no SASL principal);
//   - the username reference cannot be resolved. The lockout warning is a
//     best-effort heuristic (spec §30.3) and must never turn a working apply
//     into a failure; a real client build would surface the same resolution
//     error anyway.
func connectingPrincipal(plan *pipeline.Plan, clusterConfigFiles []string) string {
	cl := plan.Clusters[plan.SelectedCluster]
	if cl == nil || cl.Spec.Auth == nil || cl.Spec.Auth.SCRAM == nil {
		return ""
	}
	switch cl.Spec.Auth.Mechanism {
	case "", "None":
		return ""
	}
	user, err := secrets.FileEnvResolver{BaseDir: baseDir(clusterConfigFiles)}.Resolve(cl.Spec.Auth.SCRAM.Username)
	if err != nil || user == "" {
		return ""
	}
	return "User:" + user
}

// printLockoutWarnings writes the spec §30.3 self-lockout warnings for the
// plan's desired ACL set to w (the command's STDERR — stdout stays pipeable).
// Called by apply and apply --dry-run; diff/verify stay warning-free (they
// never mutate, so nothing is about to be locked out). Super-users cannot be
// detected client-side, so the warning is advisory (see access.LockoutWarnings).
func printLockoutWarnings(w io.Writer, plan *pipeline.Plan, clusterConfigFiles []string) {
	principal := connectingPrincipal(plan, clusterConfigFiles)
	if principal == "" {
		return
	}
	for _, warning := range access.LockoutWarnings(plan.DesiredACLs, principal) {
		_, _ = fmt.Fprintln(w, "warning: "+warning)
	}
}
