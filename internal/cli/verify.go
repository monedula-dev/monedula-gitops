package cli

import (
	"github.com/spf13/cobra"

	"github.com/monedula-dev/monedula-gitops/internal/operations"
	"github.com/monedula-dev/monedula-gitops/internal/output"
)

// newVerifyCmd builds the verify command. verify intentionally renders the full
// operation/drift list to stdout (same as diff) and additionally returns exit 1
// when drift exists, so CI gets both the details and the status code.
//
// Mode semantics (spec §16): drift on Enforce and DetectOnly resources counts
// toward the exit-1 contract; ObserveOnly drift is rendered but is informational
// and never fails the run. Informational findings (MirrorTopicProgressing) are
// rendered but never count as drift. Operational errors (StateReadError: live
// state could not be read) fail with exit 2 in every mode.
func newVerifyCmd() *cobra.Command {
	var f sharedFlags
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Verify live state matches the manifests (exit 1 on drift)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			// rotatePasswords is always false here: rotation is an apply-time
			// action (apply --rotate-passwords), never drift for verify.
			_, ops, err := planAndCompute(cmd, &f, true, output.KindVerify, false)
			if err != nil {
				return err
			}
			// Live state that could not be read is an operational error, not
			// drift: it fails the run (exit 2) whatever the resource's mode,
			// because verify cannot vouch for state it did not see.
			for _, op := range ops {
				if operations.OperationalError(op.Action) {
					return &ExitError{Code: 2, Msg: "live state could not be read; see StateReadError operations"}
				}
			}
			for _, op := range ops {
				if operations.Informational(op.Action) {
					continue // expected transient state (e.g. a mirror still being set up)
				}
				if op.Mode != operations.ModeObserveOnly {
					return &ExitError{Code: 1, Msg: ""}
				}
			}
			return nil
		},
	}
	f.register(cmd)
	return cmd
}
