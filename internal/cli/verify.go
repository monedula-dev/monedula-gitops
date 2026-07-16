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
// and never fails the run.
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
			for _, op := range ops {
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
