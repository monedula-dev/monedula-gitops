package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/monedula-dev/monedula-gitops/internal/pipeline"
)

func newValidateCmd() *cobra.Command {
	var f sharedFlags
	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Validate manifests (and cluster refs if cluster config is provided)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			opts, err := f.options(false)
			if err != nil {
				return err
			}
			plan, err := pipeline.Build(opts)
			if err != nil {
				return &ExitError{Code: 2, Msg: err.Error()}
			}
			n := len(plan.Topics) + len(plan.Policies) + len(plan.Quotas) + len(plan.RoleBindings)
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%d resource(s) valid\n", n)
			return nil
		},
	}
	f.register(cmd)
	return cmd
}
