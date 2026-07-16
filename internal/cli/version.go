package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

// BuildInfo carries version metadata stamped at build time.
type BuildInfo struct{ Version, Commit, Date string }

func newVersionCmd(b BuildInfo) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the monedula-gitops version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "monedula-gitops %s (commit %s, built %s)\n", b.Version, b.Commit, b.Date)
			return nil
		},
	}
}
