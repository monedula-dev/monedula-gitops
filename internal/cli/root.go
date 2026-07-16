package cli

import "github.com/spf13/cobra"

// NewRootCmd builds the monedula-gitops command tree with placeholder build
// info. It is kept for callers (and tests) that do not stamp a version.
func NewRootCmd() *cobra.Command {
	return NewRootCmdWithBuildInfo(BuildInfo{Version: "dev", Commit: "none", Date: "unknown"})
}

// NewRootCmdWithBuildInfo builds the monedula-gitops command tree, threading the
// supplied build info into the `version` subcommand and the `--version` flag.
func NewRootCmdWithBuildInfo(b BuildInfo) *cobra.Command {
	var logLevel string
	root := &cobra.Command{
		Use:           "monedula-gitops",
		Short:         "GitOps tool for managing Kafka resources declaratively",
		SilenceUsage:  true,
		SilenceErrors: true,
		// Logging (spec §31) is configured before any subcommand runs. Logs go
		// to STDERR so command output on stdout stays pipeable.
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			if err := setupLogging(cmd.ErrOrStderr(), logLevel); err != nil {
				return err
			}
			logger.Info("command start", "command", cmd.Name())
			return nil
		},
		Version: b.Version,
	}
	root.PersistentFlags().StringVar(&logLevel, "log-level", "warn",
		"log verbosity on stderr: error, warn, info, or debug")
	root.AddCommand(newValidateCmd())
	root.AddCommand(newDiffCmd())
	root.AddCommand(newVerifyCmd())
	root.AddCommand(newApplyCmd())
	root.AddCommand(newImportCmd())
	root.AddCommand(newDoctorCmd())
	root.AddCommand(newOperatorCmd())
	root.AddCommand(newE2ECmd())
	root.AddCommand(newVersionCmd(b))
	return root
}
