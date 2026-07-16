package cli

import (
	"github.com/spf13/cobra"

	"github.com/monedula-dev/monedula-gitops/internal/executor"
	"github.com/monedula-dev/monedula-gitops/internal/output"
)

func newApplyCmd() *cobra.Command {
	var f sharedFlags
	var dryRun, allowDelete, allowDestructive, prune, rotatePasswords bool
	cmd := &cobra.Command{
		Use:   "apply",
		Short: "Apply the manifests to Kafka",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if dryRun {
				// --rotate-passwords composes with --dry-run like every other
				// planned op: the RotateScramCredential ops are SHOWN (so the
				// rotation set can be reviewed), not applied.
				plan, _, err := planAndCompute(cmd, &f, true, output.KindApplyDryRun, rotatePasswords) // render plan, no mutation
				if err != nil {
					return err
				}
				printLockoutWarnings(cmd.ErrOrStderr(), plan, f.clusterConfigFiles)
				return nil
			}
			// Note: computeOps validates the output format before opening any
			// client, so no separate validateOutputFormat call is needed here.
			ctx := cmd.Context()
			plan, clients, cleanup, ops, err := computeOps(ctx, &f, true, rotatePasswords)
			defer cleanup()
			if err != nil {
				return err
			}
			// Self-lockout guard (spec §30.3): warn on STDERR before mutating,
			// while the operator can still abort. Heuristic only — super-users
			// bypass ACLs but cannot be detected client-side.
			printLockoutWarnings(cmd.ErrOrStderr(), plan, f.clusterConfigFiles)
			// In CLI mode --prune is THE prune switch (spec §10.3): resource-level
			// spec.prune is not consulted (the pipeline never stamps it), so prune
			// candidates are deleted iff the flag is given. The operator path uses
			// per-resource spec.prune consent instead.
			res := executor.Apply(ctx, clients, ops, executor.Approvals{Delete: allowDelete, Destructive: allowDestructive, Prune: prune})
			rendered, rerr := output.RenderApplyResult(res, f.output, plan.SelectedCluster)
			if rerr != nil {
				return &ExitError{Code: 2, Msg: rerr.Error()}
			}
			if _, werr := cmd.OutOrStdout().Write(rendered); werr != nil {
				return &ExitError{Code: 2, Msg: werr.Error()}
			}
			logger.Info("apply finished", "cluster", plan.SelectedCluster, "operations", len(ops), "ok", res.OK())
			if !res.OK() {
				// Empty Msg: the result is already rendered. Blocked-only -> 3
				// ("needs human approval", spec §15); anything failed -> 2.
				return &ExitError{Code: applyExitCode(res)}
			}
			return nil
		},
	}
	f.register(cmd)
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "render the planned operations without applying them")
	cmd.Flags().BoolVar(&allowDelete, "allow-delete", false, "permit topic deletion operations (used with deletionPolicy: Delete)")
	cmd.Flags().BoolVar(&allowDestructive, "allow-destructive", false, "permit destructive operations (partition increase, compatibility lowering, etc.)")
	cmd.Flags().BoolVar(&prune, "prune", false, "delete in-scope ACLs and RBAC role bindings that are no longer desired (otherwise they are only reported)")
	cmd.Flags().BoolVar(&rotatePasswords, "rotate-passwords", false, "re-upsert the SCRAM password of every declared, in-sync KafkaUser from its configured source (users with identity drift are updated regardless; with --dry-run the rotations are shown, not applied)")
	return cmd
}

// applyExitCode classifies a non-OK apply result into a process exit code
// (spec §15): 3 when the ONLY non-OK statuses are Blocked — the plan is sound
// but gated operations await human approval (--allow-delete/--allow-destructive)
// — and 2 when anything Failed/Skipped/Rejected/Unsupported, i.e. the run hit a
// real error. OK-equivalent statuses (Succeeded/ReportOnly/PruneDisabled) are
// ignored either way.
func applyExitCode(res executor.Result) int {
	for _, r := range res.Results {
		switch r.Status {
		case executor.Succeeded, executor.ReportOnly, executor.PruneDisabled, executor.Blocked:
			// OK-equivalent or approval-gated: does not force exit 2.
		default:
			return 2
		}
	}
	return 3
}
