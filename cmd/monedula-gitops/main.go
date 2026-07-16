package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/monedula-dev/monedula-gitops/internal/cli"
)

// Build info, stamped by GoReleaser via -ldflags -X.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	// Ctrl-C (SIGINT) / SIGTERM cancels the context threaded through
	// cmd.Context() into every command's RunE, so in-flight admin calls (diff,
	// apply, doctor, import) are cancelled instead of the process dying with
	// unaccounted work outstanding.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	root := cli.NewRootCmdWithBuildInfo(cli.BuildInfo{Version: version, Commit: commit, Date: date})
	err := root.ExecuteContext(ctx)
	var ee *cli.ExitError
	if errors.As(err, &ee) {
		if ee.Msg != "" {
			fmt.Fprintln(os.Stderr, ee.Msg)
		}
		os.Exit(ee.Code)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}
}
