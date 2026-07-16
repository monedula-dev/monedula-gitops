package cli

// ExitError carries a process exit code out of a command's RunE so main can map
// it to os.Exit. Code 3 = apply blocked only on approval gates (every non-OK
// operation is Blocked; rerun with the gate flag, spec §15), 2 = validation/
// config/connectivity error or a failed apply, 1 = drift (verify only),
// 0 = success. Msg, when non-empty, is printed to stderr by main.
type ExitError struct {
	Code int
	Msg  string
}

func (e *ExitError) Error() string { return e.Msg }
