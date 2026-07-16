package e2e

import (
	"fmt"
	"strings"
)

// CheckResult is the outcome of one assertion. Name identifies the check (e.g.
// "exitCode", "output contains \"CreateTopic\"", "topic payments.orders config
// cleanup.policy"). Detail explains a failure (expected vs actual).
type CheckResult struct {
	Name   string
	Pass   bool
	Detail string
}

// Report accumulates CheckResults for a scenario run and renders them.
type Report struct {
	Results []CheckResult
}

// Add appends a result.
func (r *Report) Add(c CheckResult) { r.Results = append(r.Results, c) }

// Failed reports whether any accumulated result failed.
func (r *Report) Failed() bool {
	for _, c := range r.Results {
		if !c.Pass {
			return true
		}
	}
	return false
}

// String renders a human-readable pass/fail list, with details on failures.
func (r *Report) String() string {
	var b strings.Builder
	for _, c := range r.Results {
		mark := "PASS"
		if !c.Pass {
			mark = "FAIL"
		}
		fmt.Fprintf(&b, "[%s] %s\n", mark, c.Name)
		if !c.Pass && c.Detail != "" {
			fmt.Fprintf(&b, "       %s\n", c.Detail)
		}
	}
	return b.String()
}
