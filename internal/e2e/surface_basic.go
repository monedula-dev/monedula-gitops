package e2e

import (
	"fmt"
	"regexp"
	"strings"
)

// CheckExitCode compares an observed exit code against the expectation. A nil
// want means the surface is not asserted (the result passes, unasserted).
func CheckExitCode(got int, want *int) CheckResult {
	if want == nil {
		return CheckResult{Name: "exitCode (not asserted)", Pass: true}
	}
	res := CheckResult{Name: fmt.Sprintf("exitCode == %d", *want), Pass: got == *want}
	if !res.Pass {
		// Detail is only meaningful on failure; leaving it empty on pass keeps
		// callers that inspect the struct (e.g. a JSON report) free of noise.
		res.Detail = fmt.Sprintf("expected %d, got %d", *want, got)
	}
	return res
}

// CheckOutput asserts that output contains every substring in contains and
// matches every regex in matches. Returns one failing result on the first
// problem found (named so the reader knows which clause failed), else a single
// passing result.
func CheckOutput(output string, contains, matches []string) CheckResult {
	for _, sub := range contains {
		if !strings.Contains(output, sub) {
			return CheckResult{
				Name:   fmt.Sprintf("output contains %q", sub),
				Pass:   false,
				Detail: fmt.Sprintf("substring %q not found in output:\n%s", sub, output),
			}
		}
	}
	for _, pat := range matches {
		re, err := regexp.Compile(pat)
		if err != nil {
			return CheckResult{
				Name:   fmt.Sprintf("output matches /%s/", pat),
				Pass:   false,
				Detail: fmt.Sprintf("invalid regex %q: %v", pat, err),
			}
		}
		if !re.MatchString(output) {
			return CheckResult{
				Name:   fmt.Sprintf("output matches /%s/", pat),
				Pass:   false,
				Detail: fmt.Sprintf("regex %q did not match output:\n%s", pat, output),
			}
		}
	}
	return CheckResult{Name: "output checks", Pass: true}
}
