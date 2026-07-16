package e2e

import (
	"strings"
	"testing"
)

func ptr(i int) *int { return &i }

func TestCheckExitCode(t *testing.T) {
	if r := CheckExitCode(0, ptr(0)); !r.Pass {
		t.Errorf("0==0 should pass: %+v", r)
	}
	if r := CheckExitCode(2, ptr(0)); r.Pass {
		t.Errorf("2!=0 should fail")
	}
	// nil expectation => skipped (pass, not asserted).
	if r := CheckExitCode(7, nil); !r.Pass {
		t.Errorf("nil expected exit code should not assert")
	}
}

func TestCheckOutput(t *testing.T) {
	out := "Plan:\n  CreateTopic payments.orders\nDone."
	if r := CheckOutput(out, []string{"CreateTopic", "payments.orders"}, nil); !r.Pass {
		t.Errorf("contains should pass: %+v", r)
	}
	r := CheckOutput(out, []string{"DeleteTopic"}, nil)
	if r.Pass || !strings.Contains(r.Detail, "DeleteTopic") || !strings.Contains(r.Name, "DeleteTopic") {
		t.Errorf("missing substring should fail and name it (Name + Detail): %+v", r)
	}
	if r := CheckOutput("identity collision on topic X", nil, []string{`identity collision`}); !r.Pass {
		t.Errorf("regex match should pass: %+v", r)
	}
	if r := CheckOutput("ok", nil, []string{`crash|panic`}); r.Pass {
		t.Errorf("non-matching regex should fail")
	}
	if r := CheckOutput("ok", nil, []string{`(`}); r.Pass {
		t.Errorf("invalid regex should fail, not panic")
	}
}

func TestReportFailed(t *testing.T) {
	var rep Report
	rep.Add(CheckResult{Name: "a", Pass: true})
	if rep.Failed() {
		t.Errorf("all-pass report should not be Failed")
	}
	rep.Add(CheckResult{Name: "b", Pass: false, Detail: "boom"})
	if !rep.Failed() {
		t.Errorf("report with a failure should be Failed")
	}
	if !strings.Contains(rep.String(), "boom") {
		t.Errorf("rendered report should include failure detail")
	}
}
