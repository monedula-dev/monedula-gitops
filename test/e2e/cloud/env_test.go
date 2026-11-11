//go:build cloud

package cloude2e

import (
	"strings"
	"testing"
)

func TestRESTEnvState(t *testing.T) {
	full := map[string]string{}
	for _, n := range restEnvNames {
		full[n] = "x"
	}
	lookup := func(m map[string]string) func(string) string {
		return func(k string) string { return m[k] }
	}

	if ok, missing := restEnvState(lookup(map[string]string{})); ok || len(missing) != 0 {
		t.Fatalf("none set: want (false, none missing), got (%v, %v)", ok, missing)
	}
	if ok, missing := restEnvState(lookup(full)); !ok || len(missing) != 0 {
		t.Fatalf("all set: want (true, none missing), got (%v, %v)", ok, missing)
	}
	for _, drop := range restEnvNames {
		partial := map[string]string{}
		for k, v := range full {
			if k != drop {
				partial[k] = v
			}
		}
		ok, missing := restEnvState(lookup(partial))
		if ok || len(missing) != 1 || missing[0] != drop {
			t.Fatalf("missing %s: got (%v, %v)", drop, ok, missing)
		}
	}
	// An empty value counts as unset.
	empty := map[string]string{envLinkName: ""}
	if ok, missing := restEnvState(lookup(empty)); ok || len(missing) != 0 {
		t.Fatalf("empty value only: got (%v, %v)", ok, missing)
	}
}

func TestMirrorRow(t *testing.T) {
	if r := mirrorRow("", true, "done"); r.verdict != "OK" {
		t.Fatalf("ok: %+v", r)
	}
	r := mirrorRow("", true, "CLEANUP FAILED: x")
	if r.verdict != "FAILED" || !strings.Contains(r.note, "CLEANUP FAILED") {
		t.Fatalf("cleanup failure must be reported: %+v", r)
	}
	if r := mirrorRow("why", false, ""); r.verdict != "SKIPPED" {
		t.Fatalf("skip: %+v", r)
	}
}
