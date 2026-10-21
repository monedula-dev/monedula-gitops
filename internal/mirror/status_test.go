package mirror

import "testing"

func TestClassifyStatusTable(t *testing.T) {
	cases := map[string]StatusClass{
		"ACTIVE":                    StatusHealthy,
		"PAUSED":                    StatusUnhealthy,
		"LINK_PAUSED":               StatusUnhealthy,
		"FAILED":                    StatusUnhealthy,
		"LINK_FAILED":               StatusUnhealthy,
		"SOURCE_UNAVAILABLE":        StatusUnhealthy,
		"PENDING_STOPPED":           StatusStopping,
		"STOPPED":                   StatusStopped,
		"PENDING_MIRROR":            StatusProgressing,
		"PENDING_SYNCHRONIZE":       StatusProgressing,
		"PENDING_SETUP_FOR_RESTORE": StatusProgressing,
		"PENDING_RESTORE":           StatusProgressing,
		"PENDING_RESTORE_MIRROR":    StatusProgressing,
		"PENDING_FOO":               StatusUnknown,
		"":                          StatusUnknown,
		"active":                    StatusUnknown,
	}
	for in, want := range cases {
		if got := ClassifyStatus(in); got != want {
			t.Errorf("ClassifyStatus(%q)=%v want %v", in, got, want)
		}
	}
}
