// Package mirror reads a live snapshot of destination-cluster topic and mirror
// state and classifies topics fail-closed: any read failure or unrecognized
// state yields Unknown, never Absent or Normal.
package mirror

// StatusClass groups REST mirror_status values by how they must be treated.
type StatusClass int

const (
	// StatusUnknown is any unrecognized status (including other PENDING_*).
	StatusUnknown StatusClass = iota
	StatusHealthy
	StatusUnhealthy
	StatusStopping
	StatusStopped
	StatusProgressing
)

// ClassifyStatus maps an exact mirror_status value to a StatusClass.
func ClassifyStatus(status string) StatusClass {
	switch status {
	case "ACTIVE":
		return StatusHealthy
	case "PAUSED", "LINK_PAUSED", "FAILED", "LINK_FAILED", "SOURCE_UNAVAILABLE":
		return StatusUnhealthy
	case "PENDING_STOPPED":
		return StatusStopping
	case "STOPPED":
		return StatusStopped
	case "PENDING_MIRROR", "PENDING_SYNCHRONIZE", "PENDING_SETUP_FOR_RESTORE",
		"PENDING_RESTORE", "PENDING_RESTORE_MIRROR":
		return StatusProgressing
	}
	return StatusUnknown
}
