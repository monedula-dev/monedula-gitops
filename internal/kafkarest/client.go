// Package kafkarest defines the seam to the Kafka REST v3 API used for
// Confluent cluster linking and mirror topics.
package kafkarest

import (
	"context"
	"errors"
	"fmt"
)

type retryDisabledKey struct{}

// WithoutRetry returns a context that tells the client to make a single attempt
// per request: no retries, no backoff, no Retry-After sleep.
func WithoutRetry(ctx context.Context) context.Context {
	return context.WithValue(ctx, retryDisabledKey{}, true)
}

// RetryDisabled reports whether ctx was derived with WithoutRetry.
func RetryDisabled(ctx context.Context) bool {
	v, _ := ctx.Value(retryDisabledKey{}).(bool)
	return v
}

// StatusError is a non-2xx REST response. Code is the HTTP status and ErrorCode
// is the API's own error_code when the body carries one (otherwise 0).
type StatusError struct {
	Code      int
	ErrorCode int
	Message   string
}

func (e *StatusError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("kafkarest: returned status %d", e.Code)
	}
	return fmt.Sprintf("kafkarest: returned status %d: %s", e.Code, e.Message)
}

// ErrNotFound is returned when the REST API reports 404 for a requested object.
var ErrNotFound = errors.New("kafkarest: not found")

// Link is a cluster link as seen from the destination cluster.
type Link struct {
	Name  string // link_name
	State string // link_state
	Error string // link_error, as reported (may be empty or "NO_ERROR")
}

// Mirror is a mirror topic on the destination cluster.
type Mirror struct {
	LinkName        string // link_name
	MirrorTopicName string // mirror_topic_name
	SourceTopicName string // source_topic_name
	Status          string // mirror_status
	NumPartitions   int    // num_partitions
	StateTimeMs     int64  // state_time_ms
}

// CreateMirrorRequest describes a mirror topic to create.
type CreateMirrorRequest struct {
	LinkName          string
	SourceTopicName   string
	MirrorTopicName   string
	ReplicationFactor int // 0 means omitted
	Configs           map[string]string
}

// Client is the Kafka REST v3 seam. Read methods are fail-closed: any error,
// malformed body or incomplete pagination is returned as an error, never as an
// empty or partial result.
type Client interface {
	ClusterID(ctx context.Context) (string, error)
	ListLinks(ctx context.Context) ([]Link, error)
	LinkConfigs(ctx context.Context, link string) (map[string]string, error)
	ListMirrors(ctx context.Context) ([]Mirror, error)
	CreateMirror(ctx context.Context, r CreateMirrorRequest) error
	AlterTopicConfigs(ctx context.Context, topic string, set map[string]string) error
}
