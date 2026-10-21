// Package confluent implements kafkarest.Client against the Confluent Kafka
// REST v3 API using only the standard library. The HTTP plumbing mirrors
// internal/mds/confluent. Reads are fail-closed: any error, unexpected status,
// malformed body, incomplete pagination, duplicate entry or missing required
// field is returned as an error, never as an empty or partial result.
package confluent

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/monedula-dev/monedula-gitops/internal/httpretry"
	"github.com/monedula-dev/monedula-gitops/internal/kafkarest"
)

// defaultTimeout bounds each HTTP request.
const defaultTimeout = 30 * time.Second

// maxPages bounds pagination so a misbehaving server cannot loop us forever.
const maxPages = 1000

// authType selects how the client authenticates.
type authType int

const (
	authNone   authType = iota
	authBasic           // Authorization: Basic base64(user:pass)
	authBearer          // Authorization: Bearer <token>
	// authMTLS: no header; the client cert on the http.Client is the credential.
)

// Auth holds resolved plaintext credentials. The caller resolves any secret
// references before constructing the client.
type Auth struct {
	Type     authType
	Username string // basic only
	Password string // basic only
	Token    string // bearer only
}

// BasicAuth returns an Auth for HTTP Basic authentication.
func BasicAuth(username, password string) *Auth {
	return &Auth{Type: authBasic, Username: username, Password: password}
}

// BearerAuth returns an Auth for Bearer token authentication.
func BearerAuth(token string) *Auth {
	return &Auth{Type: authBearer, Token: token}
}

// MTLSAuth returns an Auth that signals mTLS: no header is added; the TLS
// client certificate must already be configured on the *http.Client.
func MTLSAuth() *Auth {
	return &Auth{Type: authNone}
}

// Client talks to the Kafka REST v3 API over HTTP.
type Client struct {
	baseURL   string   // endpoint without trailing slash, without /v3
	base      *url.URL // parsed baseURL, used to validate pagination links
	clusterID string
	auth      *Auth
	http      *http.Client
}

var _ kafkarest.Client = (*Client)(nil)

// New returns a Client for the REST endpoint (a base URL WITHOUT /v3, e.g.
// https://broker:8090/kafka). One trailing "/" is trimmed. clusterID may be
// empty, in which case it is discovered on first use. httpClient, when
// non-nil, is used as-is; otherwise a default client with a 30 s timeout.
func New(endpoint, clusterID string, auth *Auth, httpClient *http.Client) (*Client, error) {
	endpoint = strings.TrimSuffix(endpoint, "/")
	if endpoint == "" {
		return nil, errors.New("kafkarest: endpoint is empty")
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("kafkarest: invalid endpoint: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, errors.New("kafkarest: endpoint must be an absolute URL")
	}
	if strings.HasSuffix(u.Path, "/v3") {
		return nil, errors.New("kafkarest: rest endpoint must not include /v3")
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultTimeout}
	}
	return &Client{baseURL: endpoint, base: u, clusterID: clusterID, auth: auth, http: httpClient}, nil
}

// get performs an idempotent GET of the absolute URL and decodes a 2xx JSON
// body into out. It returns the HTTP status code.
func (c *Client) get(ctx context.Context, fullURL string, out any) (int, error) {
	newReq := func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
		if err != nil {
			return nil, fmt.Errorf("kafkarest: build request: %w", err)
		}
		req.Header.Set("Accept", "application/json")
		c.applyAuth(req)
		return req, nil
	}
	resp, err := httpretry.Do(ctx, c.http, newReq, true)
	if err != nil {
		// Never wrap with credentials.
		return 0, fmt.Errorf("kafkarest: request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, fmt.Errorf("kafkarest: read response body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, statusError(resp.StatusCode, body)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return resp.StatusCode, fmt.Errorf("kafkarest: decode response: %w", err)
	}
	return resp.StatusCode, nil
}

// applyAuth adds the Authorization header, or nothing for mTLS.
func (c *Client) applyAuth(req *http.Request) {
	if c.auth == nil {
		return
	}
	switch c.auth.Type {
	case authBasic:
		raw := c.auth.Username + ":" + c.auth.Password
		req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(raw)))
	case authBearer:
		req.Header.Set("Authorization", "Bearer "+c.auth.Token)
	case authNone:
	}
}

// httpStatusError is a non-2xx response. Its message carries the status code
// and a bounded body snippet, never request headers.
type httpStatusError struct {
	status int
	msg    string
}

func (e *httpStatusError) Error() string { return e.msg }

// statusOf returns the HTTP status carried by err, or 0.
func statusOf(err error) int {
	var he *httpStatusError
	if errors.As(err, &he) {
		return he.status
	}
	return 0
}

func statusError(status int, body []byte) error {
	var env struct {
		Message string `json:"message"`
	}
	msg := fmt.Sprintf("kafkarest: returned status %d", status)
	if err := json.Unmarshal(body, &env); err == nil && env.Message != "" {
		msg += ": " + env.Message
	} else if snippet := strings.TrimSpace(string(body)); snippet != "" {
		if len(snippet) > 200 {
			snippet = snippet[:200] + "..."
		}
		msg += ": " + snippet
	}
	return &httpStatusError{status: status, msg: msg}
}

// esc URL-escapes a path segment.
func esc(s string) string { return url.PathEscape(s) }

// page is the REST v3 list envelope. Data is a pointer so a missing "data"
// field is distinguishable from an empty list.
type page[T any] struct {
	Metadata struct {
		Next *string `json:"next"`
	} `json:"metadata"`
	Data *[]T `json:"data"`
}

// checkNext validates a pagination link: same scheme and host as the
// endpoint, no userinfo, and a path under the endpoint's path.
func (c *Client) checkNext(next string) error {
	u, err := url.Parse(next)
	if err != nil {
		return errors.New("kafkarest: untrusted next link")
	}
	if u.Scheme != c.base.Scheme || u.Host != c.base.Host || u.User != nil {
		return errors.New("kafkarest: untrusted next link")
	}
	prefix := strings.TrimSuffix(c.base.Path, "/") + "/v3/"
	if !strings.HasPrefix(path.Clean(u.Path)+"/", prefix) {
		return errors.New("kafkarest: untrusted next link")
	}
	return nil
}

// listAll GETs path (relative to the endpoint) and follows metadata.next to
// completion, returning all entries in order.
func listAll[T any](ctx context.Context, c *Client, relPath string) ([]T, error) {
	var all []T
	seen := map[string]bool{}
	cur := c.baseURL + relPath
	for n := 0; ; n++ {
		if n >= maxPages {
			return nil, fmt.Errorf("kafkarest: more than %d pages", maxPages)
		}
		if seen[cur] {
			return nil, errors.New("kafkarest: pagination loop")
		}
		seen[cur] = true

		var p page[T]
		if _, err := c.get(ctx, cur, &p); err != nil {
			return nil, err
		}
		if p.Data == nil {
			return nil, errors.New("kafkarest: response is missing data")
		}
		all = append(all, *p.Data...)
		if p.Metadata.Next == nil || *p.Metadata.Next == "" {
			return all, nil
		}
		if err := c.checkNext(*p.Metadata.Next); err != nil {
			return nil, err
		}
		cur = *p.Metadata.Next
	}
}

// ClusterID returns the configured cluster ID without any request, or
// discovers it with GET /v3/clusters, requiring exactly one result. A
// discovered ID is cached; failures are not.
func (c *Client) ClusterID(ctx context.Context) (string, error) {
	if c.clusterID != "" {
		return c.clusterID, nil
	}
	clusters, err := listAll[struct {
		ClusterID string `json:"cluster_id"`
	}](ctx, c, "/v3/clusters")
	if err != nil {
		return "", fmt.Errorf("kafkarest.ClusterID: %w", err)
	}
	if len(clusters) != 1 {
		return "", fmt.Errorf("kafkarest.ClusterID: expected exactly 1 cluster, found %d", len(clusters))
	}
	if clusters[0].ClusterID == "" {
		return "", errors.New("kafkarest.ClusterID: cluster_id is empty")
	}
	c.clusterID = clusters[0].ClusterID
	return c.clusterID, nil
}

type linkJSON struct {
	LinkName  string `json:"link_name"`
	LinkState string `json:"link_state"`
	LinkError string `json:"link_error"`
}

// ListLinks returns all cluster links visible on the destination cluster.
func (c *Client) ListLinks(ctx context.Context) ([]kafkarest.Link, error) {
	id, err := c.ClusterID(ctx)
	if err != nil {
		return nil, err
	}
	raw, err := listAll[linkJSON](ctx, c, "/v3/clusters/"+esc(id)+"/links")
	if err != nil {
		return nil, fmt.Errorf("kafkarest.ListLinks: %w", err)
	}
	seen := map[string]bool{}
	out := make([]kafkarest.Link, 0, len(raw))
	for _, l := range raw {
		if l.LinkName == "" {
			return nil, errors.New("kafkarest.ListLinks: entry is missing link_name")
		}
		if seen[l.LinkName] {
			return nil, fmt.Errorf("kafkarest.ListLinks: duplicate link %q", l.LinkName)
		}
		seen[l.LinkName] = true
		out = append(out, kafkarest.Link{Name: l.LinkName, State: l.LinkState, Error: l.LinkError})
	}
	return out, nil
}

// LinkConfigs returns the link's configs (including defaults) as name -> value.
// A 404 yields kafkarest.ErrNotFound.
func (c *Client) LinkConfigs(ctx context.Context, link string) (map[string]string, error) {
	id, err := c.ClusterID(ctx)
	if err != nil {
		return nil, err
	}
	type cfg struct {
		Name  string  `json:"name"`
		Value *string `json:"value"`
	}
	raw, err := listAll[cfg](ctx, c, "/v3/clusters/"+esc(id)+"/links/"+esc(link)+"/configs")
	if err != nil {
		if statusOf(err) == http.StatusNotFound {
			return nil, fmt.Errorf("kafkarest.LinkConfigs %q: %w", link, kafkarest.ErrNotFound)
		}
		return nil, fmt.Errorf("kafkarest.LinkConfigs: %w", err)
	}
	out := make(map[string]string, len(raw))
	for _, e := range raw {
		if e.Name == "" {
			return nil, errors.New("kafkarest.LinkConfigs: entry is missing name")
		}
		if _, dup := out[e.Name]; dup {
			return nil, fmt.Errorf("kafkarest.LinkConfigs: duplicate config %q", e.Name)
		}
		v := ""
		if e.Value != nil {
			v = *e.Value
		}
		out[e.Name] = v
	}
	return out, nil
}

type mirrorJSON struct {
	LinkName        string `json:"link_name"`
	MirrorTopicName string `json:"mirror_topic_name"`
	SourceTopicName string `json:"source_topic_name"`
	MirrorStatus    string `json:"mirror_status"`
	NumPartitions   int    `json:"num_partitions"`
	StateTimeMs     int64  `json:"state_time_ms"`
}

// ListMirrors returns every mirror topic on the destination cluster across all
// links, following pagination to completion.
func (c *Client) ListMirrors(ctx context.Context) ([]kafkarest.Mirror, error) {
	id, err := c.ClusterID(ctx)
	if err != nil {
		return nil, err
	}
	raw, err := listAll[mirrorJSON](ctx, c, "/v3/clusters/"+esc(id)+"/links/-/mirrors")
	if err != nil {
		return nil, fmt.Errorf("kafkarest.ListMirrors: %w", err)
	}
	seen := map[string]bool{}
	out := make([]kafkarest.Mirror, 0, len(raw))
	for _, m := range raw {
		switch {
		case m.MirrorTopicName == "":
			return nil, errors.New("kafkarest.ListMirrors: entry is missing mirror_topic_name")
		case m.LinkName == "":
			return nil, fmt.Errorf("kafkarest.ListMirrors: mirror %q is missing link_name", m.MirrorTopicName)
		case m.MirrorStatus == "":
			return nil, fmt.Errorf("kafkarest.ListMirrors: mirror %q is missing mirror_status", m.MirrorTopicName)
		}
		if seen[m.MirrorTopicName] {
			return nil, fmt.Errorf("kafkarest.ListMirrors: duplicate mirror_topic_name %q", m.MirrorTopicName)
		}
		seen[m.MirrorTopicName] = true
		out = append(out, kafkarest.Mirror{
			LinkName:        m.LinkName,
			MirrorTopicName: m.MirrorTopicName,
			SourceTopicName: m.SourceTopicName,
			Status:          m.MirrorStatus,
			NumPartitions:   m.NumPartitions,
			StateTimeMs:     m.StateTimeMs,
		})
	}
	return out, nil
}

type nameValue struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// sortedConfigs returns set as a deterministic name-sorted list.
func sortedConfigs(set map[string]string) []nameValue {
	out := make([]nameValue, 0, len(set))
	for k, v := range set {
		out = append(out, nameValue{Name: k, Value: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// post sends a JSON POST exactly once (never retried) and treats any 2xx as
// success. The response body is discarded.
func (c *Client) post(ctx context.Context, fullURL string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("kafkarest: encode request: %w", err)
	}
	newReq := func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, fullURL, bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("kafkarest: build request: %w", err)
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Content-Type", "application/json")
		c.applyAuth(req)
		return req, nil
	}
	resp, err := httpretry.Do(ctx, c.http, newReq, false)
	if err != nil {
		return fmt.Errorf("kafkarest: request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("kafkarest: read response body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return statusError(resp.StatusCode, body)
	}
	return nil
}

// CreateMirror creates a mirror topic on a link. replication_factor and
// configs are omitted when not declared. Sent exactly once.
func (c *Client) CreateMirror(ctx context.Context, r kafkarest.CreateMirrorRequest) error {
	id, err := c.ClusterID(ctx)
	if err != nil {
		return err
	}
	body := struct {
		SourceTopicName   string      `json:"source_topic_name"`
		MirrorTopicName   string      `json:"mirror_topic_name"`
		ReplicationFactor int         `json:"replication_factor,omitempty"`
		Configs           []nameValue `json:"configs,omitempty"`
	}{r.SourceTopicName, r.MirrorTopicName, r.ReplicationFactor, sortedConfigs(r.Configs)}
	if err := c.post(ctx, c.baseURL+"/v3/clusters/"+esc(id)+"/links/"+esc(r.LinkName)+"/mirrors", body); err != nil {
		return fmt.Errorf("kafkarest.CreateMirror: %w", err)
	}
	return nil
}

// AlterTopicConfigs sets the given topic configs. Sent exactly once.
func (c *Client) AlterTopicConfigs(ctx context.Context, topic string, set map[string]string) error {
	id, err := c.ClusterID(ctx)
	if err != nil {
		return err
	}
	body := struct {
		Data []nameValue `json:"data"`
	}{sortedConfigs(set)}
	if err := c.post(ctx, c.baseURL+"/v3/clusters/"+esc(id)+"/topics/"+esc(topic)+"/configs:alter", body); err != nil {
		return fmt.Errorf("kafkarest.AlterTopicConfigs: %w", err)
	}
	return nil
}
