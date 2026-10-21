// Package mock provides an in-memory kafkarest.Client seeded from a YAML/JSON
// state fixture, so the CLI can exercise mirror-topic flows without a live
// Confluent cluster. Reads are deterministically ordered for stable goldens.
package mock

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"

	"sigs.k8s.io/yaml"

	"github.com/monedula-dev/monedula-gitops/internal/kafkarest"
)

var _ kafkarest.Client = (*Client)(nil)

// Client is an in-memory kafkarest.Client. Failures injected with Fail are
// keyed by method name and returned before any state is read or mutated.
type Client struct {
	clusterID string
	links     []kafkarest.Link
	configs   map[string]map[string]string
	mirrors   []kafkarest.Mirror
	failures  map[string]error
	created   []kafkarest.CreateMirrorRequest
	altered   map[string]map[string]string
}

type fileState struct {
	ClusterID string        `json:"clusterId"`
	Links     []fileLink    `json:"links"`
	Mirrors   []fileMirror  `json:"mirrors"`
	Failures  []fileFailure `json:"failures"`
}

type fileLink struct {
	Name    string            `json:"name"`
	State   string            `json:"state"`
	Error   string            `json:"error"`
	Configs map[string]string `json:"configs"`
}

type fileMirror struct {
	LinkName        string `json:"linkName"`
	MirrorTopicName string `json:"mirrorTopicName"`
	SourceTopicName string `json:"sourceTopicName"`
	Status          string `json:"status"`
	NumPartitions   int    `json:"numPartitions"`
}

type fileFailure struct {
	Method string `json:"method"`
	Error  string `json:"error"`
}

// New constructs an empty Client with the given cluster ID. An empty ID makes
// ClusterID return an error (fail-closed).
func New(clusterID string) *Client {
	return &Client{clusterID: clusterID, configs: map[string]map[string]string{}}
}

// FromFile loads a Client from a YAML or JSON state file.
func FromFile(path string) (*Client, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read rest state file %q: %w", path, err)
	}
	var fs fileState
	if err := yaml.Unmarshal(data, &fs); err != nil {
		return nil, fmt.Errorf("parse rest state file %q: %w", path, err)
	}
	c := New(fs.ClusterID)
	for _, l := range fs.Links {
		c.AddLink(kafkarest.Link{Name: l.Name, State: l.State, Error: l.Error}, l.Configs)
	}
	for _, m := range fs.Mirrors {
		c.AddMirror(kafkarest.Mirror{
			LinkName:        m.LinkName,
			MirrorTopicName: m.MirrorTopicName,
			SourceTopicName: m.SourceTopicName,
			Status:          m.Status,
			NumPartitions:   m.NumPartitions,
		})
	}
	for _, f := range fs.Failures {
		if f.Method == "" || f.Error == "" {
			return nil, fmt.Errorf("rest state file %q: a failures entry needs both method and error", path)
		}
		c.Fail(f.Method, errors.New(f.Error))
	}
	return c, nil
}

// AddLink registers a link and its configs (copied).
func (c *Client) AddLink(l kafkarest.Link, configs map[string]string) {
	c.links = append(c.links, l)
	if c.configs == nil {
		c.configs = map[string]map[string]string{}
	}
	c.configs[l.Name] = copyMap(configs)
}

// AddMirror registers an existing mirror.
func (c *Client) AddMirror(m kafkarest.Mirror) { c.mirrors = append(c.mirrors, m) }

// Fail makes every call to method (e.g. "ListMirrors") return err.
func (c *Client) Fail(method string, err error) {
	if c.failures == nil {
		c.failures = map[string]error{}
	}
	c.failures[method] = err
}

// Created returns the recorded successful CreateMirror requests.
func (c *Client) Created() []kafkarest.CreateMirrorRequest {
	out := make([]kafkarest.CreateMirrorRequest, len(c.created))
	for i, r := range c.created {
		out[i] = r
		out[i].Configs = copyMap(r.Configs)
	}
	return out
}

// Altered returns the config keys set per topic via AlterTopicConfigs.
func (c *Client) Altered() map[string]map[string]string {
	out := make(map[string]map[string]string, len(c.altered))
	for k, v := range c.altered {
		out[k] = copyMap(v)
	}
	return out
}

// ClusterID returns the configured cluster ID, or an error when unset.
func (c *Client) ClusterID(_ context.Context) (string, error) {
	if err := c.failures["ClusterID"]; err != nil {
		return "", err
	}
	if c.clusterID == "" {
		return "", errors.New("mock: rest cluster id not set")
	}
	return c.clusterID, nil
}

// ListLinks returns the links sorted by name.
func (c *Client) ListLinks(_ context.Context) ([]kafkarest.Link, error) {
	if err := c.failures["ListLinks"]; err != nil {
		return nil, err
	}
	out := make([]kafkarest.Link, len(c.links))
	copy(out, c.links)
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// LinkConfigs returns a copy of the link's configs; an unknown link is
// kafkarest.ErrNotFound.
func (c *Client) LinkConfigs(_ context.Context, link string) (map[string]string, error) {
	if err := c.failures["LinkConfigs"]; err != nil {
		return nil, err
	}
	cfg, ok := c.configs[link]
	if !ok {
		return nil, fmt.Errorf("link %q: %w", link, kafkarest.ErrNotFound)
	}
	return copyMap(cfg), nil
}

// ListMirrors returns the mirrors sorted by link then mirror topic name.
func (c *Client) ListMirrors(_ context.Context) ([]kafkarest.Mirror, error) {
	if err := c.failures["ListMirrors"]; err != nil {
		return nil, err
	}
	out := make([]kafkarest.Mirror, len(c.mirrors))
	copy(out, c.mirrors)
	sort.Slice(out, func(i, j int) bool {
		if out[i].LinkName != out[j].LinkName {
			return out[i].LinkName < out[j].LinkName
		}
		return out[i].MirrorTopicName < out[j].MirrorTopicName
	})
	return out, nil
}

// CreateMirror records the request and adds an ACTIVE mirror. An injected
// failure is returned without recording or mutating.
func (c *Client) CreateMirror(_ context.Context, r kafkarest.CreateMirrorRequest) error {
	if err := c.failures["CreateMirror"]; err != nil {
		return err
	}
	r.Configs = copyMap(r.Configs)
	c.created = append(c.created, r)
	c.mirrors = append(c.mirrors, kafkarest.Mirror{
		LinkName:        r.LinkName,
		MirrorTopicName: r.MirrorTopicName,
		SourceTopicName: r.SourceTopicName,
		Status:          "ACTIVE",
	})
	return nil
}

// AlterTopicConfigs records (merges) the set keys for topic.
func (c *Client) AlterTopicConfigs(_ context.Context, topic string, set map[string]string) error {
	if err := c.failures["AlterTopicConfigs"]; err != nil {
		return err
	}
	if c.altered == nil {
		c.altered = map[string]map[string]string{}
	}
	if c.altered[topic] == nil {
		c.altered[topic] = map[string]string{}
	}
	for k, v := range set {
		c.altered[topic][k] = v
	}
	return nil
}

func copyMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	cp := make(map[string]string, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}
