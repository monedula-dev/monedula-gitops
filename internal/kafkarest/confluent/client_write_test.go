package confluent

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/kafkarest"
)

func captureServer(t *testing.T, status int) (*httptest.Server, *map[string]any, *string) {
	t.Helper()
	var body map[string]any
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s", r.Method)
		}
		gotPath = r.URL.EscapedPath()
		b, _ := io.ReadAll(r.Body)
		body = nil
		if err := json.Unmarshal(b, &body); err != nil {
			t.Errorf("body: %v", err)
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv, &body, &gotPath
}

func TestCreateMirrorBody(t *testing.T) {
	srv, body, p := captureServer(t, http.StatusCreated)
	c := mustNew(t, srv.URL, "c1", nil)
	err := c.CreateMirror(t.Context(), kafkarest.CreateMirrorRequest{
		LinkName: "l1", SourceTopicName: "payments.orders", MirrorTopicName: "dr.payments.orders",
		ReplicationFactor: 3, Configs: map[string]string{"min.insync.replicas": "2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if *p != "/v3/clusters/c1/links/l1/mirrors" {
		t.Fatalf("path = %q", *p)
	}
	want := map[string]any{
		"source_topic_name": "payments.orders", "mirror_topic_name": "dr.payments.orders",
		"replication_factor": float64(3),
		"configs":            []any{map[string]any{"name": "min.insync.replicas", "value": "2"}},
	}
	if !reflect.DeepEqual(*body, want) {
		t.Fatalf("body = %v", *body)
	}
}

func TestCreateMirrorOmitsOptionalFields(t *testing.T) {
	srv, body, _ := captureServer(t, http.StatusOK)
	c := mustNew(t, srv.URL, "c1", nil)
	if err := c.CreateMirror(t.Context(), kafkarest.CreateMirrorRequest{LinkName: "l1", SourceTopicName: "a", MirrorTopicName: "a"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := (*body)["replication_factor"]; ok {
		t.Fatal("replication_factor present")
	}
	if _, ok := (*body)["configs"]; ok {
		t.Fatal("configs present")
	}
}

func TestCreateMirrorConfigsSortedByName(t *testing.T) {
	srv, body, _ := captureServer(t, http.StatusNoContent)
	c := mustNew(t, srv.URL, "c1", nil)
	err := c.CreateMirror(t.Context(), kafkarest.CreateMirrorRequest{
		LinkName: "l1", SourceTopicName: "a", MirrorTopicName: "a",
		Configs: map[string]string{"z": "1", "a": "2", "m": "3"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range (*body)["configs"].([]any) {
		names = append(names, e.(map[string]any)["name"].(string))
	}
	if !reflect.DeepEqual(names, []string{"a", "m", "z"}) {
		t.Fatalf("names = %v", names)
	}
}

func TestCreateMirrorNotRetriedOn503(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	c := mustNew(t, srv.URL, "c1", nil)
	err := c.CreateMirror(t.Context(), kafkarest.CreateMirrorRequest{LinkName: "l1", SourceTopicName: "a", MirrorTopicName: "a"})
	if err == nil {
		t.Fatal("expected error")
	}
	if n.Load() != 1 {
		t.Fatalf("requests = %d", n.Load())
	}
}

func TestAlterTopicConfigsBody(t *testing.T) {
	srv, body, p := captureServer(t, http.StatusNoContent)
	c := mustNew(t, srv.URL, "c1", nil)
	if err := c.AlterTopicConfigs(t.Context(), "dr/t", map[string]string{"min.insync.replicas": "2"}); err != nil {
		t.Fatal(err)
	}
	if *p != "/v3/clusters/c1/topics/dr%2Ft/configs:alter" {
		t.Fatalf("path = %q", *p)
	}
	want := map[string]any{"data": []any{map[string]any{"name": "min.insync.replicas", "value": "2"}}}
	if !reflect.DeepEqual(*body, want) {
		t.Fatalf("body = %v", *body)
	}
}

func TestAlterTopicConfigsNotRetriedOn503(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	c := mustNew(t, srv.URL, "c1", nil)
	if err := c.AlterTopicConfigs(t.Context(), "t", map[string]string{"a": "b"}); err == nil || n.Load() != 1 {
		t.Fatalf("err=%v requests=%d", err, n.Load())
	}
}
