package confluent

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/kafkarest"
)

func mustNew(t *testing.T, endpoint, cluster string, auth *Auth) *Client {
	t.Helper()
	c, err := New(endpoint, cluster, auth, nil)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func mirror(name string) map[string]any {
	return map[string]any{
		"link_name": "l1", "mirror_topic_name": name, "source_topic_name": name,
		"mirror_status": "ACTIVE", "num_partitions": 3, "state_time_ms": 1234,
	}
}

func TestNewRejectsV3Suffix(t *testing.T) {
	_, err := New("https://h:8090/kafka/v3", "c1", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "/v3") {
		t.Fatalf("err = %v", err)
	}
	if _, err := New("https://h:8090/kafka/v3/", "c1", nil, nil); err == nil {
		t.Fatal("expected error with trailing slash")
	}
}

func TestEndpointTrailingSlashJoinsOnce(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.URL.Path
		writeJSON(w, map[string]any{"data": []any{}})
	}))
	defer srv.Close()
	c := mustNew(t, srv.URL+"/kafka/", "c1", nil)
	if _, err := c.ListLinks(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got != "/kafka/v3/clusters/c1/links" {
		t.Fatalf("path = %q", got)
	}
}

func TestClusterIDConfiguredMakesNoRequest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request %s", r.URL)
	}))
	defer srv.Close()
	id, err := mustNew(t, srv.URL, "c1", nil).ClusterID(t.Context())
	if err != nil || id != "c1" {
		t.Fatalf("id=%q err=%v", id, err)
	}
}

func TestClusterIDDiscoveryRequiresExactlyOne(t *testing.T) {
	for _, tc := range []struct {
		ids     []string
		wantErr string
	}{{nil, "0"}, {[]string{"a", "b"}, "2"}, {[]string{"a"}, ""}} {
		calls := 0
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls++
			if r.URL.Path != "/v3/clusters" {
				t.Errorf("path %s", r.URL.Path)
			}
			data := []any{}
			for _, id := range tc.ids {
				data = append(data, map[string]any{"cluster_id": id})
			}
			writeJSON(w, map[string]any{"data": data})
		}))
		c := mustNew(t, srv.URL, "", nil)
		id, err := c.ClusterID(t.Context())
		if tc.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ids=%v err=%v", tc.ids, err)
			}
			// not cached on error
			_, _ = c.ClusterID(t.Context())
			if calls != 2 {
				t.Fatalf("calls = %d, want 2 (error not cached)", calls)
			}
		} else {
			if err != nil || id != "a" {
				t.Fatalf("id=%q err=%v", id, err)
			}
			_, _ = c.ClusterID(t.Context())
			if calls != 1 {
				t.Fatalf("calls = %d, want 1 (cached)", calls)
			}
		}
		srv.Close()
	}
}

func TestListMirrorsFollowsNext(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v3/clusters/c1/links/-/mirrors" {
			t.Errorf("path %s", r.URL.Path)
		}
		switch r.URL.Query().Get("page") {
		case "":
			writeJSON(w, map[string]any{"metadata": map[string]any{"next": srv.URL + "/v3/clusters/c1/links/-/mirrors?page=2"},
				"data": []any{mirror("t1"), mirror("t2")}})
		case "2":
			writeJSON(w, map[string]any{"metadata": map[string]any{"next": srv.URL + "/v3/clusters/c1/links/-/mirrors?page=3"},
				"data": []any{mirror("t3")}})
		default:
			writeJSON(w, map[string]any{"metadata": map[string]any{"next": nil},
				"data": []any{mirror("t4"), mirror("t5")}})
		}
	}))
	defer srv.Close()
	ms, err := mustNew(t, srv.URL, "c1", nil).ListMirrors(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 5 {
		t.Fatalf("len = %d", len(ms))
	}
	for i, m := range ms {
		if m.MirrorTopicName != fmt.Sprintf("t%d", i+1) {
			t.Fatalf("order: %v", ms)
		}
	}
	if ms[0].LinkName != "l1" || ms[0].Status != "ACTIVE" || ms[0].NumPartitions != 3 || ms[0].StateTimeMs != 1234 {
		t.Fatalf("fields: %+v", ms[0])
	}
}

func TestListMirrorsRejectsForeignNext(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"metadata": map[string]any{"next": "http://evil.example.com/v3/clusters/c1/links/-/mirrors?page=2"},
			"data": []any{mirror("t1")}})
	}))
	defer srv.Close()
	_, err := mustNew(t, srv.URL, "c1", nil).ListMirrors(t.Context())
	if err == nil || !strings.Contains(err.Error(), "untrusted next link") {
		t.Fatalf("err = %v", err)
	}
}

func TestListMirrorsRejectsNextOutsideEndpointPath(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"metadata": map[string]any{"next": srv.URL + "/other/v3/x"},
			"data": []any{mirror("t1")}})
	}))
	defer srv.Close()
	_, err := mustNew(t, srv.URL+"/kafka", "c1", nil).ListMirrors(t.Context())
	if err == nil || !strings.Contains(err.Error(), "untrusted next link") {
		t.Fatalf("err = %v", err)
	}
}

func TestListMirrorsRejectsLoop(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next := srv.URL + "/v3/clusters/c1/links/-/mirrors"
		next += "?page=2"
		name := "t" + r.URL.Query().Get("page")
		writeJSON(w, map[string]any{"metadata": map[string]any{"next": next}, "data": []any{mirror(name)}})
	}))
	defer srv.Close()
	_, err := mustNew(t, srv.URL, "c1", nil).ListMirrors(t.Context())
	if err == nil || !strings.Contains(err.Error(), "pagination loop") {
		t.Fatalf("err = %v", err)
	}
}

func TestListMirrorsRejectsDuplicateName(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"data": []any{mirror("t1"), mirror("t1")}})
	}))
	defer srv.Close()
	_, err := mustNew(t, srv.URL, "c1", nil).ListMirrors(t.Context())
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("err = %v", err)
	}
}

func TestListMirrorsMissingFieldIsError(t *testing.T) {
	for _, field := range []string{"mirror_topic_name", "link_name", "mirror_status"} {
		m := mirror("t1")
		delete(m, field)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, map[string]any{"data": []any{m}})
		}))
		ms, err := mustNew(t, srv.URL, "c1", nil).ListMirrors(t.Context())
		srv.Close()
		if err == nil || ms != nil {
			t.Fatalf("%s: ms=%v err=%v", field, ms, err)
		}
	}
}

func TestListMirrors403IsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error_code":403,"message":"forbidden"}`, http.StatusForbidden)
	}))
	defer srv.Close()
	ms, err := mustNew(t, srv.URL, "c1", nil).ListMirrors(t.Context())
	if err == nil || !strings.Contains(err.Error(), "403") || ms != nil {
		t.Fatalf("ms=%v err=%v", ms, err)
	}
}

func TestListMirrorsMalformedBodyIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("not json"))
	}))
	defer srv.Close()
	if _, err := mustNew(t, srv.URL, "c1", nil).ListMirrors(t.Context()); err == nil {
		t.Fatal("expected error")
	}
}

func TestListLinks(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"data": []any{
			map[string]any{"link_name": "l1", "link_state": "ACTIVE"},
			map[string]any{"link_name": "l2", "link_state": "PAUSED", "link_error": "NO_ERROR"},
		}})
	}))
	defer srv.Close()
	ls, err := mustNew(t, srv.URL, "c1", nil).ListLinks(t.Context())
	if err != nil || len(ls) != 2 || ls[1].Name != "l2" || ls[1].State != "PAUSED" || ls[1].Error != "NO_ERROR" {
		t.Fatalf("ls=%v err=%v", ls, err)
	}
}

func TestListLinksRejectsDuplicateAndMissing(t *testing.T) {
	for _, data := range [][]any{
		{map[string]any{"link_name": "l1", "link_state": "ACTIVE"}, map[string]any{"link_name": "l1", "link_state": "ACTIVE"}},
		{map[string]any{"link_state": "ACTIVE"}},
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, map[string]any{"data": data})
		}))
		ls, err := mustNew(t, srv.URL, "c1", nil).ListLinks(t.Context())
		srv.Close()
		if err == nil || ls != nil {
			t.Fatalf("ls=%v err=%v", ls, err)
		}
	}
}

func TestLinkConfigs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v3/clusters/c1/links/l1/configs" {
			t.Errorf("path %s", r.URL.Path)
		}
		writeJSON(w, map[string]any{"data": []any{
			map[string]any{"name": "a", "value": "1"},
			map[string]any{"name": "b", "value": "2"},
		}})
	}))
	defer srv.Close()
	m, err := mustNew(t, srv.URL, "c1", nil).LinkConfigs(t.Context(), "l1")
	if err != nil || len(m) != 2 || m["a"] != "1" || m["b"] != "2" {
		t.Fatalf("m=%v err=%v", m, err)
	}
}

func TestLinkConfigs404IsErrNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()
	_, err := mustNew(t, srv.URL, "c1", nil).LinkConfigs(t.Context(), "l1")
	if !errors.Is(err, kafkarest.ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func TestBasicAuthHeaderNotInErrors(t *testing.T) {
	var header string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header = r.Header.Get("Authorization")
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	_, err := mustNew(t, srv.URL, "c1", BasicAuth("user", "pw")).ListMirrors(t.Context())
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("err = %v", err)
	}
	if !strings.HasPrefix(header, "Basic ") {
		t.Fatalf("header = %q", header)
	}
	if strings.Contains(err.Error(), strings.TrimPrefix(header, "Basic ")) {
		t.Fatal("credentials leaked in error")
	}
}

func TestPathEscaping(t *testing.T) {
	var raw string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw = r.URL.EscapedPath()
		writeJSON(w, map[string]any{"data": []any{}})
	}))
	defer srv.Close()
	if _, err := mustNew(t, srv.URL, "c1", nil).LinkConfigs(t.Context(), "a b"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(raw, "a%20b") {
		t.Fatalf("raw path = %q", raw)
	}
}
