package confluent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/monedula-dev/monedula-gitops/internal/httpretry"
	"github.com/monedula-dev/monedula-gitops/internal/kafkarest"
)

func TestClusterIDConcurrentDiscovery(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		time.Sleep(20 * time.Millisecond)
		writeJSON(w, map[string]any{"data": []any{map[string]any{"cluster_id": "abc"}}})
	}))
	defer srv.Close()
	c := mustNew(t, srv.URL, "", nil)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := c.ClusterID(t.Context())
			if err != nil || id != "abc" {
				t.Errorf("ClusterID = %q, %v", id, err)
			}
		}()
	}
	wg.Wait()
	if n := hits.Load(); n != 1 {
		t.Fatalf("GET /v3/clusters count = %d, want 1", n)
	}
}

func TestRedirectIsAnError(t *testing.T) {
	var otherHit atomic.Int64
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		otherHit.Add(1)
		writeJSON(w, map[string]any{"data": []any{}})
	}))
	defer other.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+r.URL.Path, http.StatusFound)
	}))
	defer srv.Close()

	c := mustNew(t, srv.URL, "c1", nil)
	_, err := c.ListMirrors(t.Context())
	if err == nil || !strings.Contains(err.Error(), "302") {
		t.Fatalf("err = %v, want error containing 302", err)
	}
	if otherHit.Load() != 0 {
		t.Fatal("redirect target was contacted")
	}
}

func TestWithoutRetrySingleAttempt(t *testing.T) {
	old := httpretry.Sleep
	httpretry.Sleep = func(context.Context, time.Duration) error { return nil }
	defer func() { httpretry.Sleep = old }()

	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	c := mustNew(t, srv.URL, "c1", nil)

	if _, err := c.ListMirrors(kafkarest.WithoutRetry(t.Context())); err == nil {
		t.Fatal("expected error")
	}
	if n := hits.Load(); n != 1 {
		t.Fatalf("WithoutRetry requests = %d, want 1", n)
	}

	hits.Store(0)
	if _, err := c.ListMirrors(t.Context()); err == nil {
		t.Fatal("expected error")
	}
	if n := hits.Load(); n <= 1 {
		t.Fatalf("plain ctx requests = %d, want > 1", n)
	}
}
