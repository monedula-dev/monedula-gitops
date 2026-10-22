package controller

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

func TestRESTForRotatesSecret(t *testing.T) {
	var mu sync.Mutex
	var gotUser, gotPass string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, _ := r.BasicAuth()
		mu.Lock()
		gotUser, gotPass = u, p
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer srv.Close()

	s := newScheme(t)
	_ = corev1.AddToScheme(s)
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "rest", Namespace: "ns1"},
		Data:       map[string][]byte{"password": []byte("a")},
	}
	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(sec).Build()

	c := clusterObj()
	user := v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Inline: "svc"}}
	c.Spec.REST = &v1alpha1.KafkaRESTConfig{
		Endpoint:  srv.URL,
		ClusterID: "c1",
		Auth: &v1alpha1.MDSAuth{
			Type:     "basic",
			Username: &user,
			Password: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{
				SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "rest", Key: "password"},
			}},
		},
	}

	f := &DefaultClientFactory{Client: cl}
	call := func(want string) {
		t.Helper()
		rc, err := f.RESTFor(t.Context(), c)
		if err != nil || rc == nil {
			t.Fatalf("RESTFor = %v, %v", rc, err)
		}
		if _, err := rc.ListMirrors(t.Context()); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		defer mu.Unlock()
		if gotUser != "svc" || gotPass != want {
			t.Fatalf("server saw %q:%q, want svc:%q", gotUser, gotPass, want)
		}
	}
	call("a")

	sec.Data["password"] = []byte("b")
	if err := cl.Update(t.Context(), sec); err != nil {
		t.Fatal(err)
	}
	call("b")
}

func TestRESTForNilWithoutRest(t *testing.T) {
	f := &DefaultClientFactory{Client: fake.NewClientBuilder().WithScheme(newScheme(t)).Build()}
	rc, err := f.RESTFor(t.Context(), clusterObj())
	if rc != nil || err != nil {
		t.Fatalf("RESTFor = %v, %v; want nil, nil", rc, err)
	}
}

// TestKafkaForSkipsSchemaRegistry: KafkaFor builds only the Kafka client, so
// a Schema Registry credential that cannot be resolved fails For but not
// KafkaFor.
func TestKafkaForSkipsSchemaRegistry(t *testing.T) {
	f := &DefaultClientFactory{Client: fake.NewClientBuilder().WithScheme(newScheme(t)).Build()}
	c := clusterObj()
	c.Spec.BootstrapServers = "localhost:9092"
	c.Spec.SchemaRegistry = &v1alpha1.SchemaRegistryConf{
		Endpoint: "http://localhost:8081",
		Auth: &v1alpha1.SchemaRegistryAuth{
			Type:     "basic",
			Username: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Inline: "u"}},
			Password: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{
				SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "missing", Key: "password"},
			}},
		},
	}
	if _, _, cleanup, err := f.For(t.Context(), c); err == nil {
		cleanup()
		t.Fatal("For must fail on the unresolvable Schema Registry secret")
	}
	k, cleanup, err := f.KafkaFor(t.Context(), c)
	if err != nil || k == nil || cleanup == nil {
		t.Fatalf("KafkaFor = %v, %v; want a Kafka client and nil error", k, err)
	}
	cleanup()
}
