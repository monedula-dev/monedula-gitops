package controller

import (
	"reflect"
	"sort"
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

// vfSecret builds a *ValueFrom whose source is a secretKeyRef with the given name.
func vfSecret(name string) *v1alpha1.ValueFrom {
	return &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{
		SecretKeyRef: &v1alpha1.SecretKeyRef{Name: name, Key: "k"},
	}}
}

// vfSecretVal is the value (non-pointer) form for fields typed ValueFrom.
func vfSecretVal(name string) v1alpha1.ValueFrom {
	return *vfSecret(name)
}

// vfConfigMap builds a *ValueFrom sourced from a configMapKeyRef (no Secret).
func vfConfigMap(name string) *v1alpha1.ValueFrom {
	return &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{
		ConfigMapKeyRef: &v1alpha1.SecretKeyRef{Name: name, Key: "k"},
	}}
}

func TestClusterSecretNames(t *testing.T) {
	tests := []struct {
		name    string
		cluster *v1alpha1.KafkaCluster
		want    []string
	}{
		{
			name:    "nil cluster",
			cluster: nil,
			want:    nil,
		},
		{
			name:    "no secret-bearing fields",
			cluster: &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{BootstrapServers: "localhost:9092"}},
			want:    nil,
		},
		{
			name: "tls caCert + client cert/key",
			cluster: &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
				TLS: &v1alpha1.TLSConfig{
					Enabled:    true,
					CACert:     vfSecret("ca"),
					ClientCert: vfSecret("clientcert"),
					ClientKey:  vfSecret("clientkey"),
				},
			}},
			want: []string{"ca", "clientcert", "clientkey"},
		},
		{
			name: "scram + oauth auth",
			cluster: &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
				Auth: &v1alpha1.AuthConfig{
					Mechanism: "SCRAM-SHA-512",
					SCRAM:     &v1alpha1.SCRAMAuth{Username: vfSecretVal("scram-u"), Password: vfSecretVal("scram-p")},
					OAuth:     &v1alpha1.OAuthConfig{ClientID: vfSecretVal("oauth-id"), ClientSecret: vfSecretVal("oauth-secret")},
				},
			}},
			want: []string{"oauth-id", "oauth-secret", "scram-p", "scram-u"},
		},
		{
			name: "schema registry auth",
			cluster: &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
				SchemaRegistry: &v1alpha1.SchemaRegistryConf{
					Endpoint: "http://sr",
					Auth:     &v1alpha1.SchemaRegistryAuth{Type: "basic", Username: vfSecretVal("sr-u"), Password: vfSecretVal("sr-p")},
				},
			}},
			want: []string{"sr-p", "sr-u"},
		},
		{
			name: "schema registry tls",
			cluster: &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
				SchemaRegistry: &v1alpha1.SchemaRegistryConf{
					Endpoint: "https://sr",
					TLS: &v1alpha1.TLSConfig{
						Enabled:    true,
						CACert:     vfSecret("sr-ca"),
						ClientCert: vfSecret("sr-clientcert"),
						ClientKey:  vfSecret("sr-clientkey"),
					},
				},
			}},
			want: []string{"sr-ca", "sr-clientcert", "sr-clientkey"},
		},
		{
			name: "schema registry auth + tls",
			cluster: &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
				SchemaRegistry: &v1alpha1.SchemaRegistryConf{
					Endpoint: "https://sr",
					Auth:     &v1alpha1.SchemaRegistryAuth{Type: "basic", Username: vfSecretVal("sr-u"), Password: vfSecretVal("sr-p")},
					TLS:      &v1alpha1.TLSConfig{Enabled: true, CACert: vfSecret("sr-ca")},
				},
			}},
			want: []string{"sr-ca", "sr-p", "sr-u"},
		},
		{
			name: "mds auth + mds tls",
			cluster: &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
				Authorization: &v1alpha1.AuthorizationConfig{
					MDS: &v1alpha1.MDSConfig{
						Endpoint: "http://mds",
						Auth:     &v1alpha1.MDSAuth{Type: "basic", Username: vfSecret("mds-u"), Password: vfSecret("mds-p")},
						TLS:      &v1alpha1.TLSConfig{CACert: vfSecret("mds-ca")},
					},
				},
			}},
			want: []string{"mds-ca", "mds-p", "mds-u"},
		},
		{
			name: "mds bearer token",
			cluster: &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
				Authorization: &v1alpha1.AuthorizationConfig{
					MDS: &v1alpha1.MDSConfig{
						Endpoint: "http://mds",
						Auth:     &v1alpha1.MDSAuth{Type: "bearer", Token: vfSecret("mds-token")},
					},
				},
			}},
			want: []string{"mds-token"},
		},
		{
			name: "oauth tokenEndpointCA",
			cluster: &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
				Auth: &v1alpha1.AuthConfig{
					Mechanism: "OAUTHBEARER",
					OAuth: &v1alpha1.OAuthConfig{
						ClientID:        vfSecretVal("oauth-id"),
						ClientSecret:    vfSecretVal("oauth-secret"),
						TokenEndpointCA: vfSecret("oauth-idp-ca"),
					},
				},
			}},
			want: []string{"oauth-id", "oauth-secret", "oauth-idp-ca"},
		},
		{
			name: "configMap and inline sources are ignored",
			cluster: &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
				TLS: &v1alpha1.TLSConfig{ClientCert: vfConfigMap("cm-cert")},
			}},
			want: nil,
		},
		{
			name: "duplicate secret names dedup",
			cluster: &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
				Auth: &v1alpha1.AuthConfig{
					SCRAM: &v1alpha1.SCRAMAuth{Username: vfSecretVal("creds"), Password: vfSecretVal("creds")},
				},
			}},
			want: []string{"creds"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := clusterSecretNames(tc.cluster)
			sort.Strings(got)
			want := append([]string(nil), tc.want...)
			sort.Strings(want)
			if len(got) == 0 && len(want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("clusterSecretNames = %v, want %v", got, want)
			}
		})
	}
}
