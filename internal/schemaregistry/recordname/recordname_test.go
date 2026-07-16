package recordname

import (
	"strings"
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

func TestExtract(t *testing.T) {
	tests := []struct {
		name       string
		format     string
		definition string
		want       string
		wantErr    string // substring; "" = expect success
	}{
		// ---- AVRO ----
		{
			name:       "avro namespace + name",
			format:     "AVRO",
			definition: `{"type":"record","name":"Order","namespace":"payments","fields":[]}`,
			want:       "payments.Order",
		},
		{
			name:       "avro no namespace",
			format:     "AVRO",
			definition: `{"type":"record","name":"Order","fields":[]}`,
			want:       "Order",
		},
		{
			name:       "avro dotted name ignores namespace",
			format:     "AVRO",
			definition: `{"type":"record","name":"com.acme.Order","namespace":"payments","fields":[]}`,
			want:       "com.acme.Order",
		},
		{
			name:       "avro dotted name no namespace",
			format:     "AVRO",
			definition: `{"type":"record","name":"com.acme.Order","fields":[]}`,
			want:       "com.acme.Order",
		},
		{
			name:       "avro invalid json",
			format:     "AVRO",
			definition: `{not json`,
			wantErr:    "invalid",
		},
		{
			name:       "avro missing name",
			format:     "AVRO",
			definition: `{"type":"record","namespace":"payments","fields":[]}`,
			wantErr:    "name",
		},
		// ---- JSON ----
		{
			name:       "json title",
			format:     "JSON",
			definition: `{"title":"Order","type":"object"}`,
			want:       "Order",
		},
		{
			name:       "json missing title",
			format:     "JSON",
			definition: `{"type":"object"}`,
			wantErr:    "title",
		},
		{
			name:       "json empty title",
			format:     "JSON",
			definition: `{"title":"","type":"object"}`,
			wantErr:    "title",
		},
		{
			name:       "json title not a string",
			format:     "JSON",
			definition: `{"title":42,"type":"object"}`,
			wantErr:    "title",
		},
		{
			name:       "json invalid json",
			format:     "JSON",
			definition: `{nope`,
			wantErr:    "invalid",
		},
		// ---- PROTOBUF ----
		{
			name:       "protobuf with package",
			format:     "PROTOBUF",
			definition: "syntax = \"proto3\";\npackage acme.orders;\n\nmessage Order {\n  string id = 1;\n}\n",
			want:       "acme.orders.Order",
		},
		{
			name:       "protobuf without package",
			format:     "PROTOBUF",
			definition: "syntax = \"proto3\";\n\nmessage Order {\n  string id = 1;\n}\n",
			want:       "Order",
		},
		{
			name:       "protobuf first message wins",
			format:     "PROTOBUF",
			definition: "package acme;\nmessage First {}\nmessage Second {}\n",
			want:       "acme.First",
		},
		{
			name:       "protobuf leading whitespace",
			format:     "PROTOBUF",
			definition: "package acme;\n    message Indented {\n}\n",
			want:       "acme.Indented",
		},
		{
			name:       "protobuf no message",
			format:     "PROTOBUF",
			definition: "syntax = \"proto3\";\npackage acme;\n",
			wantErr:    "message",
		},
		// ---- unknown format ----
		{
			name:       "unknown format",
			format:     "YAML",
			definition: `{}`,
			wantErr:    "format",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Extract(tc.format, tc.definition)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("Extract(%q, ...) = %q, want error containing %q", tc.format, got, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Extract error = %q, want substring %q", err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Extract(%q, ...) unexpected error: %v", tc.format, err)
			}
			if got != tc.want {
				t.Fatalf("Extract(%q, ...) = %q, want %q", tc.format, got, tc.want)
			}
		})
	}
}

func TestSubjects(t *testing.T) {
	const avroValue = `{"type":"record","name":"Order","namespace":"payments","fields":[]}`
	const avroKey = `{"type":"record","name":"OrderKey","namespace":"payments","fields":[]}`

	tests := []struct {
		name      string
		strategy  string
		topicName string
		schema    *v1alpha1.TopicSchema
		valueDef  string
		keyDef    string
		wantValue string
		wantKey   string
		wantErr   string
	}{
		{
			name:      "TopicName value only",
			strategy:  "TopicName",
			topicName: "orders",
			schema:    &v1alpha1.TopicSchema{Format: "AVRO"},
			valueDef:  avroValue,
			wantValue: "orders-value",
			wantKey:   "",
		},
		{
			name:      "TopicName value and key",
			strategy:  "TopicName",
			topicName: "orders",
			schema:    &v1alpha1.TopicSchema{Format: "AVRO"},
			valueDef:  avroValue,
			keyDef:    avroKey,
			wantValue: "orders-value",
			wantKey:   "orders-key",
		},
		{
			name:      "empty strategy defaults to TopicName",
			strategy:  "",
			topicName: "orders",
			schema:    &v1alpha1.TopicSchema{Format: "AVRO"},
			valueDef:  avroValue,
			wantValue: "orders-value",
		},
		{
			name:      "TopicName governance mode (no defs) still names value subject",
			strategy:  "TopicName",
			topicName: "orders",
			schema:    &v1alpha1.TopicSchema{Format: "AVRO"},
			wantValue: "orders-value",
			wantKey:   "",
		},
		{
			name:      "RecordName value and key",
			strategy:  "RecordName",
			topicName: "orders",
			schema:    &v1alpha1.TopicSchema{Format: "AVRO"},
			valueDef:  avroValue,
			keyDef:    avroKey,
			wantValue: "payments.Order",
			wantKey:   "payments.OrderKey",
		},
		{
			name:      "RecordName value only",
			strategy:  "RecordName",
			topicName: "orders",
			schema:    &v1alpha1.TopicSchema{Format: "AVRO"},
			valueDef:  avroValue,
			wantValue: "payments.Order",
			wantKey:   "",
		},
		{
			name:      "TopicRecordName value and key",
			strategy:  "TopicRecordName",
			topicName: "orders",
			schema:    &v1alpha1.TopicSchema{Format: "AVRO"},
			valueDef:  avroValue,
			keyDef:    avroKey,
			wantValue: "orders-payments.Order",
			wantKey:   "orders-payments.OrderKey",
		},
		{
			name:      "Custom value and key verbatim",
			strategy:  "Custom",
			topicName: "orders",
			schema:    &v1alpha1.TopicSchema{Format: "AVRO", ValueSubject: "my.value.subject", KeySubject: "my.key.subject"},
			valueDef:  avroValue,
			keyDef:    avroKey,
			wantValue: "my.value.subject",
			wantKey:   "my.key.subject",
		},
		{
			name:      "Custom governance mode (no defs, explicit valueSubject)",
			strategy:  "Custom",
			topicName: "orders",
			schema:    &v1alpha1.TopicSchema{Format: "AVRO", ValueSubject: "my.value.subject"},
			wantValue: "my.value.subject",
			wantKey:   "",
		},
		{
			name:      "RecordName invalid value def is an error",
			strategy:  "RecordName",
			topicName: "orders",
			schema:    &v1alpha1.TopicSchema{Format: "AVRO"},
			valueDef:  `{not json`,
			wantErr:   "value",
		},
		// Subject collision: RecordName strategy where value and key records share
		// the same full name → both subjects resolve to the same string. This would
		// silently clobber one subject; Subjects must return an error.
		{
			name:      "RecordName same value and key record name is an error",
			strategy:  "RecordName",
			topicName: "orders",
			schema:    &v1alpha1.TopicSchema{Format: "AVRO"},
			valueDef:  `{"type":"record","name":"SameName","namespace":"com.acme","fields":[]}`,
			keyDef:    `{"type":"record","name":"SameName","namespace":"com.acme","fields":[{"name":"id","type":"string"}]}`,
			wantErr:   "same subject",
		},
		// Subject collision: Custom strategy where valueSubject == keySubject.
		{
			name:      "Custom same value and key subject is an error",
			strategy:  "Custom",
			topicName: "orders",
			schema:    &v1alpha1.TopicSchema{Format: "AVRO", ValueSubject: "shared.subject", KeySubject: "shared.subject"},
			valueDef:  `{"type":"record","name":"Order","fields":[]}`,
			keyDef:    `{"type":"record","name":"OrderKey","fields":[]}`,
			wantErr:   "same subject",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotValue, gotKey, err := Subjects(tc.strategy, tc.topicName, tc.schema, tc.valueDef, tc.keyDef)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("Subjects = (%q, %q), want error containing %q", gotValue, gotKey, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Subjects error = %q, want substring %q", err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Subjects unexpected error: %v", err)
			}
			if gotValue != tc.wantValue {
				t.Fatalf("Subjects valueSubject = %q, want %q", gotValue, tc.wantValue)
			}
			if gotKey != tc.wantKey {
				t.Fatalf("Subjects keySubject = %q, want %q", gotKey, tc.wantKey)
			}
		})
	}
}
