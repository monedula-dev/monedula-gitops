//go:build tools

// Package tools pins build-time code-generation tooling so `go run` uses a
// version recorded in go.mod. Not built into the binary.
package tools

import _ "sigs.k8s.io/controller-tools/cmd/controller-gen"
