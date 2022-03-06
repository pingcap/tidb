package tools

// This file ensures `go mod tidy` will not delete entries to all tools.

import (
	// golangci-lint is a package-based linter
	_ "github.com/golangci/golangci-lint/pkg/commands"

	// revive is a file-based linter
	_ "github.com/mgechev/revive"

	// govet checks for code correctness
	_ "github.com/dnephin/govet"

	// failpoint enables manual 'failure' of some execution points.
	_ "github.com/pingcap/failpoint"
	_ "github.com/pingcap/failpoint/code"
)
