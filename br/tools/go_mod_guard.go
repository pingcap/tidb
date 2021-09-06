package tools

// This file ensures `go mod tidy` will not delete entries to all tools.

import (
	// golangci-lint is a package-based linter
	_ "github.com/golangci/golangci-lint/pkg/commands"

	// revive is a file-based linter
	_ "github.com/mgechev/revive/lint"

	// gocovmerge merges multiple coverage profile into one
	_ "github.com/wadey/gocovmerge"

	// goveralls for uploading coverage profile to coverage tracking service
	_ "github.com/mattn/goveralls"

	// govet checks for code correctness
	_ "github.com/dnephin/govet"

	// failpoint enables manual 'failure' of some execution points.
	_ "github.com/pingcap/failpoint"

	// errdoc-gen generates errors.toml.
	_ "github.com/pingcap/errors/errdoc-gen"

	// A stricter gofmt
	_ "mvdan.cc/gofumpt/gofumports"

	// vfsgen for embedding HTML resources
	_ "github.com/shurcooL/vfsgen/cmd/vfsgendev"

	// gogo for generating lightning checkpoint
	_ "github.com/gogo/protobuf/proto"
)
