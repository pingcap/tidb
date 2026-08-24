// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/pingcap/tidb/pkg/parser/ast/visitor_codegen"
)

func main() {
	sourceDir := flag.String("source-dir", "pkg/parser/ast", "directory containing AST traversal sources")
	output := flag.String("output", "pkg/parser/ast/visitor_inplace_generated.go", "generated output path")
	flag.Parse()

	result, err := visitor_codegen.Generate(visitor_codegen.GenerateRequest{SourceDir: *sourceDir})
	if err == nil {
		err = visitor_codegen.WriteFileAtomically(*output, result.Source)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
