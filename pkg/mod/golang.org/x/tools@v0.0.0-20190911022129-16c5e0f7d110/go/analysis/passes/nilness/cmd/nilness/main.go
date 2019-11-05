// The nilness command applies the golang.org/x/tools/go/analysis/passes/nilness
// analysis to the specified packages of Go source code.
package main

import (
	"golang.org/x/tools/go/analysis/passes/nilness"
	"golang.org/x/tools/go/analysis/singlechecker"
)

func main() { singlechecker.Main(nilness.Analyzer) }
