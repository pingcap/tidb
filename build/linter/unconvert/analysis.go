package unconvert

import (
	"fmt"
	"go/token"
	"go/types"

	unconvertAPI "github.com/golangci/unconvert"
	"github.com/pingcap/tidb/build/linter/util"
	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/loader"
)

type Unconvert struct{}

const Name = "unconvert"

var Analyzer = &analysis.Analyzer{
	Name: Name,
	Doc:  "Remove unnecessary type conversions",
	Run:  run,
}

func run(pass *analysis.Pass) (interface{}, error) {
	var createdPkgs []*loader.PackageInfo
	createdPkgs = append(createdPkgs, util.MakeFakeLoaderPackageInfo(pass))
	allPkgs := map[*types.Package]*loader.PackageInfo{}
	for _, pkg := range createdPkgs {
		pkg := pkg
		allPkgs[pkg.Pkg] = pkg
	}
	prog := &loader.Program{
		Fset:        pass.Fset,
		Imported:    nil,         // not used without .Created in any linter
		Created:     createdPkgs, // all initial packages
		AllPackages: allPkgs,     // all initial packages and their depndencies
	}
	positions := unconvertAPI.Run(prog)
	if len(positions) == 0 {
		return nil, nil
	}

	for _, pos := range positions {
		pass.Reportf(token.Pos(pos.Offset), fmt.Sprintf("[%s] Unnecessary conversion", Name))
	}
	return nil, nil
}
