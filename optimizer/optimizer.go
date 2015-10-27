// Copyright 2015 PingCAP, Inc.
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

package optimizer

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/stmt"
	"github.com/juju/errors"
)

// Compile compiles a ast.Node into a executable statement.
func Compile(node ast.Node) (stmt.Statement, error) {
	validator := &validator{}
	if _, ok := node.Accept(validator); !ok {
		return nil, errors.Trace(validator.err)
	}

	binder := &InfoBinder{}
	if _, ok := node.Accept(validator); !ok {
		return nil, errors.Trace(binder.Err)
	}

	tpComputer := &typeComputer{}
	if _, ok := node.Accept(typeComputer{}); !ok {
		return nil, errors.Trace(tpComputer.err)
	}

	switch v := node.(type) {
	case *ast.SelectStmt:

	case *ast.SetStmt:
		return compileSet(v)
	}
	return nil, nil
}

func compileSelect(s *ast.SelectStmt) (stmt.Statement, error) {

	return nil, nil
}

func compileSet(s *ast.SetStmt) (stmt.Statement, error) {
	return nil, nil
}
