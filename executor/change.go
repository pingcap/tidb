// Copyright 2016 PingCAP, Inc.
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

package executor

import (
	"context"
	"github.com/pingcap/parser/ast"
)

// ChangeExec represents a change executor.
type ChangeExec struct {
	baseExecutor

	Statement ast.ChangeStmt
}

// Next implements the Executor Next interface.
func (e *ChangeExec) Next(ctx context.Context) error {

	/*	stmt := e.Statement
		state:=stmt.NodeType
	*/
	//todo
	return nil
}
