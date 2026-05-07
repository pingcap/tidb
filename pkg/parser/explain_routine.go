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

package parser

import "github.com/pingcap/tidb/pkg/parser/ast"

func newExplainRoutineStmt(format string, analyze bool, routineType string, call interface{}) ast.StmtNode {
	return newExplainRoutineStmtWithTarget(format, analyze, routineType, call, false, 0)
}

func newExplainRoutineStmtWithTarget(format string, analyze bool, routineType string, call interface{}, hasTarget bool, target uint64) ast.StmtNode {
	fnCall := call.(*ast.FuncCallExpr)
	return &ast.ExplainRoutineStmt{
		Format:      format,
		Analyze:     analyze,
		RoutineType: routineType,
		Name: &ast.TableName{
			Schema: fnCall.Schema,
			Name:   fnCall.FnName,
		},
		Args:                 fnCall.Args,
		HasTargetStmtOrdinal: hasTarget,
		TargetStmtOrdinal:    target,
	}
}
