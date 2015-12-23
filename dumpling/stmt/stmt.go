// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package stmt

import (
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/util/format"
)

// Statement is an interface for SQL execution.
// NOTE: all Statement implementations must be safe for
// concurrent using by multiple goroutines.
// If the Exec method requires any Execution domain local data,
// they must be held out of the implementing instance.
type Statement interface {
	// Explain gets the execution plans.
	Explain(ctx context.Context, w format.Formatter)

	// IsDDL shows whether the statement is an DDL operation.
	IsDDL() bool

	// OriginText gets the origin SQL text.
	OriginText() string

	// SetText sets the executive SQL text.
	SetText(text string)

	// Exec executes SQL and gets a Recordset.
	Exec(ctx context.Context) (rset.Recordset, error)
}

// Show statement types.
const (
	ShowNone = iota
	ShowEngines
	ShowDatabases
	ShowTables
	ShowTableStatus
	ShowColumns
	ShowWarnings
	ShowCharset
	ShowVariables
	ShowStatus
	ShowCollation
	ShowCreateTable
	ShowGrants
	ShowTriggers
	ShowProcedureStatus
	ShowIndex
)

const (
	// SessionScope shows varibales in session scope.
	SessionScope = iota
	// GlobalScope shows varibales in global scope.
	GlobalScope
)

// A dummy type to avoid naming collision in context.
type execArgsKeyType int

func (k execArgsKeyType) String() string {
	return "stmt_exec_args"
}

const execArgsKey execArgsKeyType = 0

// BindExecArgs binds executive args to context.
func BindExecArgs(ctx context.Context, args []interface{}) {
	ctx.SetValue(execArgsKey, args)
}

// GetExecArgs gets executive args from context.
func GetExecArgs(ctx context.Context) []interface{} {
	v, ok := ctx.Value(execArgsKey).([]interface{})
	if !ok {
		return nil
	}
	return v
}

// ClearExecArgs clears executive args from context.
func ClearExecArgs(ctx context.Context) {
	ctx.ClearValue(execArgsKey)
}
