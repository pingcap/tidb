// Copyright 2019 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

// ReloadDisabledOptimizeListExec indicates ReloadDisabledOptimizeList executor.
type ReloadDisabledOptimizeListExec struct {
	baseExecutor
}

// Next implements the Executor Next interface.
func (e *ReloadDisabledOptimizeListExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	return LoadDisabledOptimizeList(e.ctx)
}

// LoadDisabledOptimizeList loads the latest data from table mysql.disabled_optimize_list.
func LoadDisabledOptimizeList(ctx sessionctx.Context) (err error) {
	sql := "select HIGH_PRIORITY name, type from mysql.disabled_optimize_list"
	rows, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, sql)
	if err != nil {
		return err
	}
	newExprPushdownBlacklist := make(map[string]struct{})
	newDisabledLogicalRules := make(map[string]struct{})
	// insertSQL is for compatibility with expr_pushdown_backlist
	insertSQL := "insert into mysql.expr_pushdown_blacklist values"
	defer func() {
		insertLen := len(insertSQL)
		insertSQL = insertSQL[:insertLen-1]
		if insertSQL[insertLen-2] == ')' {
			_, err = ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), insertSQL)
		}
	}()
	for _, row := range rows {
		name := strings.ToLower(row.GetString(0))
		if row.GetString(1) == "expr_push_down" {
			insertSQL += "(\"" + name + "\"),"
			newExprPushdownBlacklist[name] = struct{}{}
		} else if row.GetString(1) == "logical_rule" {
			newDisabledLogicalRules[name] = struct{}{}
		} else {
			ctx.GetSessionVars().StmtCtx.AppendWarning(errors.New("type field of mysql.disabled_optimize_list can only be \"expr_push_down\" or \"logical_rule\""))
		}
	}
	expression.DefaultExprPushdownBlacklist.Store(newExprPushdownBlacklist)
	plannercore.DefaultDisabledLogicalRulesList.Store(newDisabledLogicalRules)
	return nil
}
