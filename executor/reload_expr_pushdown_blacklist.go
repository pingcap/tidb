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

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

// ReloadExprPushdownBlacklistExec indicates ReloadExprPushdownBlacklist executor.
type ReloadExprPushdownBlacklistExec struct {
	baseExecutor
}

func (e *ReloadExprPushdownBlacklistExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	sql := "select HIGH_PRIORITY name from mysql.expr_pushdown_blacklist"
	rows, _, err := e.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(e.ctx, sql)
	if err != nil {
		return err
	}
	newBlacklist := make(map[string]struct{})
	for _, row := range rows {
		name := row.GetString(0)
		newBlacklist[strings.ToLower(name)] = struct{}{}
	}
	expression.DefaultExprPushdownBlacklist.Store(newBlacklist)
	return nil
}
