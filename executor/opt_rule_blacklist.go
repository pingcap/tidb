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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"

	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/set"
	"github.com/pingcap/tidb/util/sqlexec"
)

// ReloadOptRuleBlacklistExec indicates ReloadOptRuleBlacklist executor.
type ReloadOptRuleBlacklistExec struct {
	baseExecutor
}

// Next implements the Executor Next interface.
func (e *ReloadOptRuleBlacklistExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	return LoadOptRuleBlacklist(e.ctx)
}

// LoadOptRuleBlacklist loads the latest data from table mysql.opt_rule_blacklist.
func LoadOptRuleBlacklist(ctx sessionctx.Context) (err error) {
	exec := ctx.(sqlexec.RestrictedSQLExecutor)
	stmt, err := exec.ParseWithParams(context.TODO(), "select HIGH_PRIORITY name from mysql.opt_rule_blacklist")
	if err != nil {
		return err
	}
	rows, _, err := exec.ExecRestrictedStmt(context.TODO(), stmt)
	if err != nil {
		return err
	}
	newDisabledLogicalRules := set.NewStringSet()
	for _, row := range rows {
		name := row.GetString(0)
		newDisabledLogicalRules.Insert(name)
	}
	plannercore.DefaultDisabledLogicalRulesList.Store(newDisabledLogicalRules)
	return nil
}
