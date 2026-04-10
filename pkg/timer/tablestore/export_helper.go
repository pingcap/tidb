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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tablestore

import (
	"context"

	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/timer/api"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// Exported functions and types for testing

// ExportedBuildInsertTimerSQL exports buildInsertTimerSQL for testing
func ExportedBuildInsertTimerSQL(dbName, tableName string, record *api.TimerRecord) (string, []any, error) {
	return buildInsertTimerSQL(dbName, tableName, record)
}

// ExportedBuildCondCriteria exports buildCondCriteria for testing
func ExportedBuildCondCriteria(cond api.Cond, args []any) (string, []any, error) {
	return buildCondCriteria(cond, args)
}

// ExportedBuildSelectTimerSQL exports buildSelectTimerSQL for testing
func ExportedBuildSelectTimerSQL(dbName, tableName string, cond api.Cond) (string, []any, error) {
	return buildSelectTimerSQL(dbName, tableName, cond)
}

// ExportedBuildUpdateCriteria exports buildUpdateCriteria for testing
func ExportedBuildUpdateCriteria(update *api.TimerUpdate, args []any) (string, []any, error) {
	return buildUpdateCriteria(update, args)
}

// ExportedBuildUpdateTimerSQL exports buildUpdateTimerSQL for testing
func ExportedBuildUpdateTimerSQL(dbName, tblName string, timerID string, update *api.TimerUpdate) (string, []any, error) {
	return buildUpdateTimerSQL(dbName, tblName, timerID, update)
}

// ExportedBuildDeleteTimerSQL exports buildDeleteTimerSQL for testing
func ExportedBuildDeleteTimerSQL(dbName, tblName string, timerID string) (string, []any) {
	return buildDeleteTimerSQL(dbName, tblName, timerID)
}

// ExportedRunInTxn exports runInTxn for testing
func ExportedRunInTxn(ctx context.Context, exec sqlexec.SQLExecutor, fn func() error) error {
	return runInTxn(ctx, exec, fn)
}

// ExportedTableTimerStoreCore exports tableTimerStoreCore type for testing
type ExportedTableTimerStoreCore = tableTimerStoreCore

// ExportedNewTableTimerStoreCore exports newTableTimerStoreCore for testing
func ExportedNewTableTimerStoreCore(dbName, tblName string, pool ...syssession.Pool) *ExportedTableTimerStoreCore {
	core := &tableTimerStoreCore{
		dbName:  dbName,
		tblName: tblName,
	}
	if len(pool) > 0 {
		core.pool = pool[0]
	}
	return core
}

// ExportedWithSession exports withSession for testing
func (e *ExportedTableTimerStoreCore) ExportedWithSession(fn func(*syssession.Session) error) error {
	return e.withSession(fn)
}

// ExportedWithSctx exports withSctx for testing
func (e *ExportedTableTimerStoreCore) ExportedWithSctx(fn func(sessionctx.Context) error) error {
	return e.withSctx(fn)
}

// ExportedExecuteSQL exports executeSQL for testing
func ExportedExecuteSQL(ctx context.Context, exec sqlexec.SQLExecutor, sql string, args ...any) ([]chunk.Row, error) {
	return executeSQL(ctx, exec, sql, args...)
}
