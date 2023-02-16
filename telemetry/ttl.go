// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package telemetry

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

const (
	// selectDeletedRowsOneDaySQL selects the deleted rows for each table of last day
	selectDeletedRowsOneDaySQL = `SELECT parent_table_id, CAST(SUM(deleted_rows) AS SIGNED)
		FROM
		    mysql.tidb_ttl_job_history
		WHERE
			status != 'running'
		    AND create_time >= CURDATE() - INTERVAL 7 DAY
			AND finish_time >= CURDATE() - INTERVAL 1 DAY
			AND finish_time < CURDATE()
		GROUP BY parent_table_id;`
	// selectDelaySQL selects the deletion delay in minute for each table at the end of last day
	selectDelaySQL = `SELECT
    		parent_table_id, TIMESTAMPDIFF(MINUTE, MIN(tm), CURDATE()) AS ttl_minutes
		FROM
			(
				SELECT
					table_id,
					parent_table_id,
					MAX(ttl_expire) AS tm
				FROM
					mysql.tidb_ttl_job_history
				WHERE
					create_time > CURDATE() - INTERVAL 7 DAY
					AND finish_time < CURDATE()
					AND status = 'finished'
					AND JSON_VALID(summary_text)
					AND summary_text ->> "$.scan_task_err" IS NULL
				GROUP BY
					table_id, parent_table_id
			) t
		GROUP BY parent_table_id;`
)

type ttlHistItem struct {
	// LessThan is not null means it collects the count of items with condition [prevLessThan, LessThan)
	// Notice that it's type is an int64 pointer to forbid serializing it when it is not set.
	LessThan *int64 `json:"less_than,omitempty"`
	// LessThanMax is true means the condition is [prevLessThan, MAX)
	LessThanMax bool `json:"less_than_max,omitempty"`
	// Count is the count of items that fit the condition
	Count int64 `json:"count"`
}

type ttlUsageCounter struct {
	TTLJobEnabled           bool           `json:"ttl_job_enabled"`
	TTLTables               int64          `json:"ttl_table_count"`
	TTLJobEnabledTables     int64          `json:"ttl_job_enabled_tables"`
	TTLHistDate             string         `json:"ttl_hist_date"`
	TableHistWithDeleteRows []*ttlHistItem `json:"table_hist_with_delete_rows"`
	TableHistWithDelayTime  []*ttlHistItem `json:"table_hist_with_delay_time"`
}

func int64Pointer(val int64) *int64 {
	v := val
	return &v
}

func (c *ttlUsageCounter) UpdateTableHistWithDeleteRows(rows int64) {
	for _, item := range c.TableHistWithDeleteRows {
		if item.LessThanMax || rows < *item.LessThan {
			item.Count++
			return
		}
	}
}

func (c *ttlUsageCounter) UpdateTableHistWithDelayTime(tblCnt int, hours int64) {
	for _, item := range c.TableHistWithDelayTime {
		if item.LessThanMax || hours < *item.LessThan {
			item.Count += int64(tblCnt)
			return
		}
	}
}

func getTTLUsageInfo(ctx context.Context, sctx sessionctx.Context) (counter *ttlUsageCounter) {
	counter = &ttlUsageCounter{
		TTLJobEnabled: variable.EnableTTLJob.Load(),
		TTLHistDate:   time.Now().Add(-24 * time.Hour).Format("2006-01-02"),
		TableHistWithDeleteRows: []*ttlHistItem{
			{
				LessThan: int64Pointer(10 * 1000),
			},
			{
				LessThan: int64Pointer(100 * 1000),
			},
			{
				LessThan: int64Pointer(1000 * 1000),
			},
			{
				LessThan: int64Pointer(10000 * 1000),
			},
			{
				LessThanMax: true,
			},
		},
		TableHistWithDelayTime: []*ttlHistItem{
			{
				LessThan: int64Pointer(1),
			},
			{
				LessThan: int64Pointer(6),
			},
			{
				LessThan: int64Pointer(24),
			},
			{
				LessThan: int64Pointer(72),
			},
			{
				LessThanMax: true,
			},
		},
	}

	is, ok := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	if !ok {
		// it should never happen
		logutil.BgLogger().Error(fmt.Sprintf("GetDomainInfoSchema returns a invalid type: %T", is))
		return
	}

	ttlTables := make(map[int64]*model.TableInfo)
	for _, db := range is.AllSchemas() {
		for _, tbl := range is.SchemaTables(db.Name) {
			tblInfo := tbl.Meta()
			if tblInfo.State != model.StatePublic || tblInfo.TTLInfo == nil {
				continue
			}

			counter.TTLTables++
			if tblInfo.TTLInfo.Enable {
				counter.TTLJobEnabledTables++
			}
			ttlTables[tblInfo.ID] = tblInfo
		}
	}

	exec := sctx.(sqlexec.RestrictedSQLExecutor)
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, selectDeletedRowsOneDaySQL)
	if err != nil {
		logutil.BgLogger().Error("exec sql error", zap.String("SQL", selectDeletedRowsOneDaySQL), zap.Error(err))
	} else {
		for _, row := range rows {
			counter.UpdateTableHistWithDeleteRows(row.GetInt64(1))
		}
	}

	rows, _, err = exec.ExecRestrictedSQL(ctx, nil, selectDelaySQL)
	if err != nil {
		logutil.BgLogger().Error("exec sql error", zap.String("SQL", selectDelaySQL), zap.Error(err))
	} else {
		noHistoryTables := len(ttlTables)
		for _, row := range rows {
			tblID := row.GetInt64(0)
			tbl, ok := ttlTables[tblID]
			if !ok {
				// table not exist, maybe truncated or deleted
				continue
			}
			noHistoryTables--

			evalIntervalSQL := fmt.Sprintf(
				"SELECT TIMESTAMPDIFF(HOUR, CURDATE() - INTERVAL %d MINUTE, CURDATE() - INTERVAL %s %s)",
				row.GetInt64(1), tbl.TTLInfo.IntervalExprStr, ast.TimeUnitType(tbl.TTLInfo.IntervalTimeUnit).String(),
			)

			innerRows, _, err := exec.ExecRestrictedSQL(ctx, nil, evalIntervalSQL)
			if err != nil || len(innerRows) == 0 {
				logutil.BgLogger().Error("exec sql error or empty rows returned", zap.String("SQL", evalIntervalSQL), zap.Error(err))
				continue
			}

			hours := innerRows[0].GetInt64(0)
			counter.UpdateTableHistWithDelayTime(1, hours)
		}

		// When no history found for a table, use max delay
		counter.UpdateTableHistWithDelayTime(noHistoryTables, math.MaxInt64)
	}
	return
}
