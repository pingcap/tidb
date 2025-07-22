// Copyright 2024 PingCAP, Inc.
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

package runaway

import (
	"context"
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/ttl/sqlbuilder"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

const (
	// watchTableName is the name of system table which save runaway watch items.
	watchTableName = "mysql.tidb_runaway_watch"
	// watchDoneTableName is the name of system table which save done runaway watch items.
	watchDoneTableName = "mysql.tidb_runaway_watch_done"

	maxIDRetries = 3
)

// NullTime is a zero time.Time.
var NullTime time.Time

// Record is used to save records which will be inserted into mysql.tidb_runaway_queries.
type Record struct {
	ResourceGroupName string
	StartTime         time.Time
	Match             string
	Action            string
	SampleText        string
	SQLDigest         string
	PlanDigest        string
	Source            string
	ExceedCause       string
	// Repeats is used to avoid inserting the same record multiple times.
	// It records the number of times after flushing the record(10s) to the table or len(map) exceeds the threshold(1024).
	// We only consider `resource_group_name`, `sql_digest`, `plan_digest` and `match_type` when comparing records.
	// default value is 1.
	Repeats int
}

// recordKey represents the composite key for record key in `tidb_runaway_queries`.
type recordKey struct {
	ResourceGroupName string
	SQLDigest         string
	PlanDigest        string
	Match             string
}

// Hash generates a hash for the recordKey.
// Because `tidb_runaway_queries` is informational and not performance-critical,
// we can lose some accuracy for other component's performance.
func (k recordKey) Hash() uint64 {
	h := fnv.New64a()
	h.Write([]byte(k.ResourceGroupName))
	h.Write([]byte(k.SQLDigest))
	h.Write([]byte(k.PlanDigest))
	h.Write([]byte(k.Match))
	return h.Sum64()
}

// genRunawayQueriesStmt generates statement with given RunawayRecords.
func genRunawayQueriesStmt(recordMap map[recordKey]*Record) (string, []any) {
	var builder strings.Builder
	params := make([]any, 0, len(recordMap)*10)
	builder.WriteString("INSERT INTO mysql.tidb_runaway_queries " +
		"(resource_group_name, start_time, match_type, action, sample_sql, sql_digest, plan_digest, tidb_server, rule, repeats) VALUES ")
	firstRecord := true
	for _, r := range recordMap {
		if !firstRecord {
			builder.WriteByte(',')
		}
		firstRecord = false
		builder.WriteString("(%?, %?, %?, %?, %?, %?, %?, %?, %?, %?)")
		params = append(params, r.ResourceGroupName, r.StartTime, r.Match, r.Action, r.SampleText, r.SQLDigest, r.PlanDigest, r.Source, r.ExceedCause, r.Repeats)
	}
	return builder.String(), params
}

// QuarantineRecord is used to save records which will be inserted into mysql.tidb_runaway_watch.
type QuarantineRecord struct {
	ID                int64
	ResourceGroupName string
	// startTime and endTime are in UTC.
	StartTime   time.Time
	EndTime     time.Time
	Watch       rmpb.RunawayWatchType
	WatchText   string
	Source      string
	ExceedCause string
	// Action-related fields.
	Action          rmpb.RunawayAction
	SwitchGroupName string
}

// getRecordKey is used to get the key in ttl cache.
func (r *QuarantineRecord) getRecordKey() string {
	return r.ResourceGroupName + "/" + r.WatchText
}

func (r *QuarantineRecord) getSwitchGroupName() string {
	if r.Action == rmpb.RunawayAction_SwitchGroup {
		return r.SwitchGroupName
	}
	return ""
}

// GetExceedCause returns the exceed cause.
func (r *QuarantineRecord) GetExceedCause() string {
	return r.ExceedCause
}

// GetActionString returns the action string.
func (r *QuarantineRecord) GetActionString() string {
	if r == nil {
		return rmpb.RunawayAction_NoneAction.String()
	}
	if r.Action == rmpb.RunawayAction_SwitchGroup {
		return fmt.Sprintf("%s(%s)", r.Action.String(), r.SwitchGroupName)
	}
	return r.Action.String()
}

func writeInsert(builder *strings.Builder, tableName string) {
	builder.WriteString("insert into ")
	builder.WriteString(tableName)
	builder.WriteString(" VALUES ")
}

// genInsertionStmt is used to generate insertion sql.
func (r *QuarantineRecord) genInsertionStmt() (string, []any) {
	var builder strings.Builder
	params := make([]any, 0, 9)
	writeInsert(&builder, watchTableName)
	builder.WriteString("(null, %?, %?, %?, %?, %?, %?, %?, %?, %?)")
	params = append(params, r.ResourceGroupName)
	params = append(params, r.StartTime)
	if r.EndTime.Equal(NullTime) {
		params = append(params, nil)
	} else {
		params = append(params, r.EndTime)
	}
	params = append(params, r.Watch)
	params = append(params, r.WatchText)
	params = append(params, r.Source)
	params = append(params, r.Action)
	params = append(params, r.getSwitchGroupName())
	params = append(params, r.ExceedCause)
	return builder.String(), params
}

// genInsertionDoneStmt is used to generate insertion sql for runaway watch done record.
func (r *QuarantineRecord) genInsertionDoneStmt() (string, []any) {
	var builder strings.Builder
	params := make([]any, 0, 11)
	writeInsert(&builder, watchDoneTableName)
	builder.WriteString("(null, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?)")
	params = append(params, r.ID)
	params = append(params, r.ResourceGroupName)
	params = append(params, r.StartTime)
	if r.EndTime.Equal(NullTime) {
		params = append(params, nil)
	} else {
		params = append(params, r.EndTime)
	}
	params = append(params, r.Watch)
	params = append(params, r.WatchText)
	params = append(params, r.Source)
	params = append(params, r.Action)
	params = append(params, r.getSwitchGroupName())
	params = append(params, r.ExceedCause)
	params = append(params, time.Now().UTC())
	return builder.String(), params
}

// genDeletionStmt is used to generate deletion sql.
func (r *QuarantineRecord) genDeletionStmt() (string, []any) {
	var builder strings.Builder
	params := make([]any, 0, 1)
	builder.WriteString("delete from ")
	builder.WriteString(watchTableName)
	builder.WriteString(" where id = %?")
	params = append(params, r.ID)
	return builder.String(), params
}

func (rm *Manager) deleteExpiredRows(expiredDuration time.Duration) {
	const (
		tableName = "tidb_runaway_queries"
		colName   = "start_time"
	)
	var systemSchemaCIStr = ast.NewCIStr("mysql")

	if !rm.ddl.OwnerManager().IsOwner() {
		return
	}
	failpoint.Inject("FastRunawayGC", func() {
		expiredDuration = time.Second * 1
	})
	expiredTime := time.Now().Add(-expiredDuration)
	tbCIStr := ast.NewCIStr(tableName)
	tbl, err := rm.infoCache.GetLatest().TableByName(context.Background(), systemSchemaCIStr, tbCIStr)
	if err != nil {
		logutil.BgLogger().Error("delete system table failed", zap.String("table", tableName), zap.Error(err))
		return
	}
	tbInfo := tbl.Meta()
	col := tbInfo.FindPublicColumnByName(colName)
	if col == nil {
		logutil.BgLogger().Error("time column is not public in table", zap.String("table", tableName), zap.String("column", colName))
		return
	}
	tb, err := cache.NewBasePhysicalTable(systemSchemaCIStr, tbInfo, ast.NewCIStr(""), col)
	if err != nil {
		logutil.BgLogger().Error("delete system table failed", zap.String("table", tableName), zap.Error(err))
		return
	}
	generator, err := sqlbuilder.NewScanQueryGenerator(tb, expiredTime, nil, nil)
	if err != nil {
		logutil.BgLogger().Error("delete system table failed", zap.String("table", tableName), zap.Error(err))
		return
	}
	var leftRows [][]types.Datum
	for {
		sql := ""
		if sql, err = generator.NextSQL(leftRows, runawayRecordGCSelectBatchSize); err != nil {
			logutil.BgLogger().Error("delete system table failed", zap.String("table", tableName), zap.Error(err))
			return
		}
		// to remove
		if len(sql) == 0 {
			return
		}

		rows, sqlErr := ExecRCRestrictedSQL(rm.sysSessionPool, sql, nil)
		if sqlErr != nil {
			logutil.BgLogger().Error("delete system table failed", zap.String("table", tableName), zap.Error(err))
			return
		}
		leftRows = make([][]types.Datum, len(rows))
		for i, row := range rows {
			leftRows[i] = row.GetDatumRow(tb.KeyColumnTypes)
		}

		for len(leftRows) > 0 {
			var delBatch [][]types.Datum
			if len(leftRows) < runawayRecordGCBatchSize {
				delBatch = leftRows
				leftRows = nil
			} else {
				delBatch = leftRows[0:runawayRecordGCBatchSize]
				leftRows = leftRows[runawayRecordGCBatchSize:]
			}
			sql, err := sqlbuilder.BuildDeleteSQL(tb, delBatch, expiredTime)
			if err != nil {
				logutil.BgLogger().Error(
					"build delete SQL failed when deleting system table",
					zap.Error(err),
					zap.String("table", tb.Schema.O+"."+tb.Name.O),
				)
				return
			}

			_, err = ExecRCRestrictedSQL(rm.sysSessionPool, sql, nil)
			if err != nil {
				logutil.BgLogger().Error(
					"delete SQL failed when deleting system table", zap.Error(err), zap.String("SQL", sql),
				)
			}
		}
	}
}

func handleRemoveStaleRunawayWatch(sysSessionPool util.SessionPool, record *QuarantineRecord) error {
	se, err := sysSessionPool.Get()
	defer func() {
		sysSessionPool.Put(se)
	}()
	if err != nil {
		return errors.Annotate(err, "get session failed")
	}
	sctx := se.(sessionctx.Context)
	exec := sctx.GetSQLExecutor()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	_, err = exec.ExecuteInternal(ctx, "BEGIN")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if err != nil {
			_, err1 := exec.ExecuteInternal(ctx, "ROLLBACK")
			terror.Log(err1)
			return
		}
		_, err = exec.ExecuteInternal(ctx, "COMMIT")
		if err != nil {
			return
		}
	}()
	sql, params := record.genDeletionStmt()
	_, err = exec.ExecuteInternal(ctx, sql, params...)
	return err
}

func handleRunawayWatchDone(sysSessionPool util.SessionPool, record *QuarantineRecord) error {
	se, err := sysSessionPool.Get()
	defer func() {
		sysSessionPool.Put(se)
	}()
	if err != nil {
		return errors.Annotate(err, "get session failed")
	}
	sctx := se.(sessionctx.Context)
	exec := sctx.GetSQLExecutor()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	_, err = exec.ExecuteInternal(ctx, "BEGIN")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if err != nil {
			_, err1 := exec.ExecuteInternal(ctx, "ROLLBACK")
			terror.Log(err1)
			return
		}
		_, err = exec.ExecuteInternal(ctx, "COMMIT")
		if err != nil {
			return
		}
	}()
	sql, params := record.genInsertionDoneStmt()
	_, err = exec.ExecuteInternal(ctx, sql, params...)
	if err != nil {
		return err
	}
	sql, params = record.genDeletionStmt()
	_, err = exec.ExecuteInternal(ctx, sql, params...)
	return err
}

// ExecRCRestrictedSQL is used to execute a restricted SQL which related to resource control.
func ExecRCRestrictedSQL(sysSessionPool util.SessionPool, sql string, params []any) ([]chunk.Row, error) {
	se, err := sysSessionPool.Get()
	defer func() {
		sysSessionPool.Put(se)
	}()
	if err != nil {
		return nil, errors.Annotate(err, "get session failed")
	}
	sctx := se.(sessionctx.Context)
	exec := sctx.GetRestrictedSQLExecutor()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	r, _, err := exec.ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
		sql, params...,
	)
	return r, err
}

// AddRunawayWatch is used to add runaway watch item manually.
func (rm *Manager) AddRunawayWatch(record *QuarantineRecord) (uint64, error) {
	se, err := rm.sysSessionPool.Get()
	defer func() {
		rm.sysSessionPool.Put(se)
	}()
	if err != nil {
		return 0, errors.Annotate(err, "get session failed")
	}
	exec := se.(sessionctx.Context).GetSQLExecutor()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	_, err = exec.ExecuteInternal(ctx, "BEGIN")
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer func() {
		if err != nil {
			_, err1 := exec.ExecuteInternal(ctx, "ROLLBACK")
			terror.Log(err1)
			return
		}
		_, err = exec.ExecuteInternal(ctx, "COMMIT")
		if err != nil {
			return
		}
	}()
	sql, params := record.genInsertionStmt()
	_, err = exec.ExecuteInternal(ctx, sql, params...)
	if err != nil {
		return 0, err
	}
	for retry := 0; retry < maxIDRetries; retry++ {
		if retry > 0 {
			select {
			case <-rm.exit:
				return 0, err
			case <-time.After(time.Millisecond * time.Duration(retry*100)):
				logutil.BgLogger().Warn("failed to get last insert id when adding runaway watch", zap.Error(err))
			}
		}
		var rs sqlexec.RecordSet
		rs, err = exec.ExecuteInternal(ctx, `SELECT LAST_INSERT_ID();`)
		if err != nil {
			continue
		}
		var rows []chunk.Row
		rows, err = sqlexec.DrainRecordSet(ctx, rs, 1)
		//nolint: errcheck
		rs.Close()
		if err != nil {
			continue
		}
		if len(rows) != 1 {
			err = errors.Errorf("unexpected result length: %d", len(rows))
			continue
		}
		return rows[0].GetUint64(0), nil
	}
	return 0, errors.Errorf("An error: %v occurred while getting the ID of the newly added watch record. Try querying information_schema.runaway_watches later", err)
}

// RemoveRunawayWatch is used to remove runaway watch item manually.
func (rm *Manager) RemoveRunawayWatch(recordID int64) error {
	rm.runawaySyncer.mu.Lock()
	defer rm.runawaySyncer.mu.Unlock()
	records, err := rm.runawaySyncer.getWatchRecordByID(recordID)
	if err != nil {
		return err
	}
	if len(records) != 1 {
		return errors.Errorf("no runaway watch with the specific ID")
	}

	err = handleRunawayWatchDone(rm.sysSessionPool, records[0])
	return err
}
