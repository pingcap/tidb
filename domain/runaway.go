// Copyright 2023 PingCAP, Inc.
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

package domain

import (
	"context"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/domain/resourcegroup"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/ttl/sqlbuilder"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
	"go.uber.org/zap"
)

const (
	runawayRecordFlushInterval   = time.Second
	runawayRecordGCInterval      = time.Hour * 24
	runawayRecordExpiredDuration = time.Hour * 24 * 7
	runawayWatchSyncInterval     = time.Second

	runawayRecordGCBatchSize       = 100
	runawayRecordGCSelectBatchSize = runawayRecordGCBatchSize * 5
)

var systemSchemaCIStr = model.NewCIStr("mysql")

func (do *Domain) deleteExpiredRows(tableName, colName string, expiredDuration time.Duration) {
	if !do.DDL().OwnerManager().IsOwner() {
		return
	}
	failpoint.Inject("FastRunawayGC", func() {
		expiredDuration = time.Second * 1
	})
	expiredTime := time.Now().Add(-expiredDuration)
	tbCIStr := model.NewCIStr(tableName)
	tbl, err := do.InfoSchema().TableByName(systemSchemaCIStr, tbCIStr)
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
	tb, err := cache.NewBasePhysicalTable(systemSchemaCIStr, tbInfo, model.NewCIStr(""), col)
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

		rows, sqlErr := do.execRestrictedSQL(sql, nil)
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

			_, err = do.execRestrictedSQL(sql, nil)
			if err != nil {
				logutil.BgLogger().Error(
					"delete SQL failed when deleting system table", zap.Error(err), zap.String("SQL", sql),
				)
			}
		}
	}
}

func (do *Domain) updateNewAndDoneWatch() error {
	do.runawaySyncer.mu.Lock()
	defer do.runawaySyncer.mu.Unlock()
	records, err := do.runawaySyncer.getNewWatchRecords()
	if err != nil {
		logutil.BgLogger().Error("try to get new runaway watch", zap.Error(err))
		return err
	}
	for _, r := range records {
		do.runawayManager.AddWatch(r)
	}
	doneRecords, err := do.runawaySyncer.getNewWatchDoneRecords()
	if err != nil {
		logutil.BgLogger().Error("try to get done runaway watch", zap.Error(err))
		return err
	}
	for _, r := range doneRecords {
		do.runawayManager.RemoveWatch(r)
	}
	return nil
}

func (do *Domain) runawayWatchSyncLoop() {
	defer util.Recover(metrics.LabelDomain, "runawayWatchSyncLoop", nil, false)
	runawayWatchSyncTicker := time.NewTicker(runawayWatchSyncInterval)
	for {
		select {
		case <-do.exit:
			return
		case <-runawayWatchSyncTicker.C:
			err := do.updateNewAndDoneWatch()
			if err != nil {
				logutil.BgLogger().Warn("get runaway watch record failed", zap.Error(err))
			}
		}
	}
}

// AddRunawayWatch is used to add runaway watch item manually.
func (do *Domain) AddRunawayWatch(record *resourcegroup.QuarantineRecord) error {
	return do.handleRunawayWatch(record)
}

// GetRunawayWatchList is used to get all items from runaway watch list.
func (do *Domain) GetRunawayWatchList() []*resourcegroup.QuarantineRecord {
	return do.runawayManager.GetWatchList()
}

// TryToUpdateRunawayWatch is used to to update watch list including
// creation and deletion by manual trigger.
func (do *Domain) TryToUpdateRunawayWatch() error {
	return do.updateNewAndDoneWatch()
}

// RemoveRunawayWatch is used to remove runaway watch item manually.
func (do *Domain) RemoveRunawayWatch(recordID int64) error {
	do.runawaySyncer.mu.Lock()
	defer do.runawaySyncer.mu.Unlock()
	records, err := do.runawaySyncer.getWatchRecordByID(recordID)
	if err != nil {
		return err
	}
	if len(records) != 1 {
		return errors.Errorf("no runaway watch with the specific ID")
	}
	err = do.handleRunawayWatchDone(records[0])
	return err
}

func (do *Domain) runawayRecordFlushLoop() {
	defer util.Recover(metrics.LabelDomain, "runawayRecordFlushLoop", nil, false)

	// this times is used to batch flushing rocords, with 1s duration,
	// we can guarantee a watch record can be seen by the user within 1s.
	runawayRecordFluashTimer := time.NewTimer(runawayRecordFlushInterval)
	runawayRecordGCTicker := time.NewTicker(runawayRecordGCInterval)
	failpoint.Inject("FastRunawayGC", func() {
		runawayRecordFluashTimer.Stop()
		runawayRecordGCTicker.Stop()
		runawayRecordFluashTimer = time.NewTimer(time.Millisecond * 50)
		runawayRecordGCTicker = time.NewTicker(time.Millisecond * 200)
	})

	fired := false
	recordCh := do.RunawayManager().RunawayRecordChan()
	quarantineRecordCh := do.RunawayManager().QuarantineRecordChan()
	staleQuarantineRecordCh := do.RunawayManager().StaleQuarantineRecordChan()
	flushThrehold := do.runawayManager.FlushThreshold()
	records := make([]*resourcegroup.RunawayRecord, 0, flushThrehold)

	flushRunawayRecords := func() {
		if len(records) == 0 {
			return
		}
		sql, params := resourcegroup.GenRunawayQueriesStmt(records)
		if _, err := do.execRestrictedSQL(sql, params); err != nil {
			logutil.BgLogger().Error("flush runaway records failed", zap.Error(err), zap.Int("count", len(records)))
		}
		records = records[:0]
	}

	for {
		select {
		case <-do.exit:
			return
		case <-runawayRecordFluashTimer.C:
			flushRunawayRecords()
			fired = true
		case r := <-recordCh:
			records = append(records, r)
			failpoint.Inject("FastRunawayGC", func() {
				flushRunawayRecords()
			})
			if len(records) >= flushThrehold {
				flushRunawayRecords()
			} else if fired {
				fired = false
				// meet a new record, reset the timer.
				runawayRecordFluashTimer.Reset(runawayRecordFlushInterval)
			}
		case <-runawayRecordGCTicker.C:
			go do.deleteExpiredRows("tidb_runaway_queries", "time", runawayRecordExpiredDuration)
		case r := <-quarantineRecordCh:
			go func() {
				err := do.handleRunawayWatch(r)
				if err != nil {
					logutil.BgLogger().Error("add runaway watch", zap.Error(err))
				}
			}()
		case r := <-staleQuarantineRecordCh:
			go func() {
				for i := 0; i < 3; i++ {
					err := do.handleRemoveStaleRunawayWatch(r)
					if err == nil {
						break
					}
					logutil.BgLogger().Error("remove stale runaway watch", zap.Error(err))
					time.Sleep(time.Second)
				}
			}()
		}
	}
}

func (do *Domain) handleRunawayWatch(record *resourcegroup.QuarantineRecord) error {
	se, err := do.sysSessionPool.Get()
	defer func() {
		do.sysSessionPool.Put(se)
	}()
	if err != nil {
		return errors.Annotate(err, "get session failed")
	}
	exec, _ := se.(sqlexec.SQLExecutor)
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
	sql, params := record.GenInsertionStmt()
	_, err = exec.ExecuteInternal(ctx, sql, params...)
	return err
}

func (do *Domain) handleRunawayWatchDone(record *resourcegroup.QuarantineRecord) error {
	se, err := do.sysSessionPool.Get()
	defer func() {
		do.sysSessionPool.Put(se)
	}()
	if err != nil {
		return errors.Annotate(err, "get session failed")
	}
	exec, _ := se.(sqlexec.SQLExecutor)
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
	sql, params := record.GenInsertionDoneStmt()
	_, err = exec.ExecuteInternal(ctx, sql, params...)
	if err != nil {
		return err
	}
	sql, params = record.GenDeletionStmt()
	_, err = exec.ExecuteInternal(ctx, sql, params...)
	return err
}

func (do *Domain) handleRemoveStaleRunawayWatch(record *resourcegroup.QuarantineRecord) error {
	se, err := do.sysSessionPool.Get()
	defer func() {
		do.sysSessionPool.Put(se)
	}()
	if err != nil {
		return errors.Annotate(err, "get session failed")
	}
	exec, _ := se.(sqlexec.SQLExecutor)
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
	sql, params := record.GenDeletionStmt()
	_, err = exec.ExecuteInternal(ctx, sql, params...)
	return err
}

func (do *Domain) execRestrictedSQL(sql string, params []interface{}) ([]chunk.Row, error) {
	se, err := do.sysSessionPool.Get()
	defer func() {
		do.sysSessionPool.Put(se)
	}()
	if err != nil {
		return nil, errors.Annotate(err, "get session failed")
	}
	exec := se.(sqlexec.RestrictedSQLExecutor)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	r, _, err := exec.ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
		sql, params...,
	)
	return r, err
}

func (do *Domain) initResourceGroupsController(ctx context.Context, pdClient pd.Client, uniqueID uint64) error {
	if pdClient == nil {
		logutil.BgLogger().Warn("cannot setup up resource controller, not using tikv storage")
		// return nil as unistore doesn't support it
		return nil
	}

	control, err := rmclient.NewResourceGroupController(ctx, uniqueID, pdClient, nil, rmclient.WithMaxWaitDuration(resourcegroup.MaxWaitDuration))
	if err != nil {
		return err
	}
	control.Start(ctx)
	serverInfo, err := infosync.GetServerInfo()
	if err != nil {
		return err
	}
	serverAddr := net.JoinHostPort(serverInfo.IP, strconv.Itoa(int(serverInfo.Port)))
	do.runawayManager = resourcegroup.NewRunawayManager(control, serverAddr)
	do.runawaySyncer = newRunawaySyncer(do.sysSessionPool)
	do.resourceGroupsController = control
	tikv.SetResourceControlInterceptor(control)
	return nil
}

type runawaySyncer struct {
	newWatchReader      *SystemTableReader
	deletionWatchReader *SystemTableReader
	sysSessionPool      *sessionPool
	mu                  sync.Mutex
}

func newRunawaySyncer(sysSessionPool *sessionPool) *runawaySyncer {
	return &runawaySyncer{
		sysSessionPool: sysSessionPool,
		newWatchReader: &SystemTableReader{
			resourcegroup.RunawayWatchTableName,
			"start_time",
			resourcegroup.NullTime},
		deletionWatchReader: &SystemTableReader{resourcegroup.RunawayWatchDoneTableName,
			"done_time",
			resourcegroup.NullTime},
	}
}

func (s *runawaySyncer) getWatchRecordByID(id int64) ([]*resourcegroup.QuarantineRecord, error) {
	return s.getWatchRecord(s.newWatchReader, s.newWatchReader.genSelectByIDStmt(id), false)
}

func (s *runawaySyncer) getNewWatchRecords() ([]*resourcegroup.QuarantineRecord, error) {
	return s.getWatchRecord(s.newWatchReader, s.newWatchReader.genSelectStmt, true)
}

func (s *runawaySyncer) getNewWatchDoneRecords() ([]*resourcegroup.QuarantineRecord, error) {
	return s.getWatchDoneRecord(s.deletionWatchReader, s.deletionWatchReader.genSelectStmt, true)
}

func (s *runawaySyncer) getWatchRecord(reader *SystemTableReader, sqlGenFn func() (string, []interface{}), push bool) ([]*resourcegroup.QuarantineRecord, error) {
	se, err := s.sysSessionPool.Get()
	defer func() {
		s.sysSessionPool.Put(se)
	}()
	if err != nil {
		return nil, errors.Annotate(err, "get session failed")
	}
	exec := se.(sqlexec.RestrictedSQLExecutor)
	return getRunawayWatchRecord(exec, reader, sqlGenFn, push)
}

func (s *runawaySyncer) getWatchDoneRecord(reader *SystemTableReader, sqlGenFn func() (string, []interface{}), push bool) ([]*resourcegroup.QuarantineRecord, error) {
	se, err := s.sysSessionPool.Get()
	defer func() {
		s.sysSessionPool.Put(se)
	}()
	if err != nil {
		return nil, errors.Annotate(err, "get session failed")
	}
	exec := se.(sqlexec.RestrictedSQLExecutor)
	return getRunawayWatchDoneRecord(exec, reader, sqlGenFn, push)
}

func getRunawayWatchRecord(exec sqlexec.RestrictedSQLExecutor, reader *SystemTableReader, sqlGenFn func() (string, []interface{}), push bool) ([]*resourcegroup.QuarantineRecord, error) {
	rs, err := reader.Read(exec, sqlGenFn)
	if err != nil {
		return nil, err
	}
	ret := make([]*resourcegroup.QuarantineRecord, 0, len(rs))
	now := time.Now().UTC()
	for _, r := range rs {
		startTime, err := r.GetTime(2).GoTime(time.UTC)
		if err != nil {
			continue
		}
		var endTime time.Time
		if !r.IsNull(3) {
			endTime, err = r.GetTime(3).GoTime(time.UTC)
			if err != nil {
				continue
			}
		}
		qr := &resourcegroup.QuarantineRecord{
			ID:                r.GetInt64(0),
			ResourceGroupName: r.GetString(1),
			StartTime:         startTime,
			EndTime:           endTime,
			Watch:             rmpb.RunawayWatchType(r.GetInt64(4)),
			WatchText:         r.GetString(5),
			Source:            r.GetString(6),
			Action:            rmpb.RunawayAction(r.GetInt64(7)),
		}
		// If a TiDB write record slow, it will occur that the record which has earlier start time is inserted later than others.
		// So we start the scan a little earlier.
		if push {
			reader.CheckPoint = now.Add(-3 * runawayWatchSyncInterval)
		}
		ret = append(ret, qr)
	}
	return ret, nil
}

func getRunawayWatchDoneRecord(exec sqlexec.RestrictedSQLExecutor, reader *SystemTableReader, sqlGenFn func() (string, []interface{}), push bool) ([]*resourcegroup.QuarantineRecord, error) {
	rs, err := reader.Read(exec, sqlGenFn)
	if err != nil {
		return nil, err
	}
	length := len(rs)
	ret := make([]*resourcegroup.QuarantineRecord, 0, length)
	now := time.Now().UTC()
	for _, r := range rs {
		startTime, err := r.GetTime(3).GoTime(time.UTC)
		if err != nil {
			continue
		}
		var endTime time.Time
		if !r.IsNull(4) {
			endTime, err = r.GetTime(4).GoTime(time.UTC)
			if err != nil {
				continue
			}
		}
		qr := &resourcegroup.QuarantineRecord{
			ID:                r.GetInt64(1),
			ResourceGroupName: r.GetString(2),
			StartTime:         startTime,
			EndTime:           endTime,
			Watch:             rmpb.RunawayWatchType(r.GetInt64(5)),
			WatchText:         r.GetString(6),
			Source:            r.GetString(7),
			Action:            rmpb.RunawayAction(r.GetInt64(8)),
		}
		// Ditto as getRunawayWatchRecord.
		if push {
			reader.CheckPoint = now.Add(-3 * runawayWatchSyncInterval)
		}
		ret = append(ret, qr)
	}
	return ret, nil
}

// SystemTableReader is used to read table `runaway_watch` and `runaway_watch_done`.
type SystemTableReader struct {
	TableName  string
	KeyCol     string
	CheckPoint time.Time
}

func (r *SystemTableReader) genSelectByIDStmt(id int64) func() (string, []interface{}) {
	return func() (string, []interface{}) {
		var builder strings.Builder
		params := make([]interface{}, 0, 1)
		builder.WriteString("select * from ")
		builder.WriteString(r.TableName)
		builder.WriteString(" where id = %?")
		params = append(params, id)
		return builder.String(), params
	}
}

func (r *SystemTableReader) genSelectStmt() (string, []interface{}) {
	var builder strings.Builder
	params := make([]interface{}, 0, 1)
	builder.WriteString("select * from ")
	builder.WriteString(r.TableName)
	builder.WriteString(" where ")
	builder.WriteString(r.KeyCol)
	builder.WriteString(" > %? order by ")
	builder.WriteString(r.KeyCol)
	params = append(params, r.CheckPoint)
	return builder.String(), params
}

func (r *SystemTableReader) Read(exec sqlexec.RestrictedSQLExecutor, genFn func() (string, []interface{})) ([]chunk.Row, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	sql, params := genFn()
	rows, _, err := exec.ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
		sql, params...,
	)
	return rows, err
}
