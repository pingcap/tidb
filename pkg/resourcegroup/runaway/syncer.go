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
	"strings"
	"sync"
	"time"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// Label values for syncer metrics.
	lblSync      = "sync"
	lblWatch     = "watch"
	lblWatchDone = "watch_done"

	// watchSyncInterval is the interval to sync the watch record.
	watchSyncInterval = time.Second
	// watchSyncOverlap is how far back CheckPoint rewinds from the captured
	// UpperBound after a non-empty scan, so the next scan re-inspects the
	// tail of the previous window. It budgets the lag between "now" on this
	// syncer node and the moment a row with `start_time ≈ now` becomes
	// visible to a subsequent snapshot read (commit/read-TS delay and
	// wall-clock skew). De-duplication across the overlap is handled by
	// `AddWatch` in memory.
	watchSyncOverlap = 3 * watchSyncInterval
	// watchSyncBatchLimit caps the number of rows returned per scan query.
	// This prevents unbounded memory usage when the time window spans a
	// large number of rows (e.g. the very first scan from NullTime).
	// When the limit is hit, CheckPoint advances to the last row's key
	// column time so the next 1-second sync cycle continues from there.
	watchSyncBatchLimit = maxWatchRecordChannelSize * 2
	// watchTableName is the name of system table which save runaway watch items.
	runawayWatchTableName = "tidb_runaway_watch"
	// watchDoneTableName is the name of system table which save done runaway watch items.
	runawayWatchDoneTableName = "tidb_runaway_watch_done"
)

func getRunawayWatchTableName() string {
	return fmt.Sprintf("mysql.%s", runawayWatchTableName)
}

func getRunawayWatchDoneTableName() string {
	return fmt.Sprintf("mysql.%s", runawayWatchDoneTableName)
}

// Column layout of `mysql.tidb_runaway_watch`, in DDL order (see
// `CreateTiDBRunawayWatchTable` in pkg/meta/metadef/system_tables_def.go).
// `watchColRule` is the DB column literally named `rule`; it persists
// `QuarantineRecord.ExceedCause` (see watchRecordColumns).
const (
	watchColID = iota
	watchColResourceGroupName
	watchColStartTime
	watchColEndTime
	watchColWatch
	watchColWatchText
	watchColSource
	watchColAction
	watchColSwitchGroupName
	watchColRule
)

// Column layout of `mysql.tidb_runaway_watch_done`, in DDL order (see
// `CreateTiDBRunawayWatchDoneTable` in pkg/meta/metadef/system_tables_def.go).
// The done table prepends its own `id`/`record_id` and appends `done_time`;
// `watchDoneColID` (the done-row PK) and `watchDoneColDoneTime` are declared
// but not projected onto QuarantineRecord.
const (
	watchDoneColID = iota
	watchDoneColRecordID
	watchDoneColResourceGroupName
	watchDoneColStartTime
	watchDoneColEndTime
	watchDoneColWatch
	watchDoneColWatchText
	watchDoneColSource
	watchDoneColAction
	watchDoneColSwitchGroupName
	watchDoneColRule
	watchDoneColDoneTime
)

// quarantineColumns projects QuarantineRecord fields onto the columns of a
// runaway watch system table. Each systemTableReader carries its own instance
// so the decoder never has to assume a shared layout between tables. The two
// naming divergences (`ID`↔`record_id` on the done table; `ExceedCause`↔`rule`
// on both) are resolved by the initializers below.
type quarantineColumns struct {
	ID                int
	ResourceGroupName int
	StartTime         int
	EndTime           int
	Watch             int
	WatchText         int
	Source            int
	Action            int
	SwitchGroupName   int
	ExceedCause       int
}

var (
	watchRecordColumns = quarantineColumns{
		ID:                watchColID,
		ResourceGroupName: watchColResourceGroupName,
		StartTime:         watchColStartTime,
		EndTime:           watchColEndTime,
		Watch:             watchColWatch,
		WatchText:         watchColWatchText,
		Source:            watchColSource,
		Action:            watchColAction,
		SwitchGroupName:   watchColSwitchGroupName,
		ExceedCause:       watchColRule,
	}
	watchDoneRecordColumns = quarantineColumns{
		ID:                watchDoneColRecordID,
		ResourceGroupName: watchDoneColResourceGroupName,
		StartTime:         watchDoneColStartTime,
		EndTime:           watchDoneColEndTime,
		Watch:             watchDoneColWatch,
		WatchText:         watchDoneColWatchText,
		Source:            watchDoneColSource,
		Action:            watchDoneColAction,
		SwitchGroupName:   watchDoneColSwitchGroupName,
		ExceedCause:       watchDoneColRule,
	}
)

// Syncer is used to sync the runaway records.
type syncer struct {
	newWatchReader      *systemTableReader
	deletionWatchReader *systemTableReader
	sysSessionPool      util.SessionPool
	infoCache           *infoschema.InfoCache

	mu sync.Mutex

	lastSyncTime   time.Time
	syncInterval   prometheus.Observer
	syncDuration   prometheus.Observer
	watchCPGauge   prometheus.Gauge
	doneCPGauge    prometheus.Gauge
	syncOKCounter  prometheus.Counter
	syncErrCounter prometheus.Counter
}

func newSyncer(sysSessionPool util.SessionPool, infoCache *infoschema.InfoCache) *syncer {
	return &syncer{
		sysSessionPool: sysSessionPool,
		infoCache:      infoCache,
		newWatchReader: &systemTableReader{
			TableName:     getRunawayWatchTableName(),
			KeyCol:        "start_time",
			KeyColIdx:     watchColStartTime,
			RecordColumns: watchRecordColumns,
			CheckPoint:    NullTime,
			UpperBound:    NullTime,
		},
		deletionWatchReader: &systemTableReader{
			TableName:     getRunawayWatchDoneTableName(),
			KeyCol:        "done_time",
			KeyColIdx:     watchDoneColDoneTime,
			RecordColumns: watchDoneRecordColumns,
			CheckPoint:    NullTime,
			UpperBound:    NullTime,
		},
		syncInterval:   metrics.RunawaySyncerIntervalHistogram.WithLabelValues(lblSync),
		syncDuration:   metrics.RunawaySyncerDurationHistogram.WithLabelValues(lblSync),
		watchCPGauge:   metrics.RunawaySyncerCheckpointGauge.WithLabelValues(lblWatch),
		doneCPGauge:    metrics.RunawaySyncerCheckpointGauge.WithLabelValues(lblWatchDone),
		syncOKCounter:  metrics.RunawaySyncerCounter.WithLabelValues(lblSync, metrics.LblOK),
		syncErrCounter: metrics.RunawaySyncerCounter.WithLabelValues(lblSync, metrics.LblError),
	}
}

var (
	systemSchemaCIStr          = ast.NewCIStr("mysql")
	runawayWatchTableCIStr     = ast.NewCIStr(runawayWatchTableName)
	runawayWatchDoneTableCIStr = ast.NewCIStr(runawayWatchDoneTableName)
)

func (s *syncer) checkWatchTableExist() bool {
	return s.checkTableExist(runawayWatchTableCIStr)
}

func (s *syncer) checkWatchDoneTableExist() bool {
	return s.checkTableExist(runawayWatchDoneTableCIStr)
}

// checkTableExist checks if the table exists using infoschema cache (memory lookup, no SQL).
func (s *syncer) checkTableExist(tableName ast.CIStr) bool {
	if s.infoCache == nil {
		return false
	}
	is := s.infoCache.GetLatest()
	if is == nil {
		return false
	}
	_, err := is.TableByName(context.Background(), systemSchemaCIStr, tableName)
	// If the table not exists, an `ErrTableNotExists` error will be returned.
	return err == nil
}

func (s *syncer) getWatchRecordByID(id int64) ([]*QuarantineRecord, error) {
	return s.readQuarantineRecords(s.newWatchReader, s.newWatchReader.genSelectByIDStmt(id))
}

func (s *syncer) getWatchRecordByGroup(groupName string) ([]*QuarantineRecord, error) {
	return s.readQuarantineRecords(s.newWatchReader, s.newWatchReader.genSelectByGroupStmt(groupName))
}

func (s *syncer) getNewWatchRecords() ([]*QuarantineRecord, error) {
	return s.scanNewRecordsInRange(s.newWatchReader)
}

func (s *syncer) getNewWatchDoneRecords() ([]*QuarantineRecord, error) {
	return s.scanNewRecordsInRange(s.deletionWatchReader)
}

// scanNewRecordsInRange performs a time-windowed scan over reader's table using
// the `[CheckPoint, UpperBound)` range produced by genSelectStmt, then advances
// CheckPoint based on the decoded row count:
//
//   - Full batch (len(records) == watchSyncBatchLimit): more rows may remain
//     in the window. CheckPoint moves to the last row's key-column time so
//     the next 1-second sync cycle continues from there. De-duplication in
//     AddWatch/removeWatch handles the row re-read at the `>=` boundary.
//   - Partial batch (0 < len(records) < watchSyncBatchLimit): the window is
//     drained. CheckPoint moves to `UpperBound - watchSyncOverlap` so rows
//     that become visible slightly later are still re-inspected.
//
// This branch is on the *decoded* count, not the raw SQL count, because in
// production every runaway watch row is written by TiDB code with canonical
// timestamps — start_time/end_time/done_time never fail to decode, so
// decoded == raw. The decoder's `continue`-on-bad-row is a safety net for
// schema evolution / external corruption, not a normal code path; letting
// such a row short-circuit the full-batch check on the (never-observed)
// tail-corruption edge case is an accepted simplification over plumbing a
// separate raw-count return value through.
//
// This method is the only writer of reader.lastScanKeyTime; point-query
// paths (getWatchRecordByID / getWatchRecordByGroup) go through
// readQuarantineRecords, which deliberately leaves scan state untouched so
// manual RemoveRunawayWatch calls can't perturb checkpoint advancement.
func (s *syncer) scanNewRecordsInRange(reader *systemTableReader) ([]*QuarantineRecord, error) {
	// Capture UpperBound before reading so the checkpoint advances
	// deterministically from it rather than from a post-decode `time.Now()`.
	reader.UpperBound = time.Now().UTC()
	sql, params := reader.genSelectStmt()
	rows, err := ExecRCRestrictedSQL(s.sysSessionPool, sql, params)
	if err != nil {
		return nil, err
	}
	records := make([]*QuarantineRecord, 0, len(rows))
	for _, r := range rows {
		rec, ok := decodeQuarantineRecord(r, reader.RecordColumns)
		if !ok {
			continue
		}
		if t, e := r.GetTime(reader.KeyColIdx).GoTime(time.UTC); e == nil {
			reader.lastScanKeyTime = t
		}
		records = append(records, rec)
	}
	if len(records) >= watchSyncBatchLimit {
		// Batch full — more rows may remain in the window. Advance
		// CheckPoint to the last row's key-column time so the next cycle
		// resumes from there (with `>=`, the boundary row will be re-read
		// and de-duplicated).
		//
		// If lastScanKeyTime is not strictly after CheckPoint, all rows in
		// the batch share the same key-column timestamp (microsecond-
		// precision collision on insert). Fall through to the UpperBound-
		// based advancement to avoid a livelock that re-reads the same page
		// every cycle.
		//
		// TODO: this same-key fallback can silently skip rows whose key is
		// >= UpperBound - watchSyncOverlap but wasn't returned in the batch.
		// Microsecond precision plus an insert rate high enough to pack 2k+
		// rows into a single microsecond is not realistic today, so the
		// trade-off is acceptable for now; revisit if it ever becomes one.
		if reader.lastScanKeyTime.After(reader.CheckPoint) {
			reader.CheckPoint = reader.lastScanKeyTime
		} else {
			reader.CheckPoint = reader.UpperBound.Add(-watchSyncOverlap)
		}
	} else if len(records) > 0 {
		reader.CheckPoint = reader.UpperBound.Add(-watchSyncOverlap)
	}
	return records, nil
}

// readQuarantineRecords runs sqlGenFn against reader's table and decodes each
// row into a QuarantineRecord via reader.RecordColumns. It is the point-query
// entry point (by ID / by resource group) and deliberately does NOT mutate
// reader.lastScanKeyTime or any other scan state — only scanNewRecordsInRange
// advances the sync cursor.
func (s *syncer) readQuarantineRecords(
	reader *systemTableReader,
	genFn sqlGenFn,
) ([]*QuarantineRecord, error) {
	sql, params := genFn()
	rows, err := ExecRCRestrictedSQL(s.sysSessionPool, sql, params)
	if err != nil {
		return nil, err
	}
	ret := make([]*QuarantineRecord, 0, len(rows))
	for _, r := range rows {
		if rec, ok := decodeQuarantineRecord(r, reader.RecordColumns); ok {
			ret = append(ret, rec)
		}
	}
	return ret, nil
}

// decodeQuarantineRecord decodes one raw chunk.Row into a QuarantineRecord
// according to cols. Rows whose start_time or end_time fail to parse are
// rejected (returns nil, false). In production these columns are always
// written by TiDB code, so this guard never fires; it keeps the sync loop
// robust against schema evolution or externally inserted corrupt rows.
func decodeQuarantineRecord(r chunk.Row, cols quarantineColumns) (*QuarantineRecord, bool) {
	startTime, err := r.GetTime(cols.StartTime).GoTime(time.UTC)
	if err != nil {
		return nil, false
	}
	var endTime time.Time
	if !r.IsNull(cols.EndTime) {
		endTime, err = r.GetTime(cols.EndTime).GoTime(time.UTC)
		if err != nil {
			return nil, false
		}
	}
	return &QuarantineRecord{
		ID:                r.GetInt64(cols.ID),
		ResourceGroupName: r.GetString(cols.ResourceGroupName),
		StartTime:         startTime,
		EndTime:           endTime,
		Watch:             rmpb.RunawayWatchType(r.GetInt64(cols.Watch)),
		WatchText:         r.GetString(cols.WatchText),
		Source:            r.GetString(cols.Source),
		Action:            rmpb.RunawayAction(r.GetInt64(cols.Action)),
		SwitchGroupName:   r.GetString(cols.SwitchGroupName),
		ExceedCause:       r.GetString(cols.ExceedCause),
	}, true
}

// systemTableReader reads `tidb_runaway_watch` or `tidb_runaway_watch_done`.
// RecordColumns gives the per-table column-index layout; CheckPoint and
// UpperBound define the half-open `[CheckPoint, UpperBound)` window used by
// the paginated scan in genSelectStmt.
type systemTableReader struct {
	TableName     string
	KeyCol        string
	KeyColIdx     int // column index of KeyCol in SELECT * result, for pagination
	RecordColumns quarantineColumns
	CheckPoint    time.Time
	UpperBound    time.Time
	// lastScanKeyTime is the key-column time of the last valid row produced
	// by the most recent scanNewRecordsInRange call. It is consulted when a
	// batch is full to advance CheckPoint. Only scanNewRecordsInRange writes
	// to it; point-query paths (readQuarantineRecords) leave it alone so
	// manual Remove* operations cannot perturb sync-cursor advancement.
	lastScanKeyTime time.Time
}

// sqlGenFn returns the SQL statement and parameters for one read issued by the
// syncer. Callers that want the paginated window scan use
// systemTableReader.genSelectStmt directly as a method value; point-query
// callers (by ID / by resource group) build a closure via genSelectBy*Stmt.
type sqlGenFn func() (string, []any)

func (r *systemTableReader) genSelectByIDStmt(id int64) sqlGenFn {
	return func() (string, []any) {
		var builder strings.Builder
		params := make([]any, 0, 1)
		builder.WriteString("select * from ")
		builder.WriteString(r.TableName)
		builder.WriteString(" where id = %?")
		params = append(params, id)
		return builder.String(), params
	}
}

func (r *systemTableReader) genSelectByGroupStmt(groupName string) sqlGenFn {
	return func() (string, []any) {
		var builder strings.Builder
		params := make([]any, 0, 1)
		builder.WriteString("select * from ")
		builder.WriteString(r.TableName)
		builder.WriteString(" where resource_group_name = %?")
		params = append(params, groupName)
		return builder.String(), params
	}
}

func (r *systemTableReader) genSelectStmt() (string, []any) {
	var builder strings.Builder
	params := make([]any, 0, 3)
	builder.WriteString("select * from ")
	builder.WriteString(r.TableName)
	builder.WriteString(" where ")
	builder.WriteString(r.KeyCol)
	builder.WriteString(" >= %? and ")
	builder.WriteString(r.KeyCol)
	builder.WriteString(" < %? order by ")
	builder.WriteString(r.KeyCol)
	builder.WriteString(" limit %?")
	params = append(params, r.CheckPoint, r.UpperBound, watchSyncBatchLimit)
	return builder.String(), params
}
