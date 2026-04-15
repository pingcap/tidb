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
	// watchSyncOverlap is the amount of overlap retained between consecutive scans.
	watchSyncOverlap = 3 * watchSyncInterval
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

// Column layout of `mysql.tidb_runaway_watch`. Keep the values in sync with
// the table's CREATE definition; readQuarantineRecords consults these by name
// so any column is resolved at the call site instead of via index arithmetic.
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
	watchColExceedCause
)

// Column layout of `mysql.tidb_runaway_watch_done`. The done table prepends
// its own primary-key column and appends `done_time`; the QuarantineRecord
// fields sit in between. Listing every column explicitly (rather than
// expressing this table's layout as an offset of `tidb_runaway_watch`) keeps
// decoding robust against future schema drift on either table.
const (
	watchDoneColDoneID = iota
	watchDoneColID
	watchDoneColResourceGroupName
	watchDoneColStartTime
	watchDoneColEndTime
	watchDoneColWatch
	watchDoneColWatchText
	watchDoneColSource
	watchDoneColAction
	watchDoneColSwitchGroupName
	watchDoneColExceedCause
	watchDoneColDoneTime
)

// quarantineColumns projects QuarantineRecord fields onto the columns of a
// runaway watch system table. Each systemTableReader carries its own instance
// so the decoder never has to assume a shared layout between tables.
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
		ExceedCause:       watchColExceedCause,
	}
	watchDoneRecordColumns = quarantineColumns{
		ID:                watchDoneColID,
		ResourceGroupName: watchDoneColResourceGroupName,
		StartTime:         watchDoneColStartTime,
		EndTime:           watchDoneColEndTime,
		Watch:             watchDoneColWatch,
		WatchText:         watchDoneColWatchText,
		Source:            watchDoneColSource,
		Action:            watchDoneColAction,
		SwitchGroupName:   watchDoneColSwitchGroupName,
		ExceedCause:       watchDoneColExceedCause,
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
			RecordColumns: watchRecordColumns,
			CheckPoint:    NullTime,
			UpperBound:    NullTime,
		},
		deletionWatchReader: &systemTableReader{
			TableName:     getRunawayWatchDoneTableName(),
			KeyCol:        "done_time",
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
// the `[CheckPoint, UpperBound)` range produced by genSelectStmt. When any rows
// are returned, it advances CheckPoint to just before the captured UpperBound
// (keeping a small overlap so rows that become visible slightly later can still
// be re-scanned; de-duplication is later handled by `AddWatch` in memory).
func (s *syncer) scanNewRecordsInRange(reader *systemTableReader) ([]*QuarantineRecord, error) {
	// Capture the upper bound before reading so the checkpoint can advance
	// deterministically from it rather than from a post-decode `time.Now()`.
	reader.UpperBound = time.Now().UTC()
	records, err := s.readQuarantineRecords(reader, reader.genSelectStmt)
	if err != nil {
		return nil, err
	}
	if len(records) > 0 {
		reader.CheckPoint = reader.UpperBound.Add(-watchSyncOverlap)
	}
	return records, nil
}

// readQuarantineRecords runs sqlGenFn against reader's table and decodes each
// row into a QuarantineRecord using reader.RecordColumns to address every
// field by its table-specific column index. This keeps decoding robust when
// either table's schema evolves: a new or reordered column only requires
// updating the corresponding column-index constants, not this decoder.
// Rows whose start_time or end_time cannot be decoded are skipped rather
// than failing the whole scan.
func (s *syncer) readQuarantineRecords(
	reader *systemTableReader,
	sqlGenFn func() (string, []any),
) ([]*QuarantineRecord, error) {
	rows, err := reader.Read(s.sysSessionPool, sqlGenFn)
	if err != nil {
		return nil, err
	}
	cols := reader.RecordColumns
	ret := make([]*QuarantineRecord, 0, len(rows))
	for _, r := range rows {
		startTime, err := r.GetTime(cols.StartTime).GoTime(time.UTC)
		if err != nil {
			continue
		}
		var endTime time.Time
		if !r.IsNull(cols.EndTime) {
			endTime, err = r.GetTime(cols.EndTime).GoTime(time.UTC)
			if err != nil {
				continue
			}
		}
		ret = append(ret, &QuarantineRecord{
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
		})
	}
	return ret, nil
}

// SystemTableReader is used to read table `runaway_watch` and `runaway_watch_done`.
//
// RecordColumns maps every QuarantineRecord field to its column index in this
// reader's table. Each reader owns its own mapping, so the shared decoder
// never has to infer one table's layout from the other's.
//
// CheckPoint and UpperBound define the half-open `[CheckPoint, UpperBound)`
// time window used by the paginated scan in genSelectStmt. UpperBound is
// captured before each scan so the checkpoint can advance deterministically
// from it rather than from a post-decode `time.Now()`.
type systemTableReader struct {
	TableName     string
	KeyCol        string
	RecordColumns quarantineColumns
	CheckPoint    time.Time
	UpperBound    time.Time
}

func (r *systemTableReader) genSelectByIDStmt(id int64) func() (string, []any) {
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

func (r *systemTableReader) genSelectByGroupStmt(groupName string) func() (string, []any) {
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
	params := make([]any, 0, 2)
	builder.WriteString("select * from ")
	builder.WriteString(r.TableName)
	builder.WriteString(" where ")
	builder.WriteString(r.KeyCol)
	builder.WriteString(" >= %? and ")
	builder.WriteString(r.KeyCol)
	builder.WriteString(" < %? order by ")
	builder.WriteString(r.KeyCol)
	params = append(params, r.CheckPoint, r.UpperBound)
	return builder.String(), params
}

func (*systemTableReader) Read(sysSessionPool util.SessionPool, genFn func() (string, []any)) ([]chunk.Row, error) {
	sql, params := genFn()
	return ExecRCRestrictedSQL(sysSessionPool, sql, params)
}
