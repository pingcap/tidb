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
			getRunawayWatchTableName(),
			"start_time",
			NullTime,
			NullTime},
		deletionWatchReader: &systemTableReader{
			getRunawayWatchDoneTableName(),
			"done_time",
			NullTime,
			NullTime},
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
	return s.getWatchRecord(s.newWatchReader, s.newWatchReader.genSelectByIDStmt(id))
}

func (s *syncer) getWatchRecordByGroup(groupName string) ([]*QuarantineRecord, error) {
	return s.getWatchRecord(s.newWatchReader, s.newWatchReader.genSelectByGroupStmt(groupName))
}

func (s *syncer) getNewWatchRecords() ([]*QuarantineRecord, error) {
	reader := s.newWatchReader
	reader.UpperBound = time.Now().UTC()
	records, err := s.getWatchRecord(reader, reader.genSelectStmt)
	if err != nil {
		return nil, err
	}
	// Advance the checkpoint from the fixed upper bound captured before the scan starts.
	// Keep a small overlap so rows that become visible slightly later can still be re-scanned.
	// De-duplication will be handled later by `AddWatch` in memory.
	if len(records) > 0 {
		reader.CheckPoint = reader.UpperBound.Add(-watchSyncOverlap)
	}
	return records, nil
}

func (s *syncer) getNewWatchDoneRecords() ([]*QuarantineRecord, error) {
	reader := s.deletionWatchReader
	reader.UpperBound = time.Now().UTC()
	records, err := s.getWatchDoneRecord(reader, reader.genSelectStmt)
	if err != nil {
		return nil, err
	}
	// Ditto as getNewWatchRecords.
	if len(records) > 0 {
		reader.CheckPoint = reader.UpperBound.Add(-watchSyncOverlap)
	}
	return records, nil
}

func (s *syncer) getWatchRecord(reader *systemTableReader, sqlGenFn func() (string, []any)) ([]*QuarantineRecord, error) {
	return getRunawayWatchRecord(s.sysSessionPool, reader, sqlGenFn)
}

func (s *syncer) getWatchDoneRecord(reader *systemTableReader, sqlGenFn func() (string, []any)) ([]*QuarantineRecord, error) {
	return getRunawayWatchDoneRecord(s.sysSessionPool, reader, sqlGenFn)
}

func getRunawayWatchRecord(
	sysSessionPool util.SessionPool,
	reader *systemTableReader,
	sqlGenFn func() (string, []any),
) ([]*QuarantineRecord, error) {
	rs, err := reader.Read(sysSessionPool, sqlGenFn)
	if err != nil {
		return nil, err
	}
	ret := make([]*QuarantineRecord, 0, len(rs))
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
		qr := &QuarantineRecord{
			ID:                r.GetInt64(0),
			ResourceGroupName: r.GetString(1),
			StartTime:         startTime,
			EndTime:           endTime,
			Watch:             rmpb.RunawayWatchType(r.GetInt64(4)),
			WatchText:         r.GetString(5),
			Source:            r.GetString(6),
			Action:            rmpb.RunawayAction(r.GetInt64(7)),
			SwitchGroupName:   r.GetString(8),
			ExceedCause:       r.GetString(9),
		}
		ret = append(ret, qr)
	}
	return ret, nil
}

func getRunawayWatchDoneRecord(
	sysSessionPool util.SessionPool,
	reader *systemTableReader,
	sqlGenFn func() (string, []any),
) ([]*QuarantineRecord, error) {
	rs, err := reader.Read(sysSessionPool, sqlGenFn)
	if err != nil {
		return nil, err
	}
	ret := make([]*QuarantineRecord, 0, len(rs))
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
		qr := &QuarantineRecord{
			ID:                r.GetInt64(1),
			ResourceGroupName: r.GetString(2),
			StartTime:         startTime,
			EndTime:           endTime,
			Watch:             rmpb.RunawayWatchType(r.GetInt64(5)),
			WatchText:         r.GetString(6),
			Source:            r.GetString(7),
			Action:            rmpb.RunawayAction(r.GetInt64(8)),
			SwitchGroupName:   r.GetString(9),
			ExceedCause:       r.GetString(10),
		}
		ret = append(ret, qr)
	}
	return ret, nil
}

// SystemTableReader is used to read table `runaway_watch` and `runaway_watch_done`.
type systemTableReader struct {
	TableName  string
	KeyCol     string
	CheckPoint time.Time
	UpperBound time.Time
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
