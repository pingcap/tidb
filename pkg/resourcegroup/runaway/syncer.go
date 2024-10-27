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
	"strings"
	"sync"
	"time"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

const (
	// watchSyncInterval is the interval to sync the watch record.
	watchSyncInterval = time.Second
)

// Syncer is used to sync the runaway records.
type syncer struct {
	newWatchReader      *systemTableReader
	deletionWatchReader *systemTableReader
	sysSessionPool      util.SessionPool

	mu sync.Mutex
}

func newSyncer(sysSessionPool util.SessionPool) *syncer {
	return &syncer{
		sysSessionPool: sysSessionPool,
		newWatchReader: &systemTableReader{
			watchTableName,
			"start_time",
			NullTime},
		deletionWatchReader: &systemTableReader{watchDoneTableName,
			"done_time",
			NullTime},
	}
}

func (s *syncer) getWatchRecordByID(id int64) ([]*QuarantineRecord, error) {
	return s.getWatchRecord(s.newWatchReader, s.newWatchReader.genSelectByIDStmt(id), false)
}

func (s *syncer) getNewWatchRecords() ([]*QuarantineRecord, error) {
	return s.getWatchRecord(s.newWatchReader, s.newWatchReader.genSelectStmt, true)
}

func (s *syncer) getNewWatchDoneRecords() ([]*QuarantineRecord, error) {
	return s.getWatchDoneRecord(s.deletionWatchReader, s.deletionWatchReader.genSelectStmt, true)
}

func (s *syncer) getWatchRecord(reader *systemTableReader, sqlGenFn func() (string, []any), push bool) ([]*QuarantineRecord, error) {
	return getRunawayWatchRecord(s.sysSessionPool, reader, sqlGenFn, push)
}

func (s *syncer) getWatchDoneRecord(reader *systemTableReader, sqlGenFn func() (string, []any), push bool) ([]*QuarantineRecord, error) {
	return getRunawayWatchDoneRecord(s.sysSessionPool, reader, sqlGenFn, push)
}

func getRunawayWatchRecord(sysSessionPool util.SessionPool, reader *systemTableReader,
	sqlGenFn func() (string, []any), push bool) ([]*QuarantineRecord, error) {
	rs, err := reader.Read(sysSessionPool, sqlGenFn)
	if err != nil {
		return nil, err
	}
	ret := make([]*QuarantineRecord, 0, len(rs))
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
		// If a TiDB write record slow, it will occur that the record which has earlier start time is inserted later than others.
		// So we start the scan a little earlier.
		if push {
			reader.CheckPoint = now.Add(-3 * watchSyncInterval)
		}
		ret = append(ret, qr)
	}
	return ret, nil
}

func getRunawayWatchDoneRecord(sysSessionPool util.SessionPool, reader *systemTableReader,
	sqlGenFn func() (string, []any), push bool) ([]*QuarantineRecord, error) {
	rs, err := reader.Read(sysSessionPool, sqlGenFn)
	if err != nil {
		return nil, err
	}
	length := len(rs)
	ret := make([]*QuarantineRecord, 0, length)
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
		// Ditto as getRunawayWatchRecord.
		if push {
			reader.CheckPoint = now.Add(-3 * watchSyncInterval)
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

func (r *systemTableReader) genSelectStmt() (string, []any) {
	var builder strings.Builder
	params := make([]any, 0, 1)
	builder.WriteString("select * from ")
	builder.WriteString(r.TableName)
	builder.WriteString(" where ")
	builder.WriteString(r.KeyCol)
	builder.WriteString(" > %? order by ")
	builder.WriteString(r.KeyCol)
	params = append(params, r.CheckPoint)
	return builder.String(), params
}

func (*systemTableReader) Read(sysSessionPool util.SessionPool, genFn func() (string, []any)) ([]chunk.Row, error) {
	sql, params := genFn()
	return ExecRCRestrictedSQL(sysSessionPool, sql, params)
}
