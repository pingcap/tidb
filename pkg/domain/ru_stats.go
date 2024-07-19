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
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const (
	maxRetryCount   int           = 10
	ruStatsInterval time.Duration = 24 * time.Hour
	// only keep stats rows for last 3 months(92 days at most).
	ruStatsGCDuration time.Duration = 92 * ruStatsInterval
	gcBatchSize       int64         = 1000
)

// RUStatsWriter represents a write to write ru historical data into mysql.request_unit_by_group.
type RUStatsWriter struct {
	// make some fields public for unit test.
	Interval  time.Duration
	RMClient  pd.ResourceManagerClient
	InfoCache *infoschema.InfoCache
	store     kv.Storage
	sessPool  *sessionPool
	// current time, cache it here to make unit test easier.
	StartTime time.Time
}

// NewRUStatsWriter build a RUStatsWriter from Domain.
func NewRUStatsWriter(do *Domain) *RUStatsWriter {
	return &RUStatsWriter{
		Interval:  ruStatsInterval,
		RMClient:  do.GetPDClient(),
		InfoCache: do.infoCache,
		store:     do.store,
		sessPool:  do.sysSessionPool,
	}
}

func (do *Domain) requestUnitsWriterLoop() {
	// do not start flush loop in unit test.
	if intest.InTest {
		return
	}
	ruWriter := NewRUStatsWriter(do)
	for {
		start := time.Now()
		count := 0
		lastTime := GetLastExpectedTime(start, ruWriter.Interval)
		if do.DDL().OwnerManager().IsOwner() {
			var err error
			for {
				ruWriter.StartTime = time.Now()
				err = ruWriter.DoWriteRUStatistics(context.Background())
				if err == nil {
					break
				}
				logutil.BgLogger().Error("failed to insert request_unit_by_group data", zap.Error(err), zap.Int("retry", count))
				count++
				if count > maxRetryCount {
					break
				}
				time.Sleep(time.Second)
			}
			// try gc outdated rows
			if err := ruWriter.GCOutdatedRecords(lastTime); err != nil {
				logutil.BgLogger().Warn("[ru_stats] gc outdated rowd failed, will try next time.", zap.Error(err))
			}

			logutil.BgLogger().Info("[ru_stats] finish write ru historical data", zap.String("end_time", lastTime.Format(time.DateTime)),
				zap.Stringer("interval", ruStatsInterval), zap.Stringer("cost", time.Since(start)), zap.Error(err))
		}

		nextTime := lastTime.Add(ruStatsInterval)
		dur := time.Until(nextTime)
		timer := time.NewTimer(dur)
		select {
		case <-do.exit:
			return
		case <-timer.C:
		}
	}
}

// GetLastExpectedTime return the last written ru time.
// NOTE:
//   - due to DST(daylight saving time), the actual duration for a specific
//     time may be shorter or longer than the interval when DST happens.
//   - All the tidb-server should be deployed in the same timezone to ensure
//     the duration is calculated correctly.
//   - The interval must not be longer than 24h.
func GetLastExpectedTime(now time.Time, interval time.Duration) time.Time {
	return GetLastExpectedTimeTZ(now, interval, time.Local)
}

// GetLastExpectedTimeTZ return the last written ru time under specifical timezone.
// make it public only for test.
func GetLastExpectedTimeTZ(now time.Time, interval time.Duration, tz *time.Location) time.Time {
	if tz == nil {
		tz = time.Local
	}
	year, month, day := now.Date()
	start := time.Date(year, month, day, 0, 0, 0, 0, tz)
	// cast to int64 to bypass the durationcheck lint.
	count := int64(now.Sub(start) / interval)
	targetDur := time.Duration(count) * interval
	// use UTC timezone to calculate target time so it can be compatible with DST.
	return start.In(time.UTC).Add(targetDur).In(tz)
}

// DoWriteRUStatistics write ru historical data into mysql.request_unit_by_group.
func (r *RUStatsWriter) DoWriteRUStatistics(ctx context.Context) error {
	// check if is already inserted
	lastEndTime := GetLastExpectedTime(r.StartTime, r.Interval)
	isInserted, err := r.isLatestDataInserted(lastEndTime)
	if err != nil {
		return err
	}
	if isInserted {
		logutil.BgLogger().Info("[ru_stats] ru data is already inserted, skip", zap.Stringer("end_time", lastEndTime))
		return nil
	}

	lastStats, err := r.loadLatestRUStats()
	if err != nil {
		return err
	}
	needFetchData := true
	if lastStats != nil && lastStats.Latest != nil {
		needFetchData = lastStats.Latest.EndTime != lastEndTime
	}

	ruStats := lastStats
	if needFetchData {
		stats, err := r.fetchResourceGroupStats(ctx)
		if err != nil {
			return err
		}
		ruStats = &meta.RUStats{
			Latest: &meta.DailyRUStats{
				EndTime: lastEndTime,
				Stats:   stats,
			},
		}
		if lastStats != nil {
			ruStats.Previous = lastStats.Latest
		}
		err = r.persistLatestRUStats(ruStats)
		if err != nil {
			return err
		}
	}
	return r.insertRUStats(ruStats)
}

func (r *RUStatsWriter) fetchResourceGroupStats(ctx context.Context) ([]meta.GroupRUStats, error) {
	groups, err := r.RMClient.ListResourceGroups(ctx, pd.WithRUStats)
	if err != nil {
		return nil, errors.Trace(err)
	}
	infos := r.InfoCache.GetLatest()
	res := make([]meta.GroupRUStats, 0, len(groups))
	for _, g := range groups {
		groupInfo, exists := infos.ResourceGroupByName(model.NewCIStr(g.Name))
		if !exists {
			continue
		}
		res = append(res, meta.GroupRUStats{
			ID:            groupInfo.ID,
			Name:          groupInfo.Name.O,
			RUConsumption: g.RUStats,
		})
	}
	return res, nil
}

func (r *RUStatsWriter) loadLatestRUStats() (*meta.RUStats, error) {
	snapshot := r.store.GetSnapshot(kv.MaxVersion)
	metaStore := meta.NewSnapshotMeta(snapshot)
	return metaStore.GetRUStats()
}

func (r *RUStatsWriter) persistLatestRUStats(stats *meta.RUStats) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	return kv.RunInNewTxn(ctx, r.store, true, func(_ context.Context, txn kv.Transaction) error {
		return meta.NewMeta(txn).SetRUStats(stats)
	})
}

func (r *RUStatsWriter) isLatestDataInserted(lastEndTime time.Time) (bool, error) {
	end := lastEndTime.Format(time.DateTime)
	start := lastEndTime.Add(-ruStatsInterval).Format(time.DateTime)
	rows, sqlErr := execRestrictedSQL(r.sessPool, "SELECT 1 from mysql.request_unit_by_group where start_time = %? and end_time = %? limit 1", []any{start, end})
	if sqlErr != nil {
		return false, errors.Trace(sqlErr)
	}
	return len(rows) > 0, nil
}

func (r *RUStatsWriter) insertRUStats(stats *meta.RUStats) error {
	sql := generateSQL(stats)
	if sql == "" {
		return nil
	}

	_, err := execRestrictedSQL(r.sessPool, sql, nil)
	return err
}

// GCOutdatedRecords delete outdated records from target table.
func (r *RUStatsWriter) GCOutdatedRecords(lastEndTime time.Time) error {
	gcEndDate := lastEndTime.Add(-ruStatsGCDuration).Format(time.DateTime)
	countSQL := fmt.Sprintf("SELECT count(*) FROM mysql.request_unit_by_group where end_time <= '%s'", gcEndDate)
	rows, err := execRestrictedSQL(r.sessPool, countSQL, nil)
	if err != nil {
		return errors.Trace(err)
	}
	totalCount := rows[0].GetInt64(0)

	loopCount := (totalCount + gcBatchSize - 1) / gcBatchSize
	for i := int64(0); i < loopCount; i++ {
		sql := fmt.Sprintf("DELETE FROM mysql.request_unit_by_group where end_time <= '%s' order by end_time limit %d", gcEndDate, gcBatchSize)
		_, err = execRestrictedSQL(r.sessPool, sql, nil)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func generateSQL(stats *meta.RUStats) string {
	var buf strings.Builder
	buf.WriteString("REPLACE INTO mysql.request_unit_by_group(start_time, end_time, resource_group, total_ru) VALUES ")
	prevStats := make(map[string]meta.GroupRUStats)
	if stats.Previous != nil {
		for _, g := range stats.Previous.Stats {
			if g.RUConsumption != nil {
				prevStats[g.Name] = g
			}
		}
	}
	end := stats.Latest.EndTime.Format(time.DateTime)
	start := stats.Latest.EndTime.Add(-ruStatsInterval).Format(time.DateTime)
	count := 0
	for _, g := range stats.Latest.Stats {
		if g.RUConsumption == nil {
			logutil.BgLogger().Warn("group ru consumption statistics data is empty", zap.String("name", g.Name), zap.Int64("id", g.ID))
			continue
		}
		ru := g.RUConsumption.RRU + g.RUConsumption.WRU
		if prev, ok := prevStats[g.Name]; ok && prev.RUConsumption != nil && g.ID == prev.ID {
			ru -= prev.RUConsumption.RRU + prev.RUConsumption.WRU
		}
		// ignore too small delta value
		if ru < 1.0 {
			continue
		}
		if count > 0 {
			buf.WriteRune(',')
		}
		rowData := fmt.Sprintf(`("%s", "%s", "%s", %d)`, start, end, g.Name, int64(ru))
		buf.WriteString(rowData)
		count++
	}
	if count == 0 {
		return ""
	}
	buf.WriteRune(';')
	return buf.String()
}
