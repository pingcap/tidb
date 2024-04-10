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

package ttlworker

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/model"
	timerapi "github.com/pingcap/tidb/pkg/timer/api"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/ttl/session"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
)

const (
	timerKeyPrefix                 = "/tidb/ttl/physical_table/"
	timerHookClass                 = "tidb.ttl"
	fullRefreshTimersCacheInterval = 10 * time.Minute
	timerDelayDeleteInterval       = 10 * time.Minute
)

// TTLTimerData is the data stored in each timer for TTL
type TTLTimerData struct {
	TableID    int64 `json:"table_id"`
	PhysicalID int64 `json:"physical_id"`
}

// TTLTimersSyncer is used to sync timers for ttl
type TTLTimersSyncer struct {
	pool           sessionPool
	cli            timerapi.TimerClient
	key2Timers     map[string]*timerapi.TimerRecord
	lastPullTimers time.Time
	delayDelete    time.Duration
	lastSyncTime   time.Time
	lastSyncVer    int64
	nowFunc        func() time.Time
}

// NewTTLTimerSyncer creates a new TTLTimersSyncer
func NewTTLTimerSyncer(pool sessionPool, cli timerapi.TimerClient) *TTLTimersSyncer {
	return &TTLTimersSyncer{
		pool:        pool,
		cli:         cli,
		key2Timers:  make(map[string]*timerapi.TimerRecord),
		nowFunc:     time.Now,
		delayDelete: timerDelayDeleteInterval,
	}
}

// SetDelayDeleteInterval sets interval for delay delete a timer
// It's better not to delete a timer immediately when the related table is not exist. The reason is that information schema
// is synced asynchronously, the new created table's meta may not synced to the current node yet.
func (g *TTLTimersSyncer) SetDelayDeleteInterval(interval time.Duration) {
	g.delayDelete = interval
}

// ManualTriggerTTLTimer triggers a TTL job for a physical table which returns a function to wait the job done.
// This returned function returns a bool value to indicates whether the job is finished.
func (g *TTLTimersSyncer) ManualTriggerTTLTimer(ctx context.Context, tbl *cache.PhysicalTable) (func() (string, bool, error), error) {
	se, err := getSession(g.pool)
	if err != nil {
		return nil, err
	}
	defer se.Close()

	timer, err := g.syncOneTimer(ctx, se, tbl.Schema, tbl.TableInfo, tbl.PartitionDef, true)
	if err != nil {
		return nil, err
	}

	reqID, err := g.cli.ManualTriggerEvent(ctx, timer.ID)
	if err != nil {
		return nil, err
	}

	return func() (string, bool, error) {
		se, err = getSession(g.pool)
		if err != nil {
			return "", false, err
		}
		defer se.Close()

		if err = ctx.Err(); err != nil {
			return "", false, err
		}

		timer, err = g.cli.GetTimerByID(ctx, timer.ID)
		if err != nil {
			return "", false, err
		}

		if timer.ManualRequestID != reqID {
			return "", false, errors.Errorf("manual request failed to trigger, request not found")
		}

		if timer.IsManualRequesting() {
			if timeout := timer.ManualTimeout; timeout > 0 && time.Since(timer.ManualRequestTime) > timeout+5*time.Second {
				return "", false, errors.New("manual request timeout")
			}
			return "", false, nil
		}

		if timer.ManualEventID == "" {
			return "", false, errors.New("manual request failed to trigger, request cancelled")
		}

		jobID := timer.ManualEventID
		rows, err := se.ExecuteSQL(ctx, "select 1 from mysql.tidb_ttl_job_history where job_id=%?", jobID)
		if err != nil {
			return "", false, err
		}

		if len(rows) == 0 {
			return "", false, nil
		}

		return jobID, true, nil
	}, nil
}

// Reset resets the syncer's state
func (g *TTLTimersSyncer) Reset() {
	var zeroTime time.Time
	g.lastPullTimers = zeroTime
	g.lastSyncTime = zeroTime
	g.lastSyncVer = 0
	if len(g.key2Timers) > 0 {
		maps.Clear(g.key2Timers)
	}
}

// GetLastSyncInfo returns last sync time and information schema version
func (g *TTLTimersSyncer) GetLastSyncInfo() (time.Time, int64) {
	return g.lastSyncTime, g.lastSyncVer
}

// SyncTimers syncs timers with TTL tables
func (g *TTLTimersSyncer) SyncTimers(ctx context.Context, is infoschema.InfoSchema) {
	g.lastSyncTime = g.nowFunc()
	g.lastSyncVer = is.SchemaMetaVersion()
	if time.Since(g.lastPullTimers) > fullRefreshTimersCacheInterval {
		metrics.TTLFullRefreshTimersCounter.Inc()
		newKey2Timers := make(map[string]*timerapi.TimerRecord, len(g.key2Timers))
		timers, err := g.cli.GetTimers(ctx, timerapi.WithKeyPrefix(timerKeyPrefix))
		if err != nil {
			logutil.BgLogger().Error("failed to pull timers", zap.Error(err))
			return
		}

		for _, timer := range timers {
			newKey2Timers[timer.Key] = timer
		}
		g.key2Timers = newKey2Timers
		g.lastPullTimers = g.nowFunc()
	}

	se, err := getSession(g.pool)
	if err != nil {
		logutil.BgLogger().Error("failed to sync TTL timers", zap.Error(err))
		return
	}
	defer se.Close()

	currentTimerKeys := make(map[string]struct{})
	for _, dbName := range is.AllSchemaNames() {
		for _, tbl := range is.SchemaTables(dbName) {
			tblInfo := tbl.Meta()
			if tblInfo.State != model.StatePublic || tblInfo.TTLInfo == nil {
				continue
			}
			for _, key := range g.syncTimersForTable(ctx, se, dbName, tblInfo) {
				currentTimerKeys[key] = struct{}{}
			}
		}
	}

	for key, timer := range g.key2Timers {
		if _, ok := currentTimerKeys[key]; ok {
			continue
		}

		if time.Since(timer.CreateTime) > g.delayDelete {
			metrics.TTLSyncTimerCounter.Inc()
			if _, err = g.cli.DeleteTimer(ctx, timer.ID); err != nil {
				logutil.BgLogger().Error("failed to delete timer", zap.Error(err), zap.String("timerID", timer.ID))
			} else {
				delete(g.key2Timers, key)
			}
		} else if timer.Enable {
			metrics.TTLSyncTimerCounter.Inc()
			if err = g.cli.UpdateTimer(ctx, timer.ID, timerapi.WithSetEnable(false)); err != nil {
				logutil.BgLogger().Error("failed to disable timer", zap.Error(err), zap.String("timerID", timer.ID))
			}

			timer, err = g.cli.GetTimerByID(ctx, timer.ID)
			if err != nil {
				logutil.BgLogger().Error("failed to get timer", zap.Error(err), zap.String("timerID", timer.ID))
			} else {
				g.key2Timers[key] = timer
			}
		}
	}
}

func (g *TTLTimersSyncer) syncTimersForTable(ctx context.Context, se session.Session, schema model.CIStr, tblInfo *model.TableInfo) []string {
	if tblInfo.Partition == nil {
		key := buildTimerKey(tblInfo, nil)
		if _, err := g.syncOneTimer(ctx, se, schema, tblInfo, nil, false); err != nil {
			logutil.BgLogger().Error("failed to syncOneTimer", zap.Error(err), zap.String("key", key))
		}
		return []string{key}
	}

	defs := tblInfo.Partition.Definitions
	keys := make([]string, 0, len(defs))
	for i := range defs {
		partition := &defs[i]
		key := buildTimerKey(tblInfo, partition)
		keys = append(keys, key)
		if _, err := g.syncOneTimer(ctx, se, schema, tblInfo, partition, false); err != nil {
			logutil.BgLogger().Error("failed to syncOneTimer", zap.Error(err), zap.String("key", key))
		}
	}
	return keys
}

func (g *TTLTimersSyncer) shouldSyncTimer(timer *timerapi.TimerRecord, schema model.CIStr, tblInfo *model.TableInfo, partition *model.PartitionDefinition) bool {
	if timer == nil {
		return true
	}

	tags := getTimerTags(schema, tblInfo, partition)
	ttlInfo := tblInfo.TTLInfo
	return !slices.Equal(timer.Tags, tags) ||
		timer.Enable != ttlInfo.Enable ||
		timer.SchedPolicyExpr != ttlInfo.JobInterval
}

func (g *TTLTimersSyncer) syncOneTimer(ctx context.Context, se session.Session, schema model.CIStr, tblInfo *model.TableInfo, partition *model.PartitionDefinition, skipCache bool) (*timerapi.TimerRecord, error) {
	key := buildTimerKey(tblInfo, partition)
	tags := getTimerTags(schema, tblInfo, partition)
	ttlInfo := tblInfo.TTLInfo

	if !skipCache {
		timer, ok := g.key2Timers[key]
		if ok && !g.shouldSyncTimer(timer, schema, tblInfo, partition) {
			return timer, nil
		}
	}

	metrics.TTLSyncTimerCounter.Inc()
	timer, err := g.cli.GetTimerByKey(ctx, key)
	if err != nil && !errors.ErrorEqual(err, timerapi.ErrTimerNotExist) {
		return nil, err
	}

	if errors.ErrorEqual(err, timerapi.ErrTimerNotExist) {
		delete(g.key2Timers, key)
		var watermark time.Time
		ttlTableStatus, err := getTTLTableStatus(ctx, se, tblInfo, partition)
		if err != nil {
			logutil.BgLogger().Warn("failed to get TTL table status", zap.Error(err), zap.String("key", key))
		}

		if ttlTableStatus != nil {
			if ttlTableStatus.CurrentJobID != "" {
				watermark = ttlTableStatus.CurrentJobStartTime
			} else {
				watermark = ttlTableStatus.LastJobStartTime
			}
		}

		dataObj := &TTLTimerData{
			TableID:    tblInfo.ID,
			PhysicalID: tblInfo.ID,
		}

		if partition != nil {
			dataObj.PhysicalID = partition.ID
		}

		data, err := json.Marshal(dataObj)
		if err != nil {
			return nil, err
		}

		timer, err = g.cli.CreateTimer(ctx, timerapi.TimerSpec{
			Key:             key,
			Tags:            tags,
			Data:            data,
			SchedPolicyType: timerapi.SchedEventInterval,
			SchedPolicyExpr: ttlInfo.JobInterval,
			HookClass:       timerHookClass,
			Watermark:       watermark,
			Enable:          ttlInfo.Enable,
		})
		if err != nil {
			return nil, err
		}
		g.key2Timers[key] = timer
		return timer, nil
	}

	g.key2Timers[key] = timer
	if !g.shouldSyncTimer(timer, schema, tblInfo, partition) {
		return timer, nil
	}

	err = g.cli.UpdateTimer(ctx, timer.ID,
		timerapi.WithSetTags(tags),
		timerapi.WithSetSchedExpr(timerapi.SchedEventInterval, tblInfo.TTLInfo.JobInterval),
		timerapi.WithSetEnable(tblInfo.TTLInfo.Enable),
	)

	if err != nil {
		logutil.BgLogger().Error("failed to update timer",
			zap.Error(err),
			zap.String("timerID", timer.ID),
			zap.String("key", key),
			zap.Strings("tags", tags),
		)
		return nil, err
	}

	timer, err = g.cli.GetTimerByID(ctx, timer.ID)
	if err != nil {
		return nil, err
	}

	g.key2Timers[timer.Key] = timer
	return timer, nil
}

func getTimerTags(schema model.CIStr, tblInfo *model.TableInfo, partition *model.PartitionDefinition) []string {
	dbTag := fmt.Sprintf("db=%s", schema.O)
	tblTag := fmt.Sprintf("table=%s", tblInfo.Name.O)
	if partition != nil {
		return []string{
			dbTag, tblTag,
			fmt.Sprintf("partition=%s", partition.Name.O),
		}
	}

	return []string{dbTag, tblTag}
}

func buildTimerKey(tblInfo *model.TableInfo, partition *model.PartitionDefinition) string {
	physicalID := tblInfo.ID
	if partition != nil {
		physicalID = partition.ID
	}
	return buildTimerKeyWithID(tblInfo.ID, physicalID)
}

func buildTimerKeyWithID(tblID, physicalID int64) string {
	return fmt.Sprintf("%s%d/%d", timerKeyPrefix, tblID, physicalID)
}

func getTTLTableStatus(ctx context.Context, se session.Session, tblInfo *model.TableInfo, partition *model.PartitionDefinition) (*cache.TableStatus, error) {
	pid := tblInfo.ID
	if partition != nil {
		pid = partition.ID
	}

	sql, args := cache.SelectFromTTLTableStatusWithID(pid)
	rows, err := se.ExecuteSQL(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return nil, nil
	}

	return cache.RowToTableStatus(se, rows[0])
}
