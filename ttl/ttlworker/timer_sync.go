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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	timerapi "github.com/pingcap/tidb/timer/api"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/ttl/session"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
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
}

// NewTTLTimerSyncer creates a new TTLTimersSyncer
func NewTTLTimerSyncer(pool sessionPool, cli timerapi.TimerClient) *TTLTimersSyncer {
	return &TTLTimersSyncer{
		pool:        pool,
		cli:         cli,
		key2Timers:  make(map[string]*timerapi.TimerRecord),
		delayDelete: timerDelayDeleteInterval,
	}
}

// SetDelayDeleteInterval sets interval for delay delete a timer
// It's better not to delete a timer immediately when the related table is not exist. The reason is that information schema
// is synced asynchronously, the new created table's meta may not synced to the current node yet.
func (g *TTLTimersSyncer) SetDelayDeleteInterval(interval time.Duration) {
	g.delayDelete = interval
}

// SyncTimers syncs timers with TTL tables
func (g *TTLTimersSyncer) SyncTimers(ctx context.Context, is infoschema.InfoSchema) {
	if time.Since(g.lastPullTimers) > fullRefreshTimersCacheInterval {
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
		g.lastPullTimers = time.Now()
	}

	se, err := getSession(g.pool)
	if err != nil {
		logutil.BgLogger().Error("failed to sync TTL timers", zap.Error(err))
		return
	}
	defer se.Close()

	currentTimerKeys := make(map[string]struct{})
	for _, db := range is.AllSchemas() {
		for _, tbl := range is.SchemaTables(db.Name) {
			tblInfo := tbl.Meta()
			if tblInfo.State != model.StatePublic || tblInfo.TTLInfo == nil {
				continue
			}
			for _, key := range g.syncTimersForTable(ctx, se, db.Name, tblInfo) {
				currentTimerKeys[key] = struct{}{}
			}
		}
	}

	for key, timer := range g.key2Timers {
		if _, ok := currentTimerKeys[key]; ok {
			continue
		}

		if time.Since(timer.CreateTime) > g.delayDelete {
			if _, err = g.cli.DeleteTimer(ctx, timer.ID); err != nil {
				logutil.BgLogger().Error("failed to delete timer", zap.Error(err), zap.String("timerID", timer.ID))
			}
		}
	}
}

func (g *TTLTimersSyncer) syncTimersForTable(ctx context.Context, se session.Session, schema model.CIStr, tblInfo *model.TableInfo) []string {
	if tblInfo.Partition == nil {
		return []string{g.syncOneTimer(ctx, se, schema, tblInfo, nil)}
	}

	defs := tblInfo.Partition.Definitions
	keys := make([]string, 0, len(defs))
	for i := range defs {
		keys = append(
			keys,
			g.syncOneTimer(ctx, se, schema, tblInfo, &defs[i]),
		)
	}
	return keys
}

func (g *TTLTimersSyncer) syncOneTimer(ctx context.Context, se session.Session, schema model.CIStr, tblInfo *model.TableInfo, partition *model.PartitionDefinition) (key string) {
	key = buildTimerKey(tblInfo, partition)
	tags := getTimerTags(schema, tblInfo, partition)
	ttlInfo := tblInfo.TTLInfo
	existTimer, ok := g.key2Timers[key]
	if ok && slices.Equal(existTimer.Tags, tags) && existTimer.Enable == ttlInfo.Enable && existTimer.SchedPolicyExpr == ttlInfo.JobInterval {
		return
	}

	timer, err := g.cli.GetTimerByKey(ctx, key)
	if err != nil && !errors.ErrorEqual(err, timerapi.ErrTimerNotExist) {
		logutil.BgLogger().Error("failed to get timer for TTL table", zap.Error(err), zap.String("key", key))
		return
	}

	if errors.ErrorEqual(err, timerapi.ErrTimerNotExist) {
		var watermark time.Time
		ttlTableStatus, err := getTTLTableStatus(ctx, se, tblInfo, partition)
		if err != nil {
			logutil.BgLogger().Warn("failed to get TTL table status", zap.Error(err), zap.String("key", key))
		}

		if ttlTableStatus != nil {
			watermark = ttlTableStatus.LastJobStartTime
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
			logutil.BgLogger().Error("failed to marshal TTL data object", zap.Error(err))
			return
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
			logutil.BgLogger().Error("failed to create new timer",
				zap.Error(err),
				zap.String("key", key),
				zap.Strings("tags", tags),
			)
			return
		}
		g.key2Timers[key] = timer
		return
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
		return
	}

	timer, err = g.cli.GetTimerByID(ctx, timer.ID)
	if err != nil {
		logutil.BgLogger().Error("failed to get timer",
			zap.Error(err),
			zap.String("timerID", timer.ID),
		)
		return
	}

	g.key2Timers[timer.Key] = timer
	return
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
	return fmt.Sprintf("%s%d/%d", timerKeyPrefix, tblInfo.ID, physicalID)
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
