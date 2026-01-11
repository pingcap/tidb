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
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/session/syssession"
	timerapi "github.com/pingcap/tidb/pkg/timer/api"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/ttl/session"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	fullRefreshTimersCacheInterval = 10 * time.Minute
	timerDelayDeleteInterval       = 10 * time.Minute
)

// TTLTimerData is the data stored in each timer for TTL.
type TTLTimerData struct {
	TableID    int64 `json:"table_id"`
	PhysicalID int64 `json:"physical_id"`
}

type timersSyncerBase struct {
	pool           syssession.Pool
	cli            timerapi.TimerClient
	cfg            timersSyncerConfig
	key2Timers     map[string]*timerapi.TimerRecord
	lastPullTimers time.Time
	delayDelete    time.Duration
	lastSyncTime   time.Time
	lastSyncVer    int64
	nowFunc        func() time.Time
}

type timersSyncerConfig struct {
	keyPrefix string
	attr      infoschemacontext.SpecialAttributeFilter
	hookClass string

	shouldSyncTable func(tblInfo *model.TableInfo) bool
	getEnable       func(tblInfo *model.TableInfo) bool
	getSchedPolicy  func(tblInfo *model.TableInfo) (timerapi.SchedPolicyType, string)
	getWatermark    func(ctx context.Context, se session.Session, tblInfo *model.TableInfo, partition *model.PartitionDefinition) (time.Time, error)
}

func newTimersSyncerBase(pool syssession.Pool, cli timerapi.TimerClient, cfg timersSyncerConfig) (timersSyncerBase, error) {
	if cfg.keyPrefix == "" {
		return timersSyncerBase{}, errors.New("timersSyncerConfig: keyPrefix is empty")
	}
	if cfg.attr == nil {
		return timersSyncerBase{}, errors.New("timersSyncerConfig: attr is nil")
	}
	if cfg.hookClass == "" {
		return timersSyncerBase{}, errors.New("timersSyncerConfig: hookClass is empty")
	}
	if cfg.shouldSyncTable == nil {
		cfg.shouldSyncTable = func(*model.TableInfo) bool { return true }
	}
	if cfg.getEnable == nil {
		return timersSyncerBase{}, errors.New("timersSyncerConfig: getEnable is nil")
	}
	if cfg.getSchedPolicy == nil {
		return timersSyncerBase{}, errors.New("timersSyncerConfig: getSchedPolicy is nil")
	}
	return timersSyncerBase{
		pool:        pool,
		cli:         cli,
		cfg:         cfg,
		key2Timers:  make(map[string]*timerapi.TimerRecord),
		nowFunc:     time.Now,
		delayDelete: timerDelayDeleteInterval,
	}, nil
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
	var timerID string
	var reqID string
	err := withSession(g.pool, func(se session.Session) error {
		timer, err := g.syncOneTimer(ctx, se, tbl.Schema, tbl.TableInfo, tbl.PartitionDef, true)
		if err != nil {
			return err
		}

		timerID = timer.ID

		reqID, err = g.cli.ManualTriggerEvent(ctx, timer.ID)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return func() (string, bool, error) {
		if err := ctx.Err(); err != nil {
			return "", false, err
		}

		timer, err := g.cli.GetTimerByID(ctx, timerID)
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
		found := false
		err = withSession(g.pool, func(se session.Session) error {
			rows, err := se.ExecuteSQL(ctx, "select 1 from mysql.tidb_ttl_job_history where job_id=%?", jobID)
			if err != nil {
				return err
			}
			found = len(rows) > 0
			return nil
		})

		if err != nil {
			return "", false, err
		}

		if !found {
			return "", false, nil
		}

		return jobID, true, nil
	}, nil
}

// Reset resets the syncer's state.
func (g *timersSyncerBase) Reset() {
	var zeroTime time.Time
	g.lastPullTimers = zeroTime
	g.lastSyncTime = zeroTime
	g.lastSyncVer = 0
	if len(g.key2Timers) > 0 {
		clear(g.key2Timers)
	}
}

// GetLastSyncInfo returns last sync time and information schema version.
func (g *timersSyncerBase) GetLastSyncInfo() (time.Time, int64) {
	return g.lastSyncTime, g.lastSyncVer
}

// GetCachedTimerRecord returns a cached timer by key
func (g *TTLTimersSyncer) GetCachedTimerRecord(key string) (r *timerapi.TimerRecord, ok bool) {
	r, ok = g.key2Timers[key]
	return
}

// SyncTimers syncs timers with tables.
func (g *timersSyncerBase) SyncTimers(ctx context.Context, is infoschema.InfoSchema) {
	cfg := g.cfg

	g.lastSyncTime = g.nowFunc()
	g.lastSyncVer = is.SchemaMetaVersion()
	if time.Since(g.lastPullTimers) > fullRefreshTimersCacheInterval {
		metrics.TTLFullRefreshTimersCounter.Inc()
		newKey2Timers := make(map[string]*timerapi.TimerRecord, len(g.key2Timers))
		timers, err := g.cli.GetTimers(ctx, timerapi.WithKeyPrefix(cfg.keyPrefix))
		if err != nil {
			logutil.BgLogger().Warn("failed to pull timers", zap.Error(err), zap.String("timer", cfg.keyPrefix))
			return
		}

		for _, timer := range timers {
			newKey2Timers[timer.Key] = timer
		}
		g.key2Timers = newKey2Timers
		g.lastPullTimers = g.nowFunc()
	}

	currentTimerKeys := make(map[string]struct{})
	err := withSession(g.pool, func(se session.Session) error {
		ch := is.ListTablesWithSpecialAttribute(cfg.attr)
		for _, v := range ch {
			for _, tblInfo := range v.TableInfos {
				for _, key := range g.syncTimersForTable(ctx, se, v.DBName, tblInfo) {
					currentTimerKeys[key] = struct{}{}
				}
			}
		}
		return nil
	})

	if err != nil {
		logutil.BgLogger().Error("failed to sync timers", zap.Error(err), zap.String("timer", cfg.keyPrefix))
		return
	}

	for key, timer := range g.key2Timers {
		if _, ok := currentTimerKeys[key]; ok {
			continue
		}

		timerID := timer.ID
		if time.Since(timer.CreateTime) > g.delayDelete {
			metrics.TTLSyncTimerCounter.Inc()
			if _, err := g.cli.DeleteTimer(ctx, timerID); err != nil {
				logutil.BgLogger().Error("failed to delete timer", zap.Error(err), zap.String("timerID", timerID))
			} else {
				delete(g.key2Timers, key)
			}
		} else if timer.Enable {
			metrics.TTLSyncTimerCounter.Inc()
			if err := g.cli.UpdateTimer(ctx, timerID, timerapi.WithSetEnable(false)); err != nil {
				logutil.BgLogger().Error("failed to disable timer", zap.Error(err), zap.String("timerID", timerID))
			}

			timer, err := g.cli.GetTimerByID(ctx, timerID)
			if errors.ErrorEqual(err, timerapi.ErrTimerNotExist) {
				delete(g.key2Timers, key)
			} else if err != nil {
				logutil.BgLogger().Error("failed to get timer", zap.Error(err), zap.String("timerID", timerID))
			} else {
				g.key2Timers[key] = timer
			}
		}
	}
}

func (g *timersSyncerBase) syncTimersForTable(ctx context.Context, se session.Session, schema ast.CIStr, tblInfo *model.TableInfo) []string {
	cfg := g.cfg
	if !cfg.shouldSyncTable(tblInfo) {
		return nil
	}

	if tblInfo.Partition == nil {
		key := buildTimerKey(cfg.keyPrefix, tblInfo, nil)
		if _, err := g.syncOneTimer(ctx, se, schema, tblInfo, nil, false); err != nil {
			logutil.BgLogger().Error("failed to syncOneTimer", zap.Error(err), zap.String("key", key))
		}
		return []string{key}
	}

	defs := tblInfo.Partition.Definitions
	keys := make([]string, 0, len(defs))
	for i := range defs {
		partition := &defs[i]
		key := buildTimerKey(cfg.keyPrefix, tblInfo, partition)
		keys = append(keys, key)
		if _, err := g.syncOneTimer(ctx, se, schema, tblInfo, partition, false); err != nil {
			logutil.BgLogger().Error("failed to syncOneTimer", zap.Error(err), zap.String("key", key))
		}
	}
	return keys
}

func (g *timersSyncerBase) shouldSyncTimer(timer *timerapi.TimerRecord, tags []string, enable bool, policyType timerapi.SchedPolicyType, policyExpr string) bool {
	if timer == nil {
		return true
	}

	return !slices.Equal(timer.Tags, tags) ||
		timer.Enable != enable ||
		timer.SchedPolicyType != policyType ||
		timer.SchedPolicyExpr != policyExpr
}

func (g *timersSyncerBase) syncOneTimer(
	ctx context.Context,
	se session.Session,
	schema ast.CIStr,
	tblInfo *model.TableInfo,
	partition *model.PartitionDefinition,
	skipCache bool,
) (*timerapi.TimerRecord, error) {
	cfg := g.cfg
	enable := cfg.getEnable(tblInfo)
	policyType, policyExpr := cfg.getSchedPolicy(tblInfo)
	watermark := time.Time{}
	if cfg.getWatermark != nil {
		wm, err := cfg.getWatermark(ctx, se, tblInfo, partition)
		if err != nil {
			logutil.BgLogger().Warn("failed to get timer watermark", zap.Error(err), zap.String("timer", cfg.keyPrefix))
		} else {
			watermark = wm
		}
	}

	key := buildTimerKey(cfg.keyPrefix, tblInfo, partition)
	tags := getTimerTags(schema, tblInfo, partition)

	if !skipCache {
		timer, ok := g.key2Timers[key]
		if ok && !g.shouldSyncTimer(timer, tags, enable, policyType, policyExpr) {
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

		dataObj := &TTLTimerData{TableID: tblInfo.ID, PhysicalID: tblInfo.ID}
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
			SchedPolicyType: policyType,
			SchedPolicyExpr: policyExpr,
			HookClass:       cfg.hookClass,
			Watermark:       watermark,
			Enable:          enable,
		})
		if err != nil {
			return nil, err
		}
		g.key2Timers[key] = timer
		return timer, nil
	}

	g.key2Timers[key] = timer
	if !g.shouldSyncTimer(timer, tags, enable, policyType, policyExpr) {
		return timer, nil
	}

	err = g.cli.UpdateTimer(ctx, timer.ID,
		timerapi.WithSetTags(tags),
		timerapi.WithSetSchedExpr(policyType, policyExpr),
		timerapi.WithSetEnable(enable),
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

func buildTimerKey(prefix string, tblInfo *model.TableInfo, partition *model.PartitionDefinition) string {
	physicalID := tblInfo.ID
	if partition != nil {
		physicalID = partition.ID
	}
	return buildTimerKeyWithID(prefix, tblInfo.ID, physicalID)
}

func buildTimerKeyWithID(prefix string, tblID, physicalID int64) string {
	return fmt.Sprintf("%s%d/%d", prefix, tblID, physicalID)
}

func getTimerTags(schema ast.CIStr, tblInfo *model.TableInfo, partition *model.PartitionDefinition) []string {
	dbTag := fmt.Sprintf("db=%s", schema.O)
	tblTag := fmt.Sprintf("table=%s", tblInfo.Name.O)
	if partition != nil {
		return []string{dbTag, tblTag, fmt.Sprintf("partition=%s", partition.Name.O)}
	}
	return []string{dbTag, tblTag}
}
