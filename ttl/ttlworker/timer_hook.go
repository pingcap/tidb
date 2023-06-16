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
	"sync"
	"time"

	"golang.org/x/exp/slices"

	"github.com/pingcap/errors"
	timerapi "github.com/pingcap/tidb/timer/api"
	timerrt "github.com/pingcap/tidb/timer/runtime"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	ttlTimerKeyPrefix = "/ttl/"
	ttlTimerHookClass = "ttlTimer"
)

var (
	ttlTimerDefaultRetry = timerapi.PreSchedEventResult{Delay: time.Minute}
)

type ttlJobBrief struct {
	ID        string
	StartTime time.Time
	Finished  bool
}

type ttlTimerHelper interface {
	CanTriggerTTLJob(physicalID int64, now time.Time) (bool, string)
	GetMaxStartTimeJob(ctx context.Context, physicalID int64) (*ttlJobBrief, error)
	GetUnfinishedJob(ctx context.Context, physicalID int64) (*ttlJobBrief, error)
	TriggerJob(ctx context.Context, tableID, partitionID int64, checkMaxStartTime time.Time) (*ttlJobBrief, error)
}

type ttlTimerContext struct {
	PhysicalID int64 `json:"physical_id"`
	TableID    int64 `json:"table_id"`
}

func decodeTTLTimerContext(data []byte) (*ttlTimerContext, error) {
	var ctx ttlTimerContext
	if err := json.Unmarshal(data, &ctx); err != nil {
		return nil, errors.Trace(err)
	}

	return &ctx, nil
}

func (c *ttlTimerContext) encode() ([]byte, error) {
	return json.Marshal(c)
}

type ttlTimerHook struct {
	logger        *zap.Logger
	ctx           context.Context
	cancel        func()
	wg            util.WaitGroupWrapper
	helper        ttlTimerHelper
	cli           timerapi.TimerClient
	currentEvents sync.Map
	stop          struct {
		sync.Mutex
		v bool
	}
}

func newTTLTimerHook(cli timerapi.TimerClient, helper ttlTimerHelper) *ttlTimerHook {
	ctx, cancel := context.WithCancel(context.Background())
	return &ttlTimerHook{
		logger: logutil.BgLogger(),
		ctx:    ctx,
		cancel: cancel,
		cli:    cli,
		helper: helper,
	}
}

func (h *ttlTimerHook) Start() {}

func (h *ttlTimerHook) Stop() {}

func (h *ttlTimerHook) OnPreSchedEvent(ctx context.Context, event timerapi.TimerShedEvent) (timerapi.PreSchedEventResult, error) {
	ttlCtx, err := decodeTTLTimerContext(event.Timer().Data)
	if err != nil {
		return ttlTimerDefaultRetry, err
	}

	ok, _, err := h.checkShouldScheduleEvent(ctx, ttlCtx.PhysicalID)
	if err != nil || !ok {
		return ttlTimerDefaultRetry, err
	}

	return timerapi.PreSchedEventResult{}, nil
}

func (h *ttlTimerHook) OnSchedEvent(ctx context.Context, event timerapi.TimerShedEvent) (err error) {
	logger := h.getEventLogger(event)
	logger.Info("start to schedule TTL timer event")
	defer func() {
		if err != nil {
			logger.Error("failed to schedule TTL timer event", zap.Error(err))
		}
	}()

	timer := event.Timer()
	ttlCtx, err := decodeTTLTimerContext(timer.Data)
	if err != nil {
		logger.Error("failed to decode timer context", zap.Error(err), zap.ByteString("data", timer.Data))
		return err
	}

	physicalID := ttlCtx.PhysicalID
	ok, reason, err := h.checkShouldScheduleEvent(ctx, physicalID)
	if err != nil {
		return err
	}

	if !ok {
		logger.Warn("Skip triggering TTL job because it is not allowed to schedule now", zap.String("reason", reason))
		return h.cli.CloseTimerEvent(ctx, timer.ID, timer.EventID, timerapi.WithKeepWatermarkUnchanged())
	}

	job, err := h.helper.GetMaxStartTimeJob(ctx, physicalID)
	if err != nil {
		return err
	}

	existJobStartTimeShouldBefore := timer.Watermark.Add(time.Minute)
	if job != nil && job.StartTime.After(existJobStartTimeShouldBefore) {
		logger.Info(
			"Skip triggering TTL job for a job with a larger start time is found",
			zap.Time("shouldBefore", existJobStartTimeShouldBefore),
			zap.String("jobID", job.ID),
			zap.Time("jobStartTime", job.StartTime),
		)
		return h.cli.CloseTimerEvent(ctx, timer.ID, timer.EventID, timerapi.WithSetWatermark(job.StartTime))
	}

	var partitionID int64
	if physicalID != ttlCtx.TableID {
		partitionID = physicalID
	}

	job, err = h.helper.TriggerJob(ctx, ttlCtx.TableID, partitionID, existJobStartTimeShouldBefore)
	if err != nil {
		return err
	}

	if err = h.cli.CloseTimerEvent(ctx, timer.ID, timer.EventID, timerapi.WithSetWatermark(job.StartTime)); err != nil {
		return err
	}

	logger.Info(
		"finished to trigger TTL job",
		zap.String("jobID", job.ID),
		zap.Time("jobStartTime", job.StartTime),
	)
	return nil
}

func (h *ttlTimerHook) getEventLogger(event timerapi.TimerShedEvent) *zap.Logger {
	timer := event.Timer()
	return logutil.BgLogger().With(
		zap.String("timerKey", timer.Key),
		zap.Strings("tags", timer.Tags),
		zap.Time("watermark", timer.Watermark),
		zap.String("interval", timer.SchedPolicyExpr),
		zap.String("eventID", event.EventID()),
		zap.Time("eventStart", timer.EventStart),
	)
}

func (h *ttlTimerHook) checkShouldScheduleEvent(ctx context.Context, tblID int64) (bool, string, error) {
	if ok, reason := h.helper.CanTriggerTTLJob(tblID, time.Now()); !ok {
		return false, reason, nil
	}

	job, err := h.helper.GetUnfinishedJob(ctx, tblID)
	if err != nil {
		return false, err.Error(), err
	}

	if job != nil {
		return false, "A TTL job is running", nil
	}

	return true, "", nil
}

type ttlTimers struct {
	helper  ttlTimerHelper
	pool    sessionPool
	store   *timerapi.TimerStore
	cli     timerapi.TimerClient
	rt      *timerrt.TimerGroupRuntime
	isCache *cache.InfoSchemaCache
}

func newTTLTimers(helper ttlTimerHelper, pool sessionPool) *ttlTimers {
	store := timerapi.NewMemoryTimerStore()
	return &ttlTimers{
		helper:  helper,
		pool:    pool,
		isCache: cache.NewInfoSchemaCache(getUpdateInfoSchemaCacheInterval()),
		store:   store,
		cli:     timerapi.NewDefaultTimerClient(store),
	}
}

func (t *ttlTimers) BecomeOwner() {
	if t.rt != nil {
		return
	}

	t.rt = timerrt.NewTimerRuntimeBuilder("ttl", t.store).
		RegisterHookFactory(ttlTimerHookClass, t.hookFactory).
		Build()
	t.rt.Start()
}

func (t *ttlTimers) ResignOwner() {
	if t.rt != nil {
		t.rt.Stop()
		t.rt = nil
	}
}

func (t *ttlTimers) Close() {
	t.ResignOwner()
	t.store.Close()
}

func (t *ttlTimers) SyncInterval() time.Duration {
	return t.isCache.GetInterval()
}

func (t *ttlTimers) SyncTimers(ctx context.Context) error {
	se, err := getSession(t.pool)
	if err != nil {
		return err
	}
	defer se.Close()

	schemaVer := t.isCache.SchemaVer()
	if err = t.isCache.Update(se); err != nil {
		return err
	}

	if t.isCache.SchemaVer() == schemaVer {
		return nil
	}

	key2Tables := make(map[string]*cache.PhysicalTable)
	for _, tbl := range t.isCache.Tables {
		key := fmt.Sprintf("%s%d/%d", ttlTimerKeyPrefix, tbl.TableInfo.ID, tbl.ID)
		key2Tables[key] = tbl
	}

	if t.cli == nil {
		return nil
	}

	timers, err := t.cli.GetTimers(ctx, timerapi.WithKeyPrefix(ttlTimerKeyPrefix))
	if err != nil {
		return err
	}

	for _, tm := range timers {
		tbl, ok := key2Tables[tm.Key]
		delete(key2Tables, tm.Key)
		if !ok || tbl.TTLInfo == nil {
			logutil.BgLogger().Info(
				"TTL table is deleted or not a TTL table now, delete timer",
				zap.String("timerKey", tm.Key),
				zap.Strings("tags", tm.Tags),
			)
			if _, err = t.cli.DeleteTimer(ctx, tm.Key); err != nil {
				return err
			}
			continue
		}

		ttlInfo := tbl.TTLInfo
		var updates []timerapi.UpdateTimerOption

		if ttlInfo.JobInterval != tm.SchedPolicyExpr {
			updates = append(updates, timerapi.WithSetSchedExpr(timerapi.SchedEventInterval, ttlInfo.JobInterval))
		}

		if ttlInfo.Enable != tm.Enable {
			updates = append(updates, timerapi.WithSetEnable(ttlInfo.Enable))
		}

		tags := []string{
			fmt.Sprintf("jobInterval=%s", ttlInfo.JobInterval),
			fmt.Sprintf("enable=%t", ttlInfo.Enable),
			fmt.Sprintf("db=%s", tbl.Schema.O),
			fmt.Sprintf("tbl=%s", tbl.Name.O),
		}

		if slices.Compare(tags, tm.Tags) != 0 {
			updates = append(updates, timerapi.WithSetTags(tags))
		}

		if len(updates) > 0 {
			logutil.BgLogger().Info(
				"TTL table's meta updated, update timer",
				zap.String("timerKey", tm.Key),
				zap.Strings("oldTags", tm.Tags),
				zap.Strings("newTags", tags),
			)
			if err = t.cli.UpdateTimer(ctx, tm.ID, updates...); err != nil {
				return err
			}
		}
	}

	for key, tbl := range key2Tables {
		ttlInfo := tbl.TTLInfo
		if ttlInfo == nil {
			continue
		}

		ttlCtx := &ttlTimerContext{
			PhysicalID: tbl.ID,
			TableID:    tbl.TableInfo.ID,
		}

		data, err := ttlCtx.encode()
		if err != nil {
			logutil.BgLogger().Error("failed to encode ttl timer ctx", zap.Error(err))
			return err
		}

		tags := []string{
			fmt.Sprintf("jobInterval=%s", ttlInfo.JobInterval),
			fmt.Sprintf("enable=%t", ttlInfo.Enable),
			fmt.Sprintf("db=%s", tbl.Schema.O),
			fmt.Sprintf("tbl=%s", tbl.Name.O),
		}

		logutil.BgLogger().Info(
			"Add new TTL timer",
			zap.String("timerKey", key),
			zap.Strings("tags", tags),
		)

		_, err = t.cli.CreateTimer(ctx, timerapi.TimerSpec{
			Key:             key,
			Data:            data,
			Tags:            tags,
			SchedPolicyType: timerapi.SchedEventInterval,
			SchedPolicyExpr: ttlInfo.JobInterval,
			Enable:          ttlInfo.Enable,
			HookClass:       ttlTimerHookClass,
		})

		if err != nil {
			return err
		}
	}

	return nil
}

func (t *ttlTimers) hookFactory(_ string, cli timerapi.TimerClient) timerapi.Hook {
	return newTTLTimerHook(cli, t.helper)
}
