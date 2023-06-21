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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx/variable"
	timerapi "github.com/pingcap/tidb/timer/api"
	"github.com/pingcap/tidb/timer/runtime"
	timertable "github.com/pingcap/tidb/timer/tablestore"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/timeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

const (
	timerPrefix    = "/tidb/ttl/table/"
	timerHookClass = "ttl/table"
)

type timerMeta struct {
	TableID    int64 `json:"table_id"`
	PhysicalID int64 `json:"physical_id"`
}

type timerRuntime struct {
	pool    sessionPool
	adapter ttlJobAdapter
	store   *timerapi.TimerStore
	cli     timerapi.TimerClient
	rt      *runtime.TimerGroupRuntime
}

func newTimerRuntime(pool sessionPool, etcd *clientv3.Client, adapter ttlJobAdapter) *timerRuntime {
	store := timertable.NewTableTimerStore(0, pool, "mysql", "tidb_timers", etcd)
	return &timerRuntime{
		pool:    pool,
		adapter: adapter,
		store:   store,
		cli:     timerapi.NewDefaultTimerClient(store),
	}
}

func (r *timerRuntime) Paused() bool {
	return r.rt == nil
}

func (r *timerRuntime) Resume(ctx context.Context) {
	if r.store == nil {
		return
	}

	if r.rt != nil {
		return
	}

	r.SyncTimers(ctx)
	r.rt = runtime.NewTimerRuntimeBuilder("ttl", r.store).
		SetCond(&timerapi.TimerCond{
			Key:       timerapi.NewOptionalVal(timerPrefix),
			KeyPrefix: true,
		}).
		RegisterHookFactory(timerHookClass, func(hookClass string, cli timerapi.TimerClient) timerapi.Hook {
			return newTTLTableHook(cli, r.adapter)
		}).
		Build()

	r.rt.Start()
}

func (r *timerRuntime) SyncTimers(ctx context.Context) {
	se, err := getSession(r.pool)
	if err != nil {
		return
	}
	defer se.Close()

	isVer := se.GetDomainInfoSchema()
	is, ok := isVer.(infoschema.InfoSchema)
	if !ok {
		logutil.BgLogger().Error(fmt.Sprintf("%T is not the type of infoschema.InfoSchema", isVer))
		return
	}

	timers, err := r.cli.GetTimers(ctx, timerapi.WithKeyPrefix(timerPrefix))
	if err != nil {
		terror.Log(err)
		return
	}

	key2Timers := make(map[string]*timerapi.TimerRecord, len(timers))
	for _, timer := range timers {
		key2Timers[timer.Key] = timer
	}

	for _, db := range is.AllSchemas() {
		for _, tbl := range is.SchemaTables(db.Name) {
			tblInfo := tbl.Meta()
			if tblInfo.TTLInfo == nil || tblInfo.State != model.StatePublic {
				continue
			}

			if tblInfo.Partition == nil {
				if err = r.syncTable(ctx, db, tblInfo, nil, key2Timers); err != nil {
					logutil.BgLogger().Error(
						"error to sync timer for table",
						zap.Error(err),
						zap.String("db", db.Name.O),
						zap.String("table", db.Name.O),
						zap.Int64("tableID", tblInfo.ID),
					)
				}
				continue
			}

			for i := range tblInfo.Partition.Definitions {
				def := &tblInfo.Partition.Definitions[i]
				if err = r.syncTable(ctx, db, tblInfo, def, key2Timers); err != nil {
					logutil.BgLogger().Error(
						"error to sync timer for partition",
						zap.Error(err),
						zap.String("db", db.Name.O),
						zap.String("table", db.Name.O),
						zap.String("partition", def.Name.O),
						zap.Int64("tableID", tblInfo.ID),
						zap.Int64("partitionID", def.ID),
					)
				}
			}
		}
	}

	for _, timer := range key2Timers {
		if time.Since(timer.CreateTime) > time.Minute {
			if _, err = r.cli.DeleteTimer(ctx, timer.ID); err != nil {
				logutil.BgLogger().Error(
					"error to delete timer",
					zap.Error(err),
					zap.String("id", timer.ID),
					zap.String("key", timer.Key),
					zap.Strings("tags", timer.Tags),
				)
			}
		}
	}

	return
}

func (r *timerRuntime) syncTable(ctx context.Context, db *model.DBInfo, tblInfo *model.TableInfo, def *model.PartitionDefinition, key2Timers map[string]*timerapi.TimerRecord) error {
	physicalID := tblInfo.ID
	partitionName := ""
	if def != nil {
		physicalID = def.ID
		partitionName = def.Name.O
	}

	key := fmt.Sprintf("%s%d/%d", timerPrefix, tblInfo.ID, physicalID)
	defer delete(key2Timers, key)

	tags := append(
		make([]string, 0, 4),
		fmt.Sprintf("db:%s", db.Name.O),
		fmt.Sprintf("table:%s", tblInfo.Name.O),
	)

	if def != nil {
		tags = append(tags, fmt.Sprintf("partition:%s", partitionName))
	}

	timer, ok := key2Timers[key]
	if ok && slices.Compare(tags, timer.Tags) == 0 &&
		timer.Enable == tblInfo.TTLInfo.Enable &&
		timer.SchedPolicyExpr == tblInfo.TTLInfo.JobInterval {
		return nil
	}

	if !ok {
		data, err := json.Marshal(&timerMeta{
			TableID:    tblInfo.ID,
			PhysicalID: physicalID,
		})

		if err != nil {
			return err
		}

		_, err = r.cli.CreateTimer(ctx, timerapi.TimerSpec{
			Key:             key,
			Data:            data,
			Tags:            tags,
			SchedPolicyType: timerapi.SchedEventInterval,
			SchedPolicyExpr: tblInfo.TTLInfo.JobInterval,
			HookClass:       timerHookClass,
			Enable:          tblInfo.TTLInfo.Enable,
		})

		return err
	}

	return r.cli.UpdateTimer(
		ctx,
		timer.ID,
		timerapi.WithSetTags(tags),
		timerapi.WithSetSchedExpr(timerapi.SchedEventInterval, tblInfo.TTLInfo.JobInterval),
		timerapi.WithSetEnable(tblInfo.TTLInfo.Enable),
	)
}

func (r *timerRuntime) Pause() {
	if r.rt == nil {
		return
	}

	r.rt.Stop()
	r.rt = nil
	return
}

func (r *timerRuntime) Close() {
	r.Pause()
	r.store.Close()
	r.store = nil
}

type ttlJobBrief struct {
	ID       string
	Finished bool
}

type ttlJobAdapter interface {
	SubmitJob(ctx context.Context, id string, tableID int64, physicalID int64) (*ttlJobBrief, error)
	GetJob(ctx context.Context, id string) (*ttlJobBrief, error)
}

type ttlTableHook struct {
	ctx    context.Context
	cancel func()
	wg     sync.WaitGroup

	cli     timerapi.TimerClient
	adapter ttlJobAdapter
}

func newTTLTableHook(cli timerapi.TimerClient, adapter ttlJobAdapter) *ttlTableHook {
	return &ttlTableHook{
		cli:     cli,
		adapter: adapter,
	}
}

func (h *ttlTableHook) Start() {
	h.ctx, h.cancel = context.WithCancel(context.Background())
}

func (h *ttlTableHook) Stop() {
	if h.cancel != nil {
		h.cancel()
		h.cancel = nil
	}
	h.wg.Wait()
}

func (h *ttlTableHook) OnPreSchedEvent(_ context.Context, _ timerapi.TimerShedEvent) (r timerapi.PreSchedEventResult, err error) {
	if !variable.EnableTTLJob.Load() {
		r.Delay = time.Minute
		return
	}

	windowStart, windowEnd := variable.TTLJobScheduleWindowStartTime.Load(), variable.TTLJobScheduleWindowEndTime.Load()
	if !timeutil.WithinDayTimePeriod(windowStart, windowEnd, time.Now()) {
		r.Delay = time.Minute
		return
	}

	return
}

func (h *ttlTableHook) OnSchedEvent(ctx context.Context, event timerapi.TimerShedEvent) error {
	timer := event.Timer()
	var meta timerMeta
	if err := json.Unmarshal(timer.Data, &meta); err != nil {
		return err
	}

	eventID := event.EventID()
	logger := logutil.BgLogger().With(
		zap.String("key", timer.Key),
		zap.String("eventID", eventID),
		zap.Strings("tags", timer.Tags),
	)

	job, err := h.adapter.GetJob(ctx, eventID)
	if err != nil {
		return err
	}

	if job == nil && time.Since(timer.EventStart) > 10*time.Minute {
		return h.cli.CloseTimerEvent(ctx, timer.ID, eventID, timerapi.WithSetWatermark(timer.Watermark))
	}

	if job == nil {
		if _, err = h.adapter.SubmitJob(ctx, eventID, meta.TableID, meta.PhysicalID); err != nil {
			return err
		}
	}

	h.wg.Add(1)
	h.waitJobFinished(logger, timer.ID, eventID, timer.EventStart)
	return nil
}

func (h *ttlTableHook) waitJobFinished(logger *zap.Logger, timerID string, eventID string, nextWatermark time.Time) {
	defer h.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
		}

		timer, err := h.cli.GetTimerByID(h.ctx, timerID)
		if err != nil {
			if errors.ErrorEqual(timerapi.ErrTimerNotExist, err) {
				logger.Warn("timer is deleted")
				return
			}

			logger.Error("GetTimerByID failed", zap.Error(err))
			continue
		}

		if timer.EventID != eventID {
			logger.Warn("current event id changed", zap.String("newEventID", timer.EventID))
			return
		}

		job, err := h.adapter.GetJob(h.ctx, eventID)
		if err != nil {
			logger.Error("GetJob error", zap.Error(err))
			continue
		}

		if job != nil && !job.Finished {
			continue
		}

		if job == nil {
			logger.Error("job not exist")
		}

		if err = h.cli.CloseTimerEvent(h.ctx, timerID, eventID, timerapi.WithSetWatermark(nextWatermark)); err != nil {
			logger.Error("CloseTimerEvent error", zap.Error(err))
			continue
		}

		return
	}
}
