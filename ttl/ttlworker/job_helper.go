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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/util/timeutil"
)

type triggerJobRequest struct {
	tblID            int64
	partitionID      int64
	checkMaxJobStart time.Time
	resp             chan<- *triggerJobResp
}

type triggerJobResp struct {
	err error
	job *ttlJobBrief
}

type ttlTableStatusHelper struct {
	pool      sessionPool
	triggerCh chan<- *triggerJobRequest
}

func newTableStatusHelper(pool sessionPool, triggerCh chan<- *triggerJobRequest) *ttlTableStatusHelper {
	return &ttlTableStatusHelper{
		pool:      pool,
		triggerCh: triggerCh,
	}
}

func (h *ttlTableStatusHelper) CanTriggerTTLJob(_ int64, now time.Time) (bool, string) {
	if !variable.EnableTTLJob.Load() {
		return false, "TTL job is disabled globally"
	}

	if !timeutil.WithinDayTimePeriod(
		variable.TTLJobScheduleWindowStartTime.Load(),
		variable.TTLJobScheduleWindowEndTime.Load(),
		now,
	) {
		return false, "Not in TTL job window"
	}

	return true, ""
}

func (h *ttlTableStatusHelper) GetMaxStartTimeJob(ctx context.Context, physicalID int64) (*ttlJobBrief, error) {
	status, err := h.getTTLTableStatus(ctx, physicalID)
	if err != nil {
		return nil, err
	}

	if status == nil {
		return nil, nil
	}

	if status.CurrentJobID != "" {
		return &ttlJobBrief{
			ID:        status.CurrentJobID,
			StartTime: status.CurrentJobStartTime,
			Finished:  false,
		}, nil
	}

	return &ttlJobBrief{
		ID:        status.LastJobID,
		StartTime: status.LastJobStartTime,
		Finished:  true,
	}, nil
}

func (h *ttlTableStatusHelper) GetUnfinishedJob(ctx context.Context, tblID int64) (*ttlJobBrief, error) {
	status, err := h.getTTLTableStatus(ctx, tblID)
	if err != nil {
		return nil, err
	}

	if status == nil || status.CurrentJobID == "" {
		return nil, nil
	}

	return &ttlJobBrief{
		ID:        status.CurrentJobID,
		StartTime: status.CurrentJobStartTime,
		Finished:  false,
	}, nil
}

func (h *ttlTableStatusHelper) TriggerJob(ctx context.Context, tableID int64, partitionID int64, checkMaxJobStart time.Time) (*ttlJobBrief, error) {
	respCh := make(chan *triggerJobResp, 1)
	req := &triggerJobRequest{
		tblID:            tableID,
		partitionID:      partitionID,
		checkMaxJobStart: checkMaxJobStart,
		resp:             respCh,
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case h.triggerCh <- req:
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-respCh:
		return resp.job, nil
	}
}

func (h *ttlTableStatusHelper) getTTLTableStatus(ctx context.Context, physicalID int64) (*cache.TableStatus, error) {
	se, err := getSession(h.pool)
	if err != nil {
		return nil, err
	}
	defer se.Close()

	sql, args := cache.SelectFromTTLTableStatusWithID(physicalID)
	rows, err := se.ExecuteSQL(ctx, sql, args...)
	if err != nil {
		return nil, errors.Wrapf(err, "execute sql: %s", sql)
	}

	if len(rows) == 0 {
		return nil, nil
	}

	return cache.RowToTableStatus(se, rows[0])
}
