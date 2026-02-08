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

	timerapi "github.com/pingcap/tidb/pkg/timer/api"
	timerrt "github.com/pingcap/tidb/pkg/timer/runtime"
)

type ttlTimerSummary struct {
	LastJobRequestID string      `json:"last_job_request_id,omitempty"`
	LastJobSummary   *TTLSummary `json:"last_job_summary,omitempty"`
}

// TTLJobTrace contains some TTL job information to trace
type TTLJobTrace struct {
	// RequestID is the request id when job submitted, we can use it to trace a job
	RequestID string
	// Finished indicates whether the job is finished
	Finished bool
	// Summary indicates the summary of the job
	Summary *TTLSummary
}

// TTLJobAdapter is used to submit TTL job and trace job status.
type TTLJobAdapter interface {
	// Now returns the current time with system timezone.
	Now() (time.Time, error)
	// CanSubmitJob returns whether a new job can be created for the specified table.
	CanSubmitJob(tableID, physicalID int64) bool
	// SubmitJob submits a new job.
	SubmitJob(ctx context.Context, tableID, physicalID int64, requestID string, watermark time.Time) (*TTLJobTrace, error)
	// GetJob returns the job to trace.
	GetJob(ctx context.Context, tableID, physicalID int64, requestID string) (*TTLJobTrace, error)
}

type ttlTimerRuntime struct {
	rt                *timerrt.TimerGroupRuntime
	store             *timerapi.TimerStore
	ttlAdapter        TTLJobAdapter
	softdeleteAdapter TTLJobAdapter
}

func newTTLTimerRuntime(store *timerapi.TimerStore, ttlAdapter TTLJobAdapter, softdeleteAdapter TTLJobAdapter) *ttlTimerRuntime {
	return &ttlTimerRuntime{
		store:             store,
		ttlAdapter:        ttlAdapter,
		softdeleteAdapter: softdeleteAdapter,
	}
}

func (r *ttlTimerRuntime) Resume() {
	if r.rt != nil {
		return
	}

	r.rt = timerrt.NewTimerRuntimeBuilder("ttl", r.store).
		// Use OR condition to listen to both TTL and softdelete timers
		SetCond(timerapi.Or(
			&timerapi.TimerCond{Key: timerapi.NewOptionalVal(ttlTimerKeyPrefix), KeyPrefix: true},
			&timerapi.TimerCond{Key: timerapi.NewOptionalVal(softdeleteTimerKeyPrefix), KeyPrefix: true},
		)).
		// Register TTL hook factory
		RegisterHookFactory(ttlTimerHookClass, func(hookClass string, cli timerapi.TimerClient) timerapi.Hook {
			return newTTLTimerHook(r.ttlAdapter, cli)
		}).
		// Register softdelete hook factory
		RegisterHookFactory(softdeleteTimerHookClass, func(hookClass string, cli timerapi.TimerClient) timerapi.Hook {
			return newSoftdeleteTimerHook(r.softdeleteAdapter, cli)
		}).
		Build()
	r.rt.Start()
}

func (r *ttlTimerRuntime) Pause() {
	if rt := r.rt; rt != nil {
		r.rt = nil
		rt.Stop()
	}
}
