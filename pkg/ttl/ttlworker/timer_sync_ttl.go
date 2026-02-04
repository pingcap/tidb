// Copyright 2026 PingCAP, Inc.
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

	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/session/syssession"
	timerapi "github.com/pingcap/tidb/pkg/timer/api"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/ttl/session"
)

const (
	ttlTimerKeyPrefix = "/tidb/ttl/physical_table/"
	ttlTimerHookClass = "tidb.ttl"
)

// TTLTimersSyncer is used to sync timers for ttl
type TTLTimersSyncer struct {
	timersSyncerBase
}

// NewTTLTimerSyncer creates a new TTLTimersSyncer
func NewTTLTimerSyncer(pool syssession.Pool, cli timerapi.TimerClient) (*TTLTimersSyncer, error) {
	cfg := timersSyncerConfig{
		keyPrefix: ttlTimerKeyPrefix,
		attr:      infoschemacontext.TTLAttribute,
		hookClass: ttlTimerHookClass,
		shouldSyncTable: func(tblInfo *model.TableInfo) bool {
			return tblInfo.TTLInfo != nil
		},
		getEnable: func(tblInfo *model.TableInfo) bool {
			return tblInfo.TTLInfo.Enable
		},
		getSchedPolicy: func(tblInfo *model.TableInfo) (timerapi.SchedPolicyType, string) {
			interval := tblInfo.TTLInfo.JobInterval
			if interval == "" {
				// This only happens when the table is created from 6.5 in which the `tidb_job_interval` is not introduced yet.
				// We use `OldDefaultTTLJobInterval` as the return value to ensure a consistent behavior for the
				// upgrades: v6.5 -> v8.5(or previous version) -> newer version than v8.5.
				interval = model.OldDefaultTTLJobInterval
			}
			return timerapi.SchedEventInterval, interval
		},
		getWatermark: func(ctx context.Context, se session.Session, tblInfo *model.TableInfo, partition *model.PartitionDefinition) (time.Time, error) {
			pid := tblInfo.ID
			if partition != nil {
				pid = partition.ID
			}

			sql, args := cache.SelectFromTableStatusWithID(session.TTLJobTypeTTL, pid)

			rows, err := se.ExecuteSQL(ctx, sql, args...)
			if err != nil {
				return time.Time{}, err
			}
			if len(rows) == 0 {
				return time.Time{}, nil
			}
			status, err := cache.RowToTableStatus(se.GetSessionVars().Location(), rows[0])
			if err != nil {
				return time.Time{}, err
			}
			if status.CurrentJobID != "" {
				return status.CurrentJobStartTime, nil
			}
			return status.LastJobStartTime, nil
		},
	}
	base, err := newTimersSyncerBase(pool, cli, cfg)
	if err != nil {
		return nil, err
	}
	return &TTLTimersSyncer{timersSyncerBase: base}, nil
}
