// Copyright 2026 PingCAP, Inc.
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
	softdeleteTimerKeyPrefix = "/tidb/softdelete/physical_table/"
	softdeleteTimerHookClass = "tidb.softdelete"
)

// SoftDeleteTimersSyncer is used to sync timers for softdelete.
type SoftDeleteTimersSyncer struct {
	timersSyncerBase
}

// NewSoftDeleteTimerSyncer creates a new SoftDeleteTimersSyncer.
func NewSoftDeleteTimerSyncer(pool syssession.Pool, cli timerapi.TimerClient) (*SoftDeleteTimersSyncer, error) {
	cfg := timersSyncerConfig{
		keyPrefix: softdeleteTimerKeyPrefix,
		attr:      infoschemacontext.SoftDeleteAttribute,
		hookClass: softdeleteTimerHookClass,
		shouldSyncTable: func(tblInfo *model.TableInfo) bool {
			return tblInfo.SoftdeleteInfo != nil
		},
		getEnable: func(tblInfo *model.TableInfo) bool {
			return tblInfo.SoftdeleteInfo.JobEnable
		},
		getSchedPolicy: func(tblInfo *model.TableInfo) (timerapi.SchedPolicyType, string) {
			interval := tblInfo.SoftdeleteInfo.JobInterval
			if interval == "" {
				interval = model.DefaultSoftDeleteJobInterval
			}
			return timerapi.SchedEventInterval, interval
		},
		getWatermark: func(ctx context.Context, se session.Session, tblInfo *model.TableInfo, partition *model.PartitionDefinition) (time.Time, error) {
			pid := tblInfo.ID
			if partition != nil {
				pid = partition.ID
			}

			sql, args := cache.SelectFromTableStatusWithID(session.TTLJobTypeSoftDelete, pid)

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
	return &SoftDeleteTimersSyncer{timersSyncerBase: base}, nil
}
