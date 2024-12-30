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

package ddl

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/lockstats"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
)

type ddlHandlerImpl struct {
	ddlEventCh   chan *notifier.SchemaChangeEvent
	statsWriter  types.StatsReadWriter
	statsHandler types.StatsHandle
	sub          *subscriber
}

// NewDDLHandler creates a new ddl handler.
func NewDDLHandler(
	statsWriter types.StatsReadWriter,
	statsHandler types.StatsHandle,
) types.DDL {
	return &ddlHandlerImpl{
		ddlEventCh:   make(chan *notifier.SchemaChangeEvent, 1000),
		statsWriter:  statsWriter,
		statsHandler: statsHandler,
		sub:          newSubscriber(statsHandler),
	}
}

// HandleDDLEvent begins to process a ddl task.
func (h *ddlHandlerImpl) HandleDDLEvent(ctx context.Context, sctx sessionctx.Context, s *notifier.SchemaChangeEvent) error {
	return h.sub.handle(ctx, sctx, s)
}

// DDLEventCh returns ddl events channel in handle.
func (h *ddlHandlerImpl) DDLEventCh() chan *notifier.SchemaChangeEvent {
	return h.ddlEventCh
}

// UpdateStatsWithCountDeltaAndModifyCountDeltaForTest updates the global stats with the given count delta and modify count delta.
func UpdateStatsWithCountDeltaAndModifyCountDeltaForTest(
	sctx sessionctx.Context,
	tableID int64,
	countDelta, modifyCountDelta int64,
) error {
	return updateStatsWithCountDeltaAndModifyCountDelta(
		util.StatsCtx,
		sctx,
		tableID,
		countDelta,
		modifyCountDelta,
	)
}

// updateStatsWithCountDeltaAndModifyCountDelta updates
// the global stats with the given count delta and modify count delta.
// Only used by some special DDLs, such as exchange partition.
func updateStatsWithCountDeltaAndModifyCountDelta(
	ctx context.Context,
	sctx sessionctx.Context,
	tableID int64,
	countDelta, modifyCountDelta int64,
) error {
	lockedTables, err := lockstats.QueryLockedTables(ctx, sctx)
	if err != nil {
		return errors.Trace(err)
	}
	isLocked := false
	if _, ok := lockedTables[tableID]; ok {
		isLocked = true
	}
	startTS, err := util.GetStartTS(sctx)
	if err != nil {
		return errors.Trace(err)
	}
	if isLocked {
		// For locked tables, it is possible that the record gets deleted. So it can be negative.
		_, err = util.Exec(
			sctx,
			"INSERT INTO mysql.stats_table_locked "+
				"(version, count, modify_count, table_id) "+
				"VALUES (%?, %?, %?, %?) "+
				"ON DUPLICATE KEY UPDATE "+
				"version = VALUES(version), "+
				"count = count + VALUES(count), "+
				"modify_count = modify_count + VALUES(modify_count)",
			startTS,
			countDelta,
			modifyCountDelta,
			tableID,
		)
		return err
	}

	// Because count can not be negative, so we need to get the current and calculate the delta.
	count, modifyCount, isNull, err := storage.StatsMetaCountAndModifyCountForUpdate(
		util.StatsCtx,
		sctx,
		tableID,
	)
	if err != nil {
		return err
	}
	if isNull {
		_, err = util.Exec(
			sctx,
			"INSERT INTO mysql.stats_meta "+
				"(version, count, modify_count, table_id) "+
				"VALUES (%?, GREATEST(0, %?), GREATEST(0, %?), %?)",
			startTS,
			countDelta,
			modifyCountDelta,
			tableID,
		)
	} else {
		_, err = util.Exec(
			sctx,
			"UPDATE mysql.stats_meta SET "+
				"version = %?, "+
				"count = GREATEST(0, %?), "+
				"modify_count = GREATEST(0, %?) "+
				"WHERE table_id = %?",
			startTS,
			count+countDelta,
			modifyCount+modifyCountDelta,
			tableID,
		)
	}

	return err
}
