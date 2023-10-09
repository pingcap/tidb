// Copyright 2018 PingCAP, Inc.
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

package handle

import (
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics/handle/storage"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

// GCStats will garbage collect the useless stats' info.
// For dropped tables, we will first update their version
// so that other tidb could know that table is deleted.
func (h *Handle) GCStats(is infoschema.InfoSchema, ddlLease time.Duration) (err error) {
	// To make sure that all the deleted tables' schema and stats info have been acknowledged to all tidb,
	// we only garbage collect version before 10 lease.
	lease := mathutil.Max(h.Lease(), ddlLease)
	offset := DurationToTS(10 * lease)
	now := oracle.GoTimeToTS(time.Now())
	if now < offset {
		return nil
	}

	// Get the last gc time.
	gcVer := now - offset
	lastGC, err := h.GetLastGCTimestamp()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			return
		}
		err = h.writeGCTimestampToKV(gcVer)
	}()

	rows, _, err := h.execRows("select table_id from mysql.stats_meta where version >= %? and version < %?", lastGC, gcVer)
	if err != nil {
		return errors.Trace(err)
	}
	for _, row := range rows {
		if err := h.gcTableStats(is, row.GetInt64(0)); err != nil {
			return errors.Trace(err)
		}
		_, existed := is.TableByID(row.GetInt64(0))
		if !existed {
			if err := h.gcHistoryStatsFromKV(row.GetInt64(0)); err != nil {
				return errors.Trace(err)
			}
		}
	}

	if err := h.ClearOutdatedHistoryStats(); err != nil {
		logutil.BgLogger().Warn("failed to gc outdated historical stats",
			zap.Duration("duration", variable.HistoricalStatsDuration.Load()),
			zap.Error(err))
	}

	return h.removeDeletedExtendedStats(gcVer)
}

// GetLastGCTimestamp loads the last gc time from mysql.tidb.
func (h *Handle) GetLastGCTimestamp() (lastGCTS uint64, err error) {
	err = h.callWithSCtx(func(sctx sessionctx.Context) error {
		lastGCTS, err = storage.GetLastGCTimestamp(sctx)
		return err
	})
	return
}

func (h *Handle) writeGCTimestampToKV(newTS uint64) error {
	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		return storage.WriteGCTimestampToKV(sctx, newTS)
	})
}

func (h *Handle) gcTableStats(is infoschema.InfoSchema, physicalID int64) error {
	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		return storage.GCTableStats(sctx, h.getTableByPhysicalID, h.MarkExtendedStatsDeleted, is, physicalID)
	})
}

// ClearOutdatedHistoryStats clear outdated historical stats
func (h *Handle) ClearOutdatedHistoryStats() error {
	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		return storage.ClearOutdatedHistoryStats(sctx)
	})
}

func (h *Handle) gcHistoryStatsFromKV(physicalID int64) error {
	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		return storage.GCHistoryStatsFromKV(sctx, physicalID)
	}, flagWrapTxn)
}

// deleteHistStatsFromKV deletes all records about a column or an index and updates version.
func (h *Handle) deleteHistStatsFromKV(physicalID int64, histID int64, isIndex int) (err error) {
	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		return storage.DeleteHistStatsFromKV(sctx, physicalID, histID, isIndex)
	}, flagWrapTxn)
}

// DeleteTableStatsFromKV deletes table statistics from kv.
// A statsID refers to statistic of a table or a partition.
func (h *Handle) DeleteTableStatsFromKV(statsIDs []int64) (err error) {
	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		return storage.DeleteTableStatsFromKV(sctx, statsIDs)
	}, flagWrapTxn)
}

func (h *Handle) removeDeletedExtendedStats(version uint64) (err error) {
	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		return storage.RemoveDeletedExtendedStats(sctx, version)
	}, flagWrapTxn)
}
