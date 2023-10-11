// Copyright 2017 PingCAP, Inc.
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
	"cmp"
	"slices"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics/handle/autoanalyze"
	"github.com/pingcap/tidb/statistics/handle/storage"
	"github.com/pingcap/tidb/statistics/handle/usage"
	utilstats "github.com/pingcap/tidb/statistics/handle/util"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/sqlexec"
)

// NewSessionIndexUsageCollector will add a new SessionIndexUsageCollector into linked list headed by idxUsageListHead.
// idxUsageListHead always points to an empty SessionIndexUsageCollector as a sentinel node. So we let idxUsageListHead.next
// points to new item. It's helpful to sweepIdxUsageList.
func (h *Handle) NewSessionIndexUsageCollector() *usage.SessionIndexUsageCollector {
	return usage.NewSessionIndexUsageCollector(h.idxUsageListHead)
}

// batchInsertSize is the batch size used by internal SQL to insert values to some system table.
const batchInsertSize = 10

// DumpIndexUsageToKV will dump in-memory index usage information to KV.
func (h *Handle) DumpIndexUsageToKV() error {
	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		return usage.DumpIndexUsageToKV(sctx, h.idxUsageListHead)
	})
}

func (h *Handle) callWithSCtx(f func(sctx sessionctx.Context) error, flags ...int) (err error) {
	return utilstats.CallWithSCtx(h.pool, f, flags...)
}

// GCIndexUsage will delete the usage information of those indexes that do not exist.
func (h *Handle) GCIndexUsage() error {
	return h.callWithSCtx(usage.GCIndexUsageOnKV)
}

var (
	// DumpStatsDeltaRatio is the lower bound of `Modify Count / Table Count` for stats delta to be dumped.
	DumpStatsDeltaRatio = 1 / 10000.0
	// dumpStatsMaxDuration is the max duration since last update.
	dumpStatsMaxDuration = time.Hour
)

// needDumpStatsDelta checks whether to dump stats delta.
// 1. If the table doesn't exist or is a mem table or system table, then return false.
// 2. If the mode is DumpAll, then return true.
// 3. If the stats delta haven't been dumped in the past hour, then return true.
// 4. If the table stats is pseudo or empty or `Modify Count / Table Count` exceeds the threshold.
func (h *Handle) needDumpStatsDelta(is infoschema.InfoSchema, mode dumpMode, id int64, item variable.TableDelta, currentTime time.Time) bool {
	tbl, ok := h.TableInfoByID(is, id)
	if !ok {
		return false
	}
	dbInfo, ok := is.SchemaByTable(tbl.Meta())
	if !ok {
		return false
	}
	if util.IsMemOrSysDB(dbInfo.Name.L) {
		return false
	}
	if mode == DumpAll {
		return true
	}
	if item.InitTime.IsZero() {
		item.InitTime = currentTime
	}
	if currentTime.Sub(item.InitTime) > dumpStatsMaxDuration {
		// Dump the stats to kv at least once an hour.
		return true
	}
	statsTbl := h.GetPartitionStats(tbl.Meta(), id)
	if statsTbl.Pseudo || statsTbl.RealtimeCount == 0 || float64(item.Count)/float64(statsTbl.RealtimeCount) > DumpStatsDeltaRatio {
		// Dump the stats when there are many modifications.
		return true
	}
	return false
}

type dumpMode bool

const (
	// DumpAll indicates dump all the delta info in to kv.
	DumpAll dumpMode = true
	// DumpDelta indicates dump part of the delta info in to kv.
	DumpDelta dumpMode = false
)

// DumpStatsDeltaToKV sweeps the whole list and updates the global map, then we dumps every table that held in map to KV.
// If the mode is `DumpDelta`, it will only dump that delta info that `Modify Count / Table Count` greater than a ratio.
func (h *Handle) DumpStatsDeltaToKV(mode dumpMode) error {
	h.SweepSessionStatsList()
	deltaMap := h.SessionTableDelta().GetDeltaAndReset()
	defer func() {
		h.SessionTableDelta().Merge(deltaMap)
	}()

	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
		currentTime := time.Now()
		for id, item := range deltaMap {
			if !h.needDumpStatsDelta(is, mode, id, item, currentTime) {
				continue
			}
			updated, err := h.dumpTableStatCountToKV(is, id, item)
			if err != nil {
				return errors.Trace(err)
			}
			if updated {
				usage.UpdateTableDeltaMap(deltaMap, id, -item.Delta, -item.Count, nil)
			}
			if err = storage.DumpTableStatColSizeToKV(sctx, id, item); err != nil {
				delete(deltaMap, id)
				return errors.Trace(err)
			}
			if updated {
				delete(deltaMap, id)
			} else {
				m := deltaMap[id]
				m.ColSize = nil
				deltaMap[id] = m
			}
		}
		return nil
	})
}

// dumpTableStatDeltaToKV dumps a single delta with some table to KV and updates the version.
// For a partitioned table, we will update its global-stats as well.
func (h *Handle) dumpTableStatCountToKV(is infoschema.InfoSchema, physicalTableID int64, delta variable.TableDelta) (updated bool, err error) {
	statsVersion := uint64(0)
	defer func() {
		if err == nil && statsVersion != 0 {
			h.recordHistoricalStatsMeta(physicalTableID, statsVersion, StatsMetaHistorySourceFlushStats)
		}
	}()
	if delta.Count == 0 {
		return true, nil
	}

	err = h.callWithSCtx(func(sctx sessionctx.Context) error {
		statsVersion, err = utilstats.GetStartTS(sctx)
		if err != nil {
			return errors.Trace(err)
		}

		tbl, _, _ := is.FindTableByPartitionID(physicalTableID)
		// Check if the table and its partitions are locked.
		tidAndPid := make([]int64, 0, 2)
		if tbl != nil {
			tidAndPid = append(tidAndPid, tbl.Meta().ID)
		}
		tidAndPid = append(tidAndPid, physicalTableID)
		lockedTables, err := h.GetLockedTables(tidAndPid...)
		if err != nil {
			return err
		}

		var affectedRows uint64
		// If it's a partitioned table and its global-stats exists,
		// update its count and modify_count as well.
		if tbl != nil {
			// We need to check if the table and the partition are locked.
			isTableLocked := false
			isPartitionLocked := false
			tableID := tbl.Meta().ID
			if _, ok := lockedTables[tableID]; ok {
				isTableLocked = true
			}
			if _, ok := lockedTables[physicalTableID]; ok {
				isPartitionLocked = true
			}
			tableOrPartitionLocked := isTableLocked || isPartitionLocked
			if err = storage.UpdateStatsMeta(sctx, statsVersion, delta,
				physicalTableID, tableOrPartitionLocked); err != nil {
				return err
			}
			affectedRows += sctx.GetSessionVars().StmtCtx.AffectedRows()
			// If the partition is locked, we don't need to update the global-stats.
			// We will update its global-stats when the partition is unlocked.
			// 1. If table is locked and partition is locked, we only stash the delta in the partition's lock info.
			//    we will update its global-stats when the partition is unlocked.
			// 2. If table is locked and partition is not locked(new partition after lock), we only stash the delta in the table's lock info.
			//    we will update its global-stats when the table is unlocked. We don't need to specially handle this case.
			//    Because updateStatsMeta will insert a new record if the record doesn't exist.
			// 3. If table is not locked and partition is locked, we only stash the delta in the partition's lock info.
			//    we will update its global-stats when the partition is unlocked.
			// 4. If table is not locked and partition is not locked, we update the global-stats.
			// To sum up, we only need to update the global-stats when the table and the partition are not locked.
			if !isTableLocked && !isPartitionLocked {
				// If it's a partitioned table and its global-stats exists, update its count and modify_count as well.
				if err = storage.UpdateStatsMeta(sctx, statsVersion, delta, tableID, isTableLocked); err != nil {
					return err
				}
				affectedRows += sctx.GetSessionVars().StmtCtx.AffectedRows()
			}
		} else {
			// This is a non-partitioned table.
			// Check if it's locked.
			isTableLocked := false
			if _, ok := lockedTables[physicalTableID]; ok {
				isTableLocked = true
			}
			if err = storage.UpdateStatsMeta(sctx, statsVersion, delta,
				physicalTableID, isTableLocked); err != nil {
				return err
			}
			affectedRows += sctx.GetSessionVars().StmtCtx.AffectedRows()
		}

		updated = affectedRows > 0
		return nil
	}, utilstats.FlagWrapTxn)
	return
}

// DumpColStatsUsageToKV sweeps the whole list, updates the column stats usage map and dumps it to KV.
func (h *Handle) DumpColStatsUsageToKV() error {
	if !variable.EnableColumnTracking.Load() {
		return nil
	}
	h.SweepSessionStatsList()
	colMap := h.SessionStatsUsage().GetUsageAndReset()
	defer func() {
		h.SessionStatsUsage().Merge(colMap)
	}()
	type pair struct {
		lastUsedAt string
		tblColID   model.TableItemID
	}
	pairs := make([]pair, 0, len(colMap))
	for id, t := range colMap {
		pairs = append(pairs, pair{tblColID: id, lastUsedAt: t.UTC().Format(types.TimeFormat)})
	}
	slices.SortFunc(pairs, func(i, j pair) int {
		if i.tblColID.TableID == j.tblColID.TableID {
			return cmp.Compare(i.tblColID.ID, j.tblColID.ID)
		}
		return cmp.Compare(i.tblColID.TableID, j.tblColID.TableID)
	})
	// Use batch insert to reduce cost.
	for i := 0; i < len(pairs); i += batchInsertSize {
		end := i + batchInsertSize
		if end > len(pairs) {
			end = len(pairs)
		}
		sql := new(strings.Builder)
		sqlexec.MustFormatSQL(sql, "INSERT INTO mysql.column_stats_usage (table_id, column_id, last_used_at) VALUES ")
		for j := i; j < end; j++ {
			// Since we will use some session from session pool to execute the insert statement, we pass in UTC time here and covert it
			// to the session's time zone when executing the insert statement. In this way we can make the stored time right.
			sqlexec.MustFormatSQL(sql, "(%?, %?, CONVERT_TZ(%?, '+00:00', @@TIME_ZONE))", pairs[j].tblColID.TableID, pairs[j].tblColID.ID, pairs[j].lastUsedAt)
			if j < end-1 {
				sqlexec.MustFormatSQL(sql, ",")
			}
		}
		sqlexec.MustFormatSQL(sql, " ON DUPLICATE KEY UPDATE last_used_at = CASE WHEN last_used_at IS NULL THEN VALUES(last_used_at) ELSE GREATEST(last_used_at, VALUES(last_used_at)) END")
		if _, _, err := h.execRows(sql.String()); err != nil {
			return errors.Trace(err)
		}
		for j := i; j < end; j++ {
			delete(colMap, pairs[j].tblColID)
		}
	}
	return nil
}

const (
	// StatsOwnerKey is the stats owner path that is saved to etcd.
	StatsOwnerKey = "/tidb/stats/owner"
	// StatsPrompt is the prompt for stats owner manager.
	StatsPrompt = "stats"
)

// HandleAutoAnalyze analyzes the newly created table or index.
func (h *Handle) HandleAutoAnalyze(is infoschema.InfoSchema) (analyzed bool) {
	_ = h.callWithSCtx(func(sctx sessionctx.Context) error {
		analyzed = autoanalyze.HandleAutoAnalyze(sctx, &autoanalyze.Opt{
			StatsLease:              h.Lease(),
			GetLockedTables:         h.GetLockedTables,
			GetTableStats:           h.GetTableStats,
			GetPartitionStats:       h.GetPartitionStats,
			SysProcTracker:          h.sysProcTracker,
			AutoAnalyzeProcIDGetter: h.autoAnalyzeProcIDGetter,
		}, is)
		return nil
	})
	return
}

// GetCurrentPruneMode returns the current latest partitioning table prune mode.
func (h *Handle) GetCurrentPruneMode() (mode string, err error) {
	err = h.callWithSCtx(func(sctx sessionctx.Context) error {
		mode = sctx.GetSessionVars().PartitionPruneMode.Load()
		return nil
	})
	return
}
