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

package storage

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
)

// UpdateStatsVersion will set statistics version to the newest TS, then
// tidb-server will reload automatic.
func UpdateStatsVersion(ctx context.Context, sctx sessionctx.Context) error {
	startTS, err := statsutil.GetStartTS(sctx)
	if err != nil {
		return errors.Trace(err)
	}
	if _, err = statsutil.ExecWithCtx(
		ctx, sctx, "update mysql.stats_meta set version = %?", startTS,
	); err != nil {
		return err
	}
	if _, err = statsutil.ExecWithCtx(
		ctx, sctx, "update mysql.stats_histograms set version = %?", startTS,
	); err != nil {
		return err
	}
	return nil
}

// DeltaUpdate is the delta update for stats meta.
type DeltaUpdate struct {
	Delta    variable.TableDelta
	TableID  int64
	IsLocked bool
}

// NewDeltaUpdate creates a new DeltaUpdate.
func NewDeltaUpdate(tableID int64, delta variable.TableDelta, isLocked bool) *DeltaUpdate {
	return &DeltaUpdate{
		Delta:    delta,
		TableID:  tableID,
		IsLocked: isLocked,
	}
}

// UpdateStatsMeta updates the stats meta for multiple tables.
// It uses the INSERT INTO ... ON DUPLICATE KEY UPDATE syntax to fill the missing records.
// Note: Make sure call this function in a transaction.
func UpdateStatsMeta(
	ctx context.Context,
	sctx sessionctx.Context,
	startTS uint64,
	updates ...*DeltaUpdate,
) (err error) {
	if len(updates) == 0 {
		return nil
	}

	// Separate locked and unlocked updates
	// In most cases, the number of locked tables is small.
	lockedTableIDs := make([]string, 0, 20)
	lockedValues := make([]string, 0, 20)
	// In most cases, the number of unlocked tables is large.
	unlockedTableIDs := make([]string, 0, len(updates))
	unlockedPosValues := make([]string, 0, max(len(updates)/2, 1))
	unlockedNegValues := make([]string, 0, max(len(updates)/2, 1))
	cacheInvalidateIDs := make([]int64, 0, len(updates))

	for _, update := range updates {
		if update.IsLocked {
			lockedTableIDs = append(lockedTableIDs, fmt.Sprintf("%d", update.TableID))
			lockedValues = append(lockedValues, fmt.Sprintf("(%d, %d, %d, %d)",
				startTS, update.TableID, update.Delta.Count, update.Delta.Delta))
		} else {
			unlockedTableIDs = append(unlockedTableIDs, fmt.Sprintf("%d", update.TableID))
			if update.Delta.Delta < 0 {
				unlockedNegValues = append(unlockedNegValues, fmt.Sprintf("(%d, %d, %d, %d)",
					startTS, update.TableID, update.Delta.Count, -update.Delta.Delta))
			} else {
				unlockedPosValues = append(unlockedPosValues, fmt.Sprintf("(%d, %d, %d, %d)",
					startTS, update.TableID, update.Delta.Count, update.Delta.Delta))
			}
			cacheInvalidateIDs = append(cacheInvalidateIDs, update.TableID)
		}
	}

	// Lock the stats_meta and stats_table_locked tables using SELECT FOR UPDATE to prevent write conflicts.
	// This ensures that we acquire the necessary locks before attempting to update the tables, reducing the likelihood
	// of encountering lock conflicts during the update process.
	lockedTableIDsStr := strings.Join(lockedTableIDs, ",")
	if lockedTableIDsStr != "" {
		if _, err = statsutil.ExecWithCtx(ctx, sctx, fmt.Sprintf("select * from mysql.stats_table_locked where table_id in (%s) for update", lockedTableIDsStr)); err != nil {
			return err
		}
	}

	unlockedTableIDsStr := strings.Join(unlockedTableIDs, ",")
	if unlockedTableIDsStr != "" {
		if _, err = statsutil.ExecWithCtx(ctx, sctx, fmt.Sprintf("select * from mysql.stats_meta where table_id in (%s) for update", unlockedTableIDsStr)); err != nil {
			return err
		}
	}
	// Execute locked updates
	if len(lockedValues) > 0 {
		sql := fmt.Sprintf("insert into mysql.stats_table_locked (version, table_id, modify_count, count) values %s "+
			"on duplicate key update version = values(version), modify_count = modify_count + values(modify_count), "+
			"count = count + values(count)", strings.Join(lockedValues, ","))
		if _, err = statsutil.ExecWithCtx(ctx, sctx, sql); err != nil {
			return err
		}
	}

	// Execute unlocked updates with positive delta
	if len(unlockedPosValues) > 0 {
		sql := fmt.Sprintf("insert into mysql.stats_meta (version, table_id, modify_count, count) values %s "+
			"on duplicate key update version = values(version), modify_count = modify_count + values(modify_count), "+
			"count = count + values(count)", strings.Join(unlockedPosValues, ","))
		if _, err = statsutil.ExecWithCtx(ctx, sctx, sql); err != nil {
			return err
		}
	}

	// Execute unlocked updates with negative delta
	if len(unlockedNegValues) > 0 {
		sql := fmt.Sprintf("insert into mysql.stats_meta (version, table_id, modify_count, count) values %s "+
			"on duplicate key update version = values(version), modify_count = modify_count + values(modify_count), "+
			"count = if(count > values(count), count - values(count), 0)", strings.Join(unlockedNegValues, ","))
		if _, err = statsutil.ExecWithCtx(ctx, sctx, sql); err != nil {
			return err
		}
	}

	return nil
}

var changeGlobalStatsTables = []string{
	"stats_meta", "stats_top_n", "stats_fm_sketch", "stats_buckets",
	"stats_histograms", "column_stats_usage",
}

// ChangeGlobalStatsID changes the table ID in global-stats to the new table ID.
func ChangeGlobalStatsID(
	ctx context.Context,
	sctx sessionctx.Context,
	from, to int64,
) error {
	for _, table := range changeGlobalStatsTables {
		_, err := statsutil.ExecWithCtx(
			ctx, sctx,
			"update mysql."+table+" set table_id = %? where table_id = %?",
			to, from,
		)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// UpdateStatsMetaVerAndLastHistUpdateVer updates the version to the newest TS for a table.
func UpdateStatsMetaVerAndLastHistUpdateVer(
	ctx context.Context,
	sctx sessionctx.Context,
	physicalID int64,
) (uint64, error) {
	startTS, err := statsutil.GetStartTS(sctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if _, err = statsutil.ExecWithCtx(
		ctx,
		sctx,
		"update mysql.stats_meta set version=%?, last_stats_histograms_version=%? where table_id =%?",
		startTS, startTS, physicalID,
	); err != nil {
		return 0, errors.Trace(err)
	}
	return startTS, nil
}
