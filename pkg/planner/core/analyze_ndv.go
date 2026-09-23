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

package core

import (
	"context"
	"fmt"
	"slices"
	"strconv"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
)

// checkRetainedNDVSketches checks the saved sketches that the global merge
// reuses: those of partitions this ANALYZE skips. It returns why those
// partitions must be analyzed too. Only headers are read here; the merge
// validates its actual inputs again.
func (b *PlanBuilder) checkRetainedNDVSketches(tbl *resolve.TableNameW, updated []int64, cols []*model.ColumnInfo, rate float64) (string, error) {
	defs := tbl.TableInfo.Partition.Definitions
	retained := make([]model.PartitionDefinition, 0, len(defs))
	retainedIDs := make([]string, 0, len(defs))
	for _, def := range defs {
		if !slices.Contains(updated, def.ID) {
			retained = append(retained, def)
			retainedIDs = append(retainedIDs, strconv.FormatInt(def.ID, 10))
		}
	}
	if len(retained) == 0 {
		return "", nil
	}

	type target struct{ isIndex, id int64 }
	type expected struct {
		target
		name string
		rate float64
	}
	targets := make([]expected, 0, len(cols))
	for _, col := range cols {
		if !col.IsVirtualGenerated() {
			targets = append(targets, expected{target{0, col.ID}, "column " + col.Name.O, rate})
		}
	}
	indexes, independent, _ := getModifiedIndexesInfoForAnalyze(b.ctx, tbl.TableInfo, len(cols) == len(tbl.TableInfo.Columns), cols)
	for _, idx := range append(indexes, independent...) {
		idxRate := rate
		if idx.MVIndex {
			idxRate = 1
		}
		// idx may use offsets into cols, so check the table's own index.
		for _, col := range tbl.TableInfo.FindIndexByName(idx.Name.L).Columns {
			if col.Length != types.UnspecifiedLength || tbl.TableInfo.Columns[col.Offset].IsVirtualGenerated() {
				idxRate = 1
			}
		}
		targets = append(targets, expected{target{1, idx.ID}, "index " + idx.Name.O, idxRate})
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStatsForegroundPriority)
	rows, _, err := b.ctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, nil,
		"SELECT table_id, is_index, hist_id, SUBSTRING(value, 1, 10) FROM mysql.stats_fm_sketch WHERE table_id IN (%?)", retainedIDs)
	if err != nil {
		return "", err
	}
	type key struct {
		partition int64
		target
	}
	saved := make(map[key][]byte, len(rows))
	for _, row := range rows {
		saved[key{row.GetInt64(0), target{row.GetInt64(1), row.GetInt64(2)}}] = row.GetBytes(3)
	}
	for _, def := range retained {
		for _, want := range targets {
			var problem string
			if data := saved[key{def.ID, want.target}]; len(data) == 0 {
				// A full-input global merge skips partitions without a sketch.
				if want.rate == 1 {
					continue
				}
				problem = "has no saved sketch"
			} else if got, err := statistics.SavedFMSketchNDVRate(data); err != nil {
				problem = "has an unreadable sketch: " + err.Error()
			} else if got != want.rate {
				problem = fmt.Sprintf("uses NDVRATE %g, requested %g", got, want.rate)
			} else {
				continue
			}
			return fmt.Sprintf("partition %s %s %s", def.Name.O, want.name, problem), nil
		}
	}
	return "", nil
}
