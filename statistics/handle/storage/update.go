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
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics/handle/cache"
	statsutil "github.com/pingcap/tidb/statistics/handle/util"
)

// UpdateStatsVersion will set statistics version to the newest TS,
// then tidb-server will reload automatic.
func UpdateStatsVersion(sctx sessionctx.Context) error {
	startTS, err := statsutil.GetStartTS(sctx)
	if err != nil {
		return errors.Trace(err)
	}
	if _, err = statsutil.Exec(sctx, "update mysql.stats_meta set version = %?", startTS); err != nil {
		return err
	}
	if _, err = statsutil.Exec(sctx, "update mysql.stats_extended set version = %?", startTS); err != nil {
		return err
	}
	if _, err = statsutil.Exec(sctx, "update mysql.stats_histograms set version = %?", startTS); err != nil {
		return err
	}
	return nil
}

// UpdateStatsMeta update the stats meta stat for this Table.
func UpdateStatsMeta(
	sctx sessionctx.Context,
	startTS uint64,
	delta variable.TableDelta,
	id int64,
	isLocked bool,
) (err error) {
	if isLocked {
		// use INSERT INTO ... ON DUPLICATE KEY UPDATE here to fill missing stats_table_locked.
		// Note: For locked tables, it is possible that the record gets deleted. So it can be negative.
		_, err = statsutil.Exec(sctx, "insert into mysql.stats_table_locked (version, table_id, modify_count, count) values (%?, %?, %?, %?) on duplicate key "+
			"update version = values(version), modify_count = modify_count + values(modify_count), count = count + values(count)",
			startTS, id, delta.Count, delta.Delta)
	} else {
		if delta.Delta < 0 {
			// use INSERT INTO ... ON DUPLICATE KEY UPDATE here to fill missing stats_meta.
			_, err = statsutil.Exec(sctx, "insert into mysql.stats_meta (version, table_id, modify_count, count) values (%?, %?, %?, 0) on duplicate key "+
				"update version = values(version), modify_count = modify_count + values(modify_count), count = if(count > %?, count - %?, 0)",
				startTS, id, delta.Count, -delta.Delta, -delta.Delta)
		} else {
			// use INSERT INTO ... ON DUPLICATE KEY UPDATE here to fill missing stats_meta.
			_, err = statsutil.Exec(sctx, "insert into mysql.stats_meta (version, table_id, modify_count, count) values (%?, %?, %?, %?) on duplicate key "+
				"update version = values(version), modify_count = modify_count + values(modify_count), count = count + values(count)", startTS,
				id, delta.Count, delta.Delta)
		}
		cache.TableRowStatsCache.Invalidate(id)
	}
	return err
}

// DumpTableStatColSizeToKV dumps the column size stats to storage.
func DumpTableStatColSizeToKV(sctx sessionctx.Context, id int64, delta variable.TableDelta) error {
	if len(delta.ColSize) == 0 {
		return nil
	}
	values := make([]string, 0, len(delta.ColSize))
	for histID, deltaColSize := range delta.ColSize {
		if deltaColSize == 0 {
			continue
		}
		values = append(values, fmt.Sprintf("(%d, 0, %d, 0, %d)", id, histID, deltaColSize))
	}
	if len(values) == 0 {
		return nil
	}
	sql := fmt.Sprintf("insert into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, tot_col_size) "+
		"values %s on duplicate key update tot_col_size = tot_col_size + values(tot_col_size)", strings.Join(values, ","))
	_, _, err := statsutil.ExecRows(sctx, sql)
	return errors.Trace(err)
}
