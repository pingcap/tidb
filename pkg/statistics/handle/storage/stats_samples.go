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

package storage

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tipb/go-tipb"
)

// SaveSampleCollectorToStorage persists a pruned partition sample collector
// into mysql.stats_samples. It uses REPLACE INTO so repeated calls for the
// same (table_id, partition_id) are idempotent.
func SaveSampleCollectorToStorage(
	sctx sessionctx.Context,
	tableID, partitionID int64,
	version uint64,
	collector *statistics.ReservoirRowSampleCollector,
) error {
	pbCollector := collector.Base().ToProto()
	data, err := pbCollector.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = util.Exec(sctx,
		"REPLACE INTO mysql.stats_samples (table_id, partition_id, version, sample_data, max_sample_size, sample_count, row_count) VALUES (%?, %?, %?, %?, %?, %?, %?)",
		tableID, partitionID, version, data,
		collector.MaxSampleSize, len(collector.Base().Samples), collector.Base().Count,
	)
	return errors.Trace(err)
}

// LoadSampleCollectorsFromStorage loads all persisted partition sample
// collectors for the given table, optionally excluding certain partition IDs.
func LoadSampleCollectorsFromStorage(
	sctx sessionctx.Context,
	tableID int64,
	excludePartitionIDs []int64,
) (map[int64]*statistics.ReservoirRowSampleCollector, error) {
	var sql string
	var args []any
	if len(excludePartitionIDs) == 0 {
		sql = "SELECT partition_id, sample_data, max_sample_size FROM mysql.stats_samples WHERE table_id = %?"
		args = []any{tableID}
	} else {
		// Convert int64 IDs to strings for the %? SQL escaper.
		strIDs := make([]string, 0, len(excludePartitionIDs))
		for _, id := range excludePartitionIDs {
			strIDs = append(strIDs, fmt.Sprintf("%d", id))
		}
		sql = "SELECT partition_id, sample_data, max_sample_size FROM mysql.stats_samples WHERE table_id = %? AND partition_id NOT IN (%?)"
		args = []any{tableID, strIDs}
	}
	rows, _, err := util.ExecRows(sctx, sql, args...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	result := make(map[int64]*statistics.ReservoirRowSampleCollector, len(rows))
	tracker := memory.NewTracker(-1, -1)
	for _, row := range rows {
		partitionID := row.GetInt64(0)
		data := row.GetBytes(1)
		maxSampleSize := int(row.GetInt64(2))

		pbCollector := &tipb.RowSampleCollector{}
		if err := pbCollector.Unmarshal(data); err != nil {
			return nil, errors.Trace(err)
		}
		collector := statistics.NewReservoirRowSampleCollector(maxSampleSize, 0)
		collector.Base().FromProto(pbCollector, tracker)
		result[partitionID] = collector
	}
	return result, nil
}

// DeleteSamplesByPartition removes persisted samples for a specific partition.
func DeleteSamplesByPartition(ctx context.Context, sctx sessionctx.Context, partitionID int64) error {
	_, err := util.ExecWithCtx(ctx, sctx,
		"DELETE FROM mysql.stats_samples WHERE partition_id = %?",
		partitionID,
	)
	return errors.Trace(err)
}

// DeleteSamplesByTable removes all persisted samples for a table.
func DeleteSamplesByTable(ctx context.Context, sctx sessionctx.Context, tableID int64) error {
	_, err := util.ExecWithCtx(ctx, sctx,
		"DELETE FROM mysql.stats_samples WHERE table_id = %?",
		tableID,
	)
	return errors.Trace(err)
}
