// Copyright 2025 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tipb/go-tipb"
)

// MergeDataTypeSample is the type value used in stats_global_merge_data for
// persisted per-partition sample collectors.
const MergeDataTypeSample = 2

// SavePartitionSampleData persists pre-serialized (proto-marshalled) sample data
// into mysql.stats_global_merge_data with type=2 and hist_id=0.
func SavePartitionSampleData(sctx sessionctx.Context, partitionID int64, data []byte) error {
	_, _, err := util.ExecRows(sctx,
		"REPLACE INTO mysql.stats_global_merge_data (table_id, type, hist_id, value) VALUES (%?, %?, 0, %?)",
		partitionID, MergeDataTypeSample, data,
	)
	return err
}

// LoadPartitionSample reads a persisted sample collector from
// mysql.stats_global_merge_data for the given partition.
// Returns (nil, nil) if no row is found.
func LoadPartitionSample(sctx sessionctx.Context, partitionID int64) (*statistics.ReservoirRowSampleCollector, error) {
	rows, _, err := util.ExecRows(sctx,
		"SELECT value FROM mysql.stats_global_merge_data WHERE table_id = %? AND type = %? AND hist_id = 0",
		partitionID, MergeDataTypeSample,
	)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	data := rows[0].GetBytes(0)
	var pbCollector tipb.RowSampleCollector
	if err := pbCollector.Unmarshal(data); err != nil {
		return nil, err
	}
	totalLen := len(pbCollector.FmSketch)
	rc := statistics.NewReservoirRowSampleCollector(len(pbCollector.Samples), totalLen)
	rc.Base().FromProto(&pbCollector, memory.NewTracker(-1, -1))
	return rc, nil
}

// DeletePartitionSamples removes all persisted sample data for a partition
// from mysql.stats_global_merge_data.
func DeletePartitionSamples(sctx sessionctx.Context, partitionID int64) error {
	_, _, err := util.ExecRows(sctx,
		"DELETE FROM mysql.stats_global_merge_data WHERE table_id = %? AND type = %?",
		partitionID, MergeDataTypeSample,
	)
	return err
}
