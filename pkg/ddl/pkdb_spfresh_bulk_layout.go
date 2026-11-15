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

package ddl

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/lightning/backend/remote"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	spfreshBulkTrainingRowsPerShard   = uint64(4_096)
	spfreshBulkValidationRowsPerShard = uint64(1_024)
	spfreshBulkMaxTrainingRows        = uint64(1_000_000)
	spfreshBulkMaxValidationRows      = uint64(8_192)
)

type spfreshBulkLayout struct {
	shardCount       uint32
	sampleTargetRows uint64
	explicit         bool
}

// Nil estimates mean unknown current statistics, never a pseudo row count.
// They are planning inputs only; scan evidence owns exact source facts.
type spfreshBulkLayoutEstimate struct {
	rows         *uint64
	payloadBytes *uint64
	source       string
	version      uint64
	modifyCount  int64
}

// planSPFreshBulkLayout runs before scanning. Explicit layouts do not require
// statistics or resource inputs. A single shard does not allocate a sampler.
func planSPFreshBulkLayout(estimate spfreshBulkLayoutEstimate, buildShard, dimension uint32, capacities []uint64) (spfreshBulkLayout, error) {
	if dimension == 0 || dimension > 16383 {
		return spfreshBulkLayout{}, fmt.Errorf("SPFresh sample dimension must be in 1..16383")
	}
	count := uint64(buildShard)
	if count > uint64(model.SPFreshMaxBuildShards) {
		return spfreshBulkLayout{}, fmt.Errorf("SPFresh bulk shard count exceeds the current flat-route limit %d", model.SPFreshMaxBuildShards)
	}
	if count == 0 {
		if len(capacities) == 0 {
			return spfreshBulkLayout{}, fmt.Errorf("SPFresh automatic layout requires worker memory capacities")
		}
		capacity := capacities[0]
		for _, n := range capacities {
			if n == 0 {
				return spfreshBulkLayout{}, fmt.Errorf("SPFresh worker has no build row capacity")
			}
			capacity = min(capacity, n)
		}
		// Memory capacity alone determines automatic K. Extra workers do not
		// justify extra coarse boundaries, which can reduce search recall.
		// Unknown row statistics start with one shard; exact source validation
		// and execution memory admission remain authoritative.
		count = 1
		if estimate.rows != nil && *estimate.rows > 0 {
			count = 1 + (*estimate.rows-1)/capacity
		}
		count = min(count, uint64(model.SPFreshMaxBuildShards))
	}
	layout := spfreshBulkLayout{shardCount: uint32(count), explicit: buildShard != 0}
	if count > 1 {
		layout.sampleTargetRows = spfreshBulkSampleTarget(uint32(count), dimension)
	}
	return layout, nil
}

// finish contracts an automatic overestimate without rereading source objects.
// Explicit counts remain user-owned and must be feasible for the actual source.
func (layout spfreshBulkLayout) finish(rows uint64, dimension uint32) (spfreshBulkLayout, error) {
	if rows == 0 || layout.shardCount == 0 {
		return spfreshBulkLayout{}, fmt.Errorf("SPFresh bulk layout requires positive source rows and a pre-scan plan")
	}
	if rows < uint64(layout.shardCount) {
		if layout.explicit {
			return spfreshBulkLayout{}, fmt.Errorf("SPFresh bulk shard count exceeds source rows")
		}
		layout.shardCount = uint32(rows)
	}
	layout.sampleTargetRows = 0
	if layout.shardCount > 1 {
		layout.sampleTargetRows = min(rows, spfreshBulkSampleTarget(layout.shardCount, dimension))
	}
	return layout, nil
}

func prepareSPFreshBulkLayout(ctx context.Context, w *worker, run *spfreshBulkRunContext) (spfreshBulkLayout, error) {
	started := time.Now()
	estimate := spfreshBulkLayoutEstimate{source: "not_read"}
	var capacities []uint64
	if run.buildShard == 0 {
		var err error
		estimate, err = readSPFreshBulkLayoutEstimate(ctx, w, run)
		if err != nil {
			return spfreshBulkLayout{}, err
		}
		payloadBytes := uint64(0)
		if estimate.payloadBytes != nil {
			payloadBytes = *estimate.payloadBytes
		}
		var budgets []uint64
		budgets, capacities, err = readSPFreshBulkWorkerResources(ctx, spfreshBulkHTTPRemote{client: util.InternalHTTPClient()}, run, payloadBytes)
		if err != nil {
			return spfreshBulkLayout{}, err
		}
		run.workerMemoryBudgets = budgets
	}
	layout, err := planSPFreshBulkLayout(estimate, run.buildShard, run.dimension, capacities)
	if err != nil {
		return spfreshBulkLayout{}, err
	}
	source := "auto"
	if layout.explicit {
		source = "explicit"
	}
	logutil.DDLLogger().Info("SPFresh pre-scan shard layout",
		zap.String("build_id", run.buildID), zap.String("layout_source", source),
		zap.String("stats_source", estimate.source), zap.Uint64("stats_version", estimate.version),
		zap.Int64("modify_count", estimate.modifyCount), zap.Any("estimated_rows", estimate.rows),
		zap.Any("estimated_payload_bytes_per_row", estimate.payloadBytes),
		zap.Uint64s("worker_memory_budgets", run.workerMemoryBudgets), zap.Uint64s("worker_row_capacities", capacities),
		zap.Uint32("target_shards", layout.shardCount), zap.Uint64("sample_capacity", layout.sampleTargetRows),
		zap.Bool("sampler_enabled", layout.sampleTargetRows > 0), zap.Duration("duration", time.Since(started)))
	logSPFreshBulkScanUploadStage(run, "pre_scan_layout", time.Since(started))
	return layout, nil
}

func readSPFreshBulkLayoutEstimate(ctx context.Context, w *worker, run *spfreshBulkRunContext) (spfreshBulkLayoutEstimate, error) {
	estimate := spfreshBulkLayoutEstimate{source: "missing"}
	var stats *statistics.Table
	if w.statsHandle != nil {
		var ok bool
		stats, ok = w.statsHandle.GetNonPseudoPhysicalTableStats(run.tableID)
		if ok && stats.RealtimeCount >= 0 {
			rows := uint64(stats.RealtimeCount)
			estimate.rows, estimate.version, estimate.modifyCount = &rows, stats.Version, stats.ModifyCount
			estimate.source = "cache"
		}
	}
	if estimate.rows == nil {
		// A point read of metadata only: no ANALYZE, COUNT(*) or histogram load.
		session, err := w.sessPool.Get()
		if err != nil {
			return estimate, err
		}
		defer w.sessPool.Put(session)
		rows, _, err := session.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, nil,
			"SELECT count, modify_count, version FROM mysql.stats_meta WHERE table_id=%?", run.tableID)
		if err != nil {
			return estimate, err
		}
		if len(rows) == 1 && !rows[0].IsNull(0) && rows[0].GetInt64(0) >= 0 {
			count := uint64(rows[0].GetInt64(0))
			estimate.rows, estimate.modifyCount, estimate.version = &count, rows[0].GetInt64(1), rows[0].GetUint64(2)
			estimate.source = "meta"
		}
	}
	// Sidecar estimates use only already available column-size statistics.
	// Missing column stats remain unknown instead of assuming a zero payload.
	payload := uint64(0)
	if len(run.storedColumns) == 0 {
		estimate.payloadBytes = &payload
	} else if stats != nil && !stats.Pseudo && stats.RealtimeCount > 0 {
		known := true
		var encodedID []byte
		for _, column := range run.storedColumns {
			hist := stats.GetCol(column.ColumnID)
			if hist == nil || hist.TotColSize <= 0 {
				known = false
				break
			}
			// Sidecars use old-row EncodeValue framing. Column-size statistics
			// exclude the value flag and interleaved encoded column ID.
			var err error
			encodedID, err = codec.EncodeValue(time.UTC, encodedID[:0], types.NewIntDatum(column.ColumnID))
			if err != nil {
				return estimate, err
			}
			width := uint64(len(encodedID)) + 1 + 1 + (uint64(hist.TotColSize)-1)/uint64(stats.RealtimeCount)
			if err := addSPFreshBulkCount(&payload, width, "estimated stored payload bytes"); err != nil {
				return estimate, err
			}
		}
		if known {
			estimate.payloadBytes = &payload
		}
	}
	return estimate, nil
}

func readSPFreshBulkWorkerResources(ctx context.Context, client spfreshBulkDistributedRemote, run *spfreshBulkRunContext, payloadBytes uint64) ([]uint64, []uint64, error) {
	if len(run.workerEndpoints) == 0 {
		return nil, nil, fmt.Errorf("SPFresh bulk layout requires worker endpoints")
	}
	budgets := make([]uint64, len(run.workerEndpoints))
	capacities := make([]uint64, len(run.workerEndpoints))
	request := remote.SPFreshBulkResourcesRequest{Dimension: run.dimension,
		MaxPartitionSize: run.maxPartitionSize, TargetOccupancy: run.targetOccupancy, RowPayloadBytes: payloadBytes}
	group, groupCtx := errgroup.WithContext(ctx)
	for i, endpoint := range run.workerEndpoints {
		group.Go(func() error {
			resource, err := client.resources(groupCtx, endpoint, request)
			if err != nil {
				return err
			}
			if resource.MemoryBudgetBytes == 0 || resource.BuildRowCapacity == 0 {
				return fmt.Errorf("SPFresh worker has no build capacity")
			}
			budgets[i], capacities[i] = resource.MemoryBudgetBytes, resource.BuildRowCapacity
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, nil, err
	}
	return budgets, capacities, nil
}

// spfreshBulkSampleTarget includes disjoint training/validation rows and caps
// raw f32 storage at 1 GiB. Dimension is validated by DDL admission/layout.
func spfreshBulkSampleTarget(shards uint32, dimension uint32) uint64 {
	rows := min(uint64(shards)*spfreshBulkTrainingRowsPerShard, spfreshBulkMaxTrainingRows) + min(uint64(shards)*spfreshBulkValidationRowsPerShard, spfreshBulkMaxValidationRows)
	return min(rows, (1<<30)/(uint64(dimension)*4))
}
