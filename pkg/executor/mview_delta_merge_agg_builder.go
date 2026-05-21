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

package executor

import "github.com/pingcap/tidb/pkg/executor/mviewdeltamergeagg"

// MViewDeltaMergeAggExec is the executor alias for delta MView merge.
type MViewDeltaMergeAggExec = mviewdeltamergeagg.Exec

// MViewDeltaMergeAggMapping is the mapping alias for delta MView aggregate merge.
type MViewDeltaMergeAggMapping = mviewdeltamergeagg.Mapping

// MinMaxRecomputeExec is the alias for min/max recompute execution descriptor.
type MinMaxRecomputeExec = mviewdeltamergeagg.MinMaxRecomputeExec

// MinMaxRecomputeSpec is the alias for per-mapping min/max recompute metadata.
type MinMaxRecomputeSpec = mviewdeltamergeagg.MinMaxRecomputeSpec

// MinMaxRecomputeStrategy is the alias for min/max recompute strategy enum.
type MinMaxRecomputeStrategy = mviewdeltamergeagg.MinMaxRecomputeStrategy

// MinMaxRecomputeSingleRowExec is the alias for single-row recompute runtime wiring.
type MinMaxRecomputeSingleRowExec = mviewdeltamergeagg.MinMaxRecomputeSingleRowExec

// MinMaxRecomputeSingleRowWorker is the alias for single-row worker slot.
type MinMaxRecomputeSingleRowWorker = mviewdeltamergeagg.MinMaxRecomputeSingleRowWorker

// MinMaxBatchLookupContent is the alias for batch lookup key content.
type MinMaxBatchLookupContent = mviewdeltamergeagg.MinMaxBatchLookupContent

// MinMaxBatchBuildRequest is the alias for batch build request.
type MinMaxBatchBuildRequest = mviewdeltamergeagg.MinMaxBatchBuildRequest

// MinMaxBatchExecBuilder is the alias for batch recompute executor builder.
type MinMaxBatchExecBuilder = mviewdeltamergeagg.MinMaxBatchExecBuilder

// MViewDeltaMergeAggRowOpType is the alias of row operation type for MView merge.
type MViewDeltaMergeAggRowOpType = mviewdeltamergeagg.RowOpType

// MViewDeltaMergeAggRowOp is the alias of row operation item for MView merge.
type MViewDeltaMergeAggRowOp = mviewdeltamergeagg.RowOp

// MViewDeltaMergeAggChunkResult is the alias of one merged chunk result.
type MViewDeltaMergeAggChunkResult = mviewdeltamergeagg.ChunkResult

// MViewDeltaMergeAggResultWriter is the alias of merge result writer interface.
type MViewDeltaMergeAggResultWriter = mviewdeltamergeagg.ResultWriter

// MViewDeltaMergeAgg row operation constants.
const (
	// Min/max recompute strategy constants.
	MinMaxRecomputeUnknown   = mviewdeltamergeagg.MinMaxRecomputeUnknown
	MinMaxRecomputeSingleRow = mviewdeltamergeagg.MinMaxRecomputeSingleRow
	MinMaxRecomputeBatch     = mviewdeltamergeagg.MinMaxRecomputeBatch

	MViewDeltaMergeAggRowOpNoOp   = mviewdeltamergeagg.RowOpNoOp
	MViewDeltaMergeAggRowOpInsert = mviewdeltamergeagg.RowOpInsert
	MViewDeltaMergeAggRowOpUpdate = mviewdeltamergeagg.RowOpUpdate
	MViewDeltaMergeAggRowOpDelete = mviewdeltamergeagg.RowOpDelete
)
