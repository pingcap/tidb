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

import "github.com/pingcap/tidb/pkg/executor/mvdeltamergeagg"

// MVDeltaMergeAggExec is the executor alias for delta MV merge.
type MVDeltaMergeAggExec = mvdeltamergeagg.Exec

// MVDeltaMergeAggMapping is the mapping alias for delta MV aggregate merge.
type MVDeltaMergeAggMapping = mvdeltamergeagg.Mapping

// MinMaxRecomputeExec is the alias for min/max recompute execution descriptor.
type MinMaxRecomputeExec = mvdeltamergeagg.MinMaxRecomputeExec

// MinMaxRecomputeMapping is the alias for per-mapping min/max recompute metadata.
type MinMaxRecomputeMapping = mvdeltamergeagg.MinMaxRecomputeMapping

// MinMaxRecomputeStrategy is the alias for min/max recompute strategy enum.
type MinMaxRecomputeStrategy = mvdeltamergeagg.MinMaxRecomputeStrategy

// MinMaxRecomputeSingleRowExec is the alias for single-row recompute runtime wiring.
type MinMaxRecomputeSingleRowExec = mvdeltamergeagg.MinMaxRecomputeSingleRowExec

// MinMaxRecomputeSingleRowWorker is the alias for single-row worker slot.
type MinMaxRecomputeSingleRowWorker = mvdeltamergeagg.MinMaxRecomputeSingleRowWorker

// MinMaxBatchLookupContent is the alias for batch lookup key content.
type MinMaxBatchLookupContent = mvdeltamergeagg.MinMaxBatchLookupContent

// MinMaxBatchBuildRequest is the alias for batch build request.
type MinMaxBatchBuildRequest = mvdeltamergeagg.MinMaxBatchBuildRequest

// MinMaxBatchExecBuilder is the alias for batch recompute executor builder.
type MinMaxBatchExecBuilder = mvdeltamergeagg.MinMaxBatchExecBuilder

// MVDeltaMergeAggRowOpType is the alias of row operation type for MV merge.
type MVDeltaMergeAggRowOpType = mvdeltamergeagg.RowOpType

// MVDeltaMergeAggRowOp is the alias of row operation item for MV merge.
type MVDeltaMergeAggRowOp = mvdeltamergeagg.RowOp

// MVDeltaMergeAggChunkResult is the alias of one merged chunk result.
type MVDeltaMergeAggChunkResult = mvdeltamergeagg.ChunkResult

// MVDeltaMergeAggResultWriter is the alias of merge result writer interface.
type MVDeltaMergeAggResultWriter = mvdeltamergeagg.ResultWriter

// MVDeltaMergeAgg row operation constants.
const (
	// Min/max recompute strategy constants.
	MinMaxRecomputeSingleRow = mvdeltamergeagg.MinMaxRecomputeSingleRow
	MinMaxRecomputeBatch     = mvdeltamergeagg.MinMaxRecomputeBatch

	MVDeltaMergeAggRowOpNoOp   = mvdeltamergeagg.RowOpNoOp
	MVDeltaMergeAggRowOpInsert = mvdeltamergeagg.RowOpInsert
	MVDeltaMergeAggRowOpUpdate = mvdeltamergeagg.RowOpUpdate
	MVDeltaMergeAggRowOpDelete = mvdeltamergeagg.RowOpDelete
)
