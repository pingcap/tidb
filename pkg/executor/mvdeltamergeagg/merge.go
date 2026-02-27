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

package mvdeltamergeagg

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

type aggMerger interface {
	outputColIDs() []int
	mergeChunk(input *chunk.Chunk, computedByOrder []*chunk.Column, outputCols []*chunk.Column, workerData *mvMergeAggWorkerData) error
}

type depRefSource uint8

const (
	depFromInput depRefSource = iota
	depFromComputed
)

type depRef struct {
	source depRefSource
	idx    int
}

func resolveDepRef(depColID int, colID2ComputedIdx map[int]int, deltaAggColCount int) (depRef, error) {
	if idx, ok := colID2ComputedIdx[depColID]; ok {
		return depRef{
			source: depFromComputed,
			idx:    idx,
		}, nil
	}
	if depColID < 0 || depColID >= deltaAggColCount {
		return depRef{}, errors.Errorf(
			"dependency col id %d is invalid: expect delta agg range [0,%d) or previously computed columns",
			depColID,
			deltaAggColCount,
		)
	}
	return depRef{
		source: depFromInput,
		idx:    depColID,
	}, nil
}

func getDepColumn(input *chunk.Chunk, computedByOrder []*chunk.Column, ref depRef) (*chunk.Column, error) {
	switch ref.source {
	case depFromInput:
		if ref.idx < 0 || ref.idx >= input.NumCols() {
			return nil, errors.Errorf("input dependency col idx %d out of range [0,%d)", ref.idx, input.NumCols())
		}
		return input.Column(ref.idx), nil
	case depFromComputed:
		if ref.idx < 0 || ref.idx >= len(computedByOrder) {
			return nil, errors.Errorf("computed dependency idx %d out of range [0,%d)", ref.idx, len(computedByOrder))
		}
		return computedByOrder[ref.idx], nil
	default:
		return nil, errors.Errorf("unknown dependency source %d", ref.source)
	}
}

func resolveFieldTypeByColID(colID int, childTypes []*types.FieldType) (*types.FieldType, error) {
	if colID < 0 || colID >= len(childTypes) {
		return nil, errors.Errorf("col id %d out of range [0,%d)", colID, len(childTypes))
	}
	retTp := childTypes[colID]
	if retTp == nil {
		return nil, errors.Errorf("col id %d type is unavailable", colID)
	}
	return retTp, nil
}
