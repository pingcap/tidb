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

package core

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tipb/go-tipb"
)

func buildVectorIndexExtra(
	indexInfo *model.IndexInfo,
	queryType tipb.ANNQueryType,
	distanceMetric tipb.VectorDistanceMetric,
	topk uint32,
	columnName string,
	refVec []byte,
	column *tipb.ColumnInfo,
) *physicalop.ColumnarIndexExtra {
	return &physicalop.ColumnarIndexExtra{
		IndexInfo: indexInfo,
		QueryInfo: &tipb.ColumnarIndexInfo{
			IndexType: tipb.ColumnarIndexType_TypeVector,
			Index: &tipb.ColumnarIndexInfo_AnnQueryInfo{
				AnnQueryInfo: &tipb.ANNQueryInfo{
					QueryType:      queryType,
					DistanceMetric: distanceMetric,
					TopK:           topk,
					ColumnName:     columnName,
					IndexId:        indexInfo.ID,
					RefVecF32:      refVec,
					Column:         *column,
				},
			},
		},
	}
}

// DistanceMetricToTipb converts a model.DistanceMetric to a tipb.VectorDistanceMetric.
func DistanceMetricToTipb(dm model.DistanceMetric) tipb.VectorDistanceMetric {
	switch dm {
	case model.DistanceMetricL2:
		return tipb.VectorDistanceMetric_L2
	case model.DistanceMetricCosine:
		return tipb.VectorDistanceMetric_COSINE
	case model.DistanceMetricInnerProduct:
		return tipb.VectorDistanceMetric_INNER_PRODUCT
	default:
		return tipb.VectorDistanceMetric_INVALID_DISTANCE_METRIC
	}
}

// buildTiCIVectorQueryInfo creates a TiCIVectorQueryInfo from index info and vector search properties.
func buildTiCIVectorQueryInfo(
	indexInfo *model.IndexInfo,
	vsInfo *expression.VSInfo,
	topK uint32,
	tableInfo *model.TableInfo,
) *tipb.TiCIVectorQueryInfo {
	if indexInfo.HybridInfo == nil {
		return nil
	}

	// Find the matching HybridVectorSpec by column ID.
	var matchedSpec *model.HybridVectorSpec
	var vecColInfo *model.ColumnInfo
	for _, vecSpec := range indexInfo.HybridInfo.Vector {
		if len(vecSpec.Columns) != 1 {
			continue
		}
		colInfo := tableInfo.Columns[vecSpec.Columns[0].Offset]
		if colInfo.ID == vsInfo.Column.ID {
			matchedSpec = vecSpec
			vecColInfo = colInfo
			break
		}
	}
	if matchedSpec == nil {
		return nil
	}

	// Map distance function name to tipb enum.
	dm, ok := model.IndexableFnNameToDistanceMetric[vsInfo.DistanceFnName.L]
	if !ok {
		return nil
	}
	distMetric := DistanceMetricToTipb(dm)

	// Serialize the query vector.
	queryVector := vsInfo.Vec.SerializeTo(nil)

	// Determine dimension.
	dim := uint32(vsInfo.Vec.Len())
	if matchedSpec.IndexInfo != nil && matchedSpec.IndexInfo.Dimension != nil {
		dim = uint32(*matchedSpec.IndexInfo.Dimension)
	}
	if dim != uint32(vsInfo.Vec.Len()) {
		return nil
	}

	return &tipb.TiCIVectorQueryInfo{
		IndexId:        indexInfo.ID,
		ColumnId:       vecColInfo.ID,
		DistanceMetric: distMetric,
		TopK:           topK,
		QueryVector:    queryVector,
		Dimension:      dim,
		ColumnName:     vecColInfo.Name.L,
	}
}
