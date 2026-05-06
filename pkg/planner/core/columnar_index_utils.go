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
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tipb/go-tipb"
)

type vectorSearchIndexMatch struct {
	ColumnInfo        *model.ColumnInfo
	DistanceMetricPB  tipb.VectorDistanceMetric
	QueryVector       []byte
	Dimension         uint32
	IsTiCIHybridIndex bool
}

func hasTiCIHybridVectorIndex(indexInfo *model.IndexInfo) bool {
	return indexInfo != nil && indexInfo.IsTiCIIndex() && indexInfo.HasHybridVectorComponent()
}

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

func findVectorSearchIndexMatch(
	indexInfo *model.IndexInfo,
	vsInfo *expression.VSInfo,
	tableInfo *model.TableInfo,
) *vectorSearchIndexMatch {
	if indexInfo == nil || vsInfo == nil || tableInfo == nil {
		return nil
	}

	distanceMetric, ok := model.IndexableFnNameToDistanceMetric[vsInfo.DistanceFnName.L]
	if !ok {
		return nil
	}
	distMetricPB := DistanceMetricToTipb(distanceMetric)
	if distMetricPB == tipb.VectorDistanceMetric_INVALID_DISTANCE_METRIC {
		return nil
	}
	queryVector := vsInfo.Vec.SerializeTo(nil)

	if indexInfo.VectorInfo != nil {
		if len(indexInfo.Columns) == 0 {
			return nil
		}
		colInfo := tableInfo.Columns[indexInfo.Columns[0].Offset]
		if colInfo.ID != vsInfo.Column.ID || indexInfo.VectorInfo.DistanceMetric != distanceMetric {
			return nil
		}
		if indexInfo.VectorInfo.Dimension != uint64(vsInfo.Vec.Len()) {
			return nil
		}
		return &vectorSearchIndexMatch{
			ColumnInfo:       colInfo,
			DistanceMetricPB: distMetricPB,
			QueryVector:      queryVector,
			Dimension:        uint32(indexInfo.VectorInfo.Dimension),
		}
	}

	if indexInfo.HybridInfo == nil {
		return nil
	}
	for _, vecSpec := range indexInfo.HybridInfo.Vector {
		if len(vecSpec.Columns) != 1 || vecSpec.IndexInfo == nil {
			continue
		}
		colInfo := tableInfo.Columns[vecSpec.Columns[0].Offset]
		if colInfo.ID != vsInfo.Column.ID {
			continue
		}
		if model.DistanceMetric(vecSpec.IndexInfo.DistanceMetric) != distanceMetric {
			continue
		}
		dim := uint32(vsInfo.Vec.Len())
		if vecSpec.IndexInfo.Dimension != nil {
			dim = uint32(*vecSpec.IndexInfo.Dimension)
		}
		if dim != uint32(vsInfo.Vec.Len()) {
			continue
		}
		return &vectorSearchIndexMatch{
			ColumnInfo:        colInfo,
			DistanceMetricPB:  distMetricPB,
			QueryVector:       queryVector,
			Dimension:         dim,
			IsTiCIHybridIndex: true,
		}
	}
	return nil
}

func buildVectorIndexExtraForTopN(
	indexInfo *model.IndexInfo,
	vsInfo *expression.VSInfo,
	topK uint32,
	tableInfo *model.TableInfo,
) *physicalop.ColumnarIndexExtra {
	match := findVectorSearchIndexMatch(indexInfo, vsInfo, tableInfo)
	if match == nil || indexInfo == nil || indexInfo.VectorInfo == nil {
		return nil
	}
	return buildVectorIndexExtra(
		indexInfo,
		tipb.ANNQueryType_OrderBy,
		match.DistanceMetricPB,
		topK,
		match.ColumnInfo.Name.L,
		match.QueryVector,
		tidbutil.ColumnToProto(match.ColumnInfo, false, false),
	)
}

// buildTiCIVectorQueryInfo creates a TiCIVectorQueryInfo from index info and vector search properties.
func buildTiCIVectorQueryInfo(
	indexInfo *model.IndexInfo,
	vsInfo *expression.VSInfo,
	topK uint32,
	tableInfo *model.TableInfo,
) *tipb.TiCIVectorQueryInfo {
	match := findVectorSearchIndexMatch(indexInfo, vsInfo, tableInfo)
	if match == nil || !match.IsTiCIHybridIndex {
		return nil
	}

	return &tipb.TiCIVectorQueryInfo{
		IndexId:        indexInfo.ID,
		ColumnId:       match.ColumnInfo.ID,
		DistanceMetric: match.DistanceMetricPB,
		TopK:           topK,
		QueryVector:    match.QueryVector,
		Dimension:      match.Dimension,
		ColumnName:     match.ColumnInfo.Name.L,
	}
}
