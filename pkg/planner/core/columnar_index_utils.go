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
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tipb/go-tipb"
)

func buildVectorIndexExtra(
	indexInfo *model.IndexInfo,
	queryType tipb.ANNQueryType,
	distanceMetric tipb.VectorDistanceMetric,
	topk uint32,
	columnName string,
	columnID int64,
	indexID int64,
	refVec []byte,
	column *tipb.ColumnInfo,
) *ColumnarIndexExtra {
	return &ColumnarIndexExtra{
		IndexInfo: indexInfo,
		QueryInfo: &tipb.ColumnarIndexInfo{
			IndexType: tipb.ColumnarIndexType_TypeVector,
			Index: &tipb.ColumnarIndexInfo_AnnQueryInfo{
				AnnQueryInfo: &tipb.ANNQueryInfo{
					QueryType:          queryType,
					DistanceMetric:     distanceMetric,
					TopK:               topk,
					ColumnName:         columnName,
					DeprecatedColumnId: &columnID, // deprecated field, will be removed after TiFlash supports the new field.
					IndexId:            indexID,
					RefVecF32:          refVec,
					Column:             *column,
				},
			},
		},
	}
}

func buildInvertedIndexExtra(
	indexInfo *model.IndexInfo,
	columnID int64,
	indexID int64,
) *ColumnarIndexExtra {
	return &ColumnarIndexExtra{
		IndexInfo: indexInfo,
		QueryInfo: &tipb.ColumnarIndexInfo{
			IndexType: tipb.ColumnarIndexType_TypeInverted,
			Index: &tipb.ColumnarIndexInfo_InvertedQueryInfo{
				InvertedQueryInfo: &tipb.InvertedQueryInfo{
					IndexId:  indexID,
					ColumnId: columnID,
				},
			},
		},
	}
}
