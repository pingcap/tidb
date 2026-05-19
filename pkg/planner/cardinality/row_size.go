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

package cardinality

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/planner/util/rowsize"
	"github.com/pingcap/tidb/pkg/statistics"
)

// GetIndexAvgRowSize computes average row size for a index scan.
func GetIndexAvgRowSize(ctx planctx.PlanContext, coll *statistics.HistColl, cols []*expression.Column, isUnique bool) (size float64) {
	return rowsize.GetIndexAvgRowSize(ctx.GetSessionVars(), coll, cols, isUnique)
}

// GetTableAvgRowSize computes average row size for a table scan, exclude the index key-value pairs.
func GetTableAvgRowSize(ctx planctx.PlanContext, coll *statistics.HistColl, cols []*expression.Column, storeType kv.StoreType, handleInCols bool) (size float64) {
	return rowsize.GetTableAvgRowSize(ctx.GetSessionVars(), coll, cols, storeType, handleInCols)
}

// GetAvgRowSize computes average row size for given columns.
func GetAvgRowSize(ctx planctx.PlanContext, coll *statistics.HistColl, cols []*expression.Column, isEncodedKey bool, isForScan bool) (size float64) {
	return rowsize.GetAvgRowSize(ctx.GetSessionVars(), coll, cols, isEncodedKey, isForScan)
}

// GetAvgRowSizeDataInDiskByRows computes average row size for given columns.
func GetAvgRowSizeDataInDiskByRows(coll *statistics.HistColl, cols []*expression.Column) (size float64) {
	return rowsize.GetAvgRowSizeDataInDiskByRows(coll, cols)
}

// AvgColSize is the average column size of the histogram. These sizes are derived from function `encode`
// and `Datum::ConvertTo`, so we need to update them if those 2 functions are changed.
func AvgColSize(c *statistics.Column, count int64, isKey bool) float64 {
	return rowsize.AvgColSize(c, count, isKey)
}

// AvgColSizeChunkFormat is the average column size of the histogram. These sizes are derived from function `Encode`
// and `DecodeToChunk`, so we need to update them if those 2 functions are changed.
func AvgColSizeChunkFormat(c *statistics.Column, count int64) float64 {
	return rowsize.AvgColSizeChunkFormat(c, count)
}

// AvgColSizeDataInDiskByRows is the average column size of the histogram. These sizes are derived
// from `chunk.DataInDiskByRows` so we need to update them if those 2 functions are changed.
func AvgColSizeDataInDiskByRows(c *statistics.Column, count int64) float64 {
	return rowsize.AvgColSizeDataInDiskByRows(c, count)
}
