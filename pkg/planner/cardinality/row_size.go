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
	"math"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/context"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

const pseudoColSize = 8.0

// GetIndexAvgRowSize computes average row size for a index scan.
func GetIndexAvgRowSize(ctx context.PlanContext, coll *statistics.HistColl, cols []*expression.Column, isUnique bool) (size float64) {
	size = GetAvgRowSize(ctx, coll, cols, true, true)
	// tablePrefix(1) + tableID(8) + indexPrefix(2) + indexID(8)
	// Because the cols for index scan always contain the handle, so we don't add the rowID here.
	size += 19
	if !isUnique {
		// add the len("_")
		size++
	}
	return
}

// GetTableAvgRowSize computes average row size for a table scan, exclude the index key-value pairs.
func GetTableAvgRowSize(ctx context.PlanContext, coll *statistics.HistColl, cols []*expression.Column, storeType kv.StoreType, handleInCols bool) (size float64) {
	size = GetAvgRowSize(ctx, coll, cols, false, true)
	switch storeType {
	case kv.TiKV:
		size += tablecodec.RecordRowKeyLen
		// The `cols` for TiKV always contain the row_id, so prefix row size subtract its length.
		size -= 8
	case kv.TiFlash:
		if !handleInCols {
			size += 8 /* row_id length */
		}
	}
	return
}

// GetAvgRowSize computes average row size for given columns.
func GetAvgRowSize(ctx context.PlanContext, coll *statistics.HistColl, cols []*expression.Column, isEncodedKey bool, isForScan bool) (size float64) {
	sessionVars := ctx.GetSessionVars()
	if coll.Pseudo || len(coll.Columns) == 0 || coll.RealtimeCount == 0 {
		size = pseudoColSize * float64(len(cols))
	} else {
		for _, col := range cols {
			colHist, ok := coll.Columns[col.UniqueID]
			// Normally this would not happen, it is for compatibility with old version stats which
			// does not include TotColSize.
			if !ok || (!colHist.IsHandle && colHist.TotColSize == 0 && (colHist.NullCount != coll.RealtimeCount)) {
				size += pseudoColSize
				continue
			}
			// We differentiate if the column is encoded as key or value, because the resulted size
			// is different.
			if sessionVars.EnableChunkRPC && !isForScan {
				size += AvgColSizeChunkFormat(colHist, coll.RealtimeCount)
			} else {
				size += AvgColSize(colHist, coll.RealtimeCount, isEncodedKey)
			}
		}
	}
	if sessionVars.EnableChunkRPC && !isForScan {
		// Add 1/8 byte for each column's nullBitMap byte.
		return size + float64(len(cols))/8
	}
	// Add 1 byte for each column's flag byte. See `encode` for details.
	return size + float64(len(cols))
}

// GetAvgRowSizeDataInDiskByRows computes average row size for given columns.
func GetAvgRowSizeDataInDiskByRows(coll *statistics.HistColl, cols []*expression.Column) (size float64) {
	if coll.Pseudo || len(coll.Columns) == 0 || coll.RealtimeCount == 0 {
		for _, col := range cols {
			size += float64(chunk.EstimateTypeWidth(col.GetStaticType()))
		}
	} else {
		for _, col := range cols {
			colHist, ok := coll.Columns[col.UniqueID]
			// Normally this would not happen, it is for compatibility with old version stats which
			// does not include TotColSize.
			if !ok || (!colHist.IsHandle && colHist.TotColSize == 0 && (colHist.NullCount != coll.RealtimeCount)) {
				size += float64(chunk.EstimateTypeWidth(col.GetStaticType()))
				continue
			}
			size += AvgColSizeDataInDiskByRows(colHist, coll.RealtimeCount)
		}
	}
	// Add 8 byte for each column's size record. See `DataInDiskByRows` for details.
	return size + float64(8*len(cols))
}

// AvgColSize is the average column size of the histogram. These sizes are derived from function `encode`
// and `Datum::ConvertTo`, so we need to update them if those 2 functions are changed.
func AvgColSize(c *statistics.Column, count int64, isKey bool) float64 {
	if count == 0 {
		return 0
	}
	// Note that, if the handle column is encoded as value, instead of key, i.e,
	// when the handle column is in a unique index, the real column size may be
	// smaller than 8 because it is encoded using `EncodeVarint`. Since we don't
	// know the exact value size now, use 8 as approximation.
	if c.IsHandle {
		return 8
	}
	histCount := c.TotalRowCount()
	notNullRatio := 1.0
	if histCount > 0 {
		notNullRatio = 1.0 - float64(c.NullCount)/histCount
	}
	switch c.Histogram.Tp.GetType() {
	case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeDuration, mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		return 8 * notNullRatio
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear, mysql.TypeEnum, mysql.TypeBit, mysql.TypeSet:
		if isKey {
			return 8 * notNullRatio
		}
	}
	// Keep two decimal place.
	return math.Round(float64(c.TotColSize)/float64(count)*100) / 100
}

// AvgColSizeChunkFormat is the average column size of the histogram. These sizes are derived from function `Encode`
// and `DecodeToChunk`, so we need to update them if those 2 functions are changed.
func AvgColSizeChunkFormat(c *statistics.Column, count int64) float64 {
	if count == 0 {
		return 0
	}
	fixedLen := chunk.GetFixedLen(c.Histogram.Tp)
	if fixedLen != -1 {
		return float64(fixedLen)
	}
	// Keep two decimal place.
	// Add 8 bytes for unfixed-len type's offsets.
	// Minus Log2(avgSize) for unfixed-len type LEN.
	avgSize := float64(c.TotColSize) / float64(count)
	if avgSize < 1 {
		return math.Round(avgSize*100)/100 + 8
	}
	return math.Round((avgSize-math.Log2(avgSize))*100)/100 + 8
}

// AvgColSizeDataInDiskByRows is the average column size of the histogram. These sizes are derived
// from `chunk.DataInDiskByRows` so we need to update them if those 2 functions are changed.
func AvgColSizeDataInDiskByRows(c *statistics.Column, count int64) float64 {
	if count == 0 {
		return 0
	}
	histCount := c.TotalRowCount()
	notNullRatio := 1.0
	if histCount > 0 {
		notNullRatio = 1.0 - float64(c.NullCount)/histCount
	}
	size := chunk.GetFixedLen(c.Histogram.Tp)
	if size != -1 {
		return float64(size) * notNullRatio
	}
	// Keep two decimal place.
	// Minus Log2(avgSize) for unfixed-len type LEN.
	avgSize := float64(c.TotColSize) / float64(count)
	if avgSize < 1 {
		return math.Round((avgSize)*100) / 100
	}
	return math.Round((avgSize-math.Log2(avgSize))*100) / 100
}
