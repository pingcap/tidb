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
	"bytes"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"go.uber.org/zap"
)

// NewHistCollBySelectivity creates new HistColl by the given statsNodes.
// TODO: remove this function after removing docs.pingcap.com/tidb/stable/system-variables#tidb_optimizer_selectivity_level.
func NewHistCollBySelectivity(sctx sessionctx.Context, coll *statistics.HistColl, statsNodes []*StatsNode) *statistics.HistColl {
	newColl := &statistics.HistColl{
		Columns:       make(map[int64]*statistics.Column),
		Indices:       make(map[int64]*statistics.Index),
		Idx2ColumnIDs: coll.Idx2ColumnIDs,
		ColID2IdxIDs:  coll.ColID2IdxIDs,
		RealtimeCount: coll.RealtimeCount,
	}
	for _, node := range statsNodes {
		if node.Tp == IndexType {
			idxHist, ok := coll.Indices[node.ID]
			if !ok {
				continue
			}
			newIdxHist, err := newIndexBySelectivity(sctx.GetSessionVars().StmtCtx, idxHist, node)
			if err != nil {
				logutil.BgLogger().Warn("something wrong happened when calculating row count, "+
					"failed to build histogram for index %v of table %v",
					zap.String("category", "Histogram-in-plan"), zap.String("index", idxHist.Info.Name.O),
					zap.String("table", idxHist.Info.Table.O), zap.Error(err))
				continue
			}
			newColl.Indices[node.ID] = newIdxHist
			continue
		}
		oldCol, ok := coll.Columns[node.ID]
		if !ok {
			continue
		}
		newCol := &statistics.Column{
			PhysicalID: oldCol.PhysicalID,
			Info:       oldCol.Info,
			IsHandle:   oldCol.IsHandle,
			CMSketch:   oldCol.CMSketch,
		}
		newCol.Histogram = *statistics.NewHistogram(oldCol.ID, int64(float64(oldCol.Histogram.NDV)*node.Selectivity), 0, 0, oldCol.Tp, chunk.InitialCapacity, 0)
		var err error
		splitRanges, ok := oldCol.Histogram.SplitRange(sctx.GetSessionVars().StmtCtx, node.Ranges, false)
		if !ok {
			logutil.BgLogger().Warn("the type of histogram and ranges mismatch", zap.String("category", "Histogram-in-plan"))
			continue
		}
		// Deal with some corner case.
		if len(splitRanges) > 0 {
			// Deal with NULL values.
			if splitRanges[0].LowVal[0].IsNull() {
				newCol.NullCount = oldCol.NullCount
				if splitRanges[0].HighVal[0].IsNull() {
					splitRanges = splitRanges[1:]
				} else {
					splitRanges[0].LowVal[0].SetMinNotNull()
				}
			}
		}
		if oldCol.IsHandle {
			err = newHistogramBySelectivity(sctx, node.ID, &oldCol.Histogram, &newCol.Histogram, splitRanges, coll.GetRowCountByIntColumnRanges)
		} else {
			err = newHistogramBySelectivity(sctx, node.ID, &oldCol.Histogram, &newCol.Histogram, splitRanges, coll.GetRowCountByColumnRanges)
		}
		if err != nil {
			logutil.BgLogger().Warn("something wrong happened when calculating row count", zap.String("category", "Histogram-in-plan"),
				zap.Error(err))
			continue
		}
		newCol.StatsLoadedStatus = oldCol.StatsLoadedStatus
		newColl.Columns[node.ID] = newCol
	}
	for id, idx := range coll.Indices {
		_, ok := newColl.Indices[id]
		if !ok {
			newColl.Indices[id] = idx
		}
	}
	for id, col := range coll.Columns {
		_, ok := newColl.Columns[id]
		if !ok {
			newColl.Columns[id] = col
		}
	}
	return newColl
}

type countByRangeFunc = func(sessionctx.Context, int64, []*ranger.Range) (float64, error)

// newHistogramBySelectivity fulfills the content of new histogram by the given selectivity result.
// TODO: remove this function after removing docs.pingcap.com/tidb/stable/system-variables#tidb_optimizer_selectivity_level.
func newHistogramBySelectivity(sctx sessionctx.Context, histID int64, oldHist, newHist *statistics.Histogram, ranges []*ranger.Range, cntByRangeFunc countByRangeFunc) error {
	cntPerVal := int64(oldHist.AvgCountPerNotNullValue(int64(oldHist.TotalRowCount())))
	var totCnt int64
	for boundIdx, ranIdx, highRangeIdx := 0, 0, 0; boundIdx < oldHist.Bounds.NumRows() && ranIdx < len(ranges); boundIdx, ranIdx = boundIdx+2, highRangeIdx {
		for highRangeIdx < len(ranges) && chunk.Compare(oldHist.Bounds.GetRow(boundIdx+1), 0, &ranges[highRangeIdx].HighVal[0]) >= 0 {
			highRangeIdx++
		}
		if boundIdx+2 >= oldHist.Bounds.NumRows() && highRangeIdx < len(ranges) && ranges[highRangeIdx].HighVal[0].Kind() == types.KindMaxValue {
			highRangeIdx++
		}
		if ranIdx == highRangeIdx {
			continue
		}
		cnt, err := cntByRangeFunc(sctx, histID, ranges[ranIdx:highRangeIdx])
		// This should not happen.
		if err != nil {
			return err
		}
		if cnt == 0 {
			continue
		}
		if int64(cnt) > oldHist.BucketCount(boundIdx/2) {
			cnt = float64(oldHist.BucketCount(boundIdx / 2))
		}
		newHist.Bounds.AppendRow(oldHist.Bounds.GetRow(boundIdx))
		newHist.Bounds.AppendRow(oldHist.Bounds.GetRow(boundIdx + 1))
		totCnt += int64(cnt)
		bkt := statistics.Bucket{Count: totCnt}
		if chunk.Compare(oldHist.Bounds.GetRow(boundIdx+1), 0, &ranges[highRangeIdx-1].HighVal[0]) == 0 && !ranges[highRangeIdx-1].HighExclude {
			bkt.Repeat = cntPerVal
		}
		newHist.Buckets = append(newHist.Buckets, bkt)
		switch newHist.Tp.EvalType() {
		case types.ETString, types.ETDecimal, types.ETDatetime, types.ETTimestamp:
			newHist.Scalars = append(newHist.Scalars, oldHist.Scalars[boundIdx/2])
		}
	}
	return nil
}

// TODO: remove this function after removing docs.pingcap.com/tidb/stable/system-variables#tidb_optimizer_selectivity_level.
func newIndexBySelectivity(sc *stmtctx.StatementContext, idx *statistics.Index, statsNode *StatsNode) (*statistics.Index, error) {
	var (
		ranLowEncode, ranHighEncode []byte
		err                         error
	)
	newIndexHist := &statistics.Index{Info: idx.Info, StatsVer: idx.StatsVer, CMSketch: idx.CMSketch, PhysicalID: idx.PhysicalID}
	newIndexHist.Histogram = *statistics.NewHistogram(idx.Histogram.ID, int64(float64(idx.Histogram.NDV)*statsNode.Selectivity), 0, 0, types.NewFieldType(mysql.TypeBlob), chunk.InitialCapacity, 0)

	lowBucketIdx, highBucketIdx := 0, 0
	var totCnt int64

	// Bucket bound of index is encoded one, so we need to decode it if we want to calculate the fraction accurately.
	// TODO: enhance its calculation.
	// Now just remove the bucket that no range fell in.
	for _, ran := range statsNode.Ranges {
		lowBucketIdx = highBucketIdx
		ranLowEncode, ranHighEncode, err = ran.Encode(sc, ranLowEncode, ranHighEncode)
		if err != nil {
			return nil, err
		}
		for ; highBucketIdx < idx.Histogram.Len(); highBucketIdx++ {
			// Encoded value can only go to its next quickly. So ranHighEncode is actually range.HighVal's PrefixNext value.
			// So the Bound should also go to its PrefixNext.
			bucketLowerEncoded := idx.Histogram.Bounds.GetRow(highBucketIdx * 2).GetBytes(0)
			if bytes.Compare(ranHighEncode, kv.Key(bucketLowerEncoded).PrefixNext()) < 0 {
				break
			}
		}
		for ; lowBucketIdx < highBucketIdx; lowBucketIdx++ {
			bucketUpperEncoded := idx.Histogram.Bounds.GetRow(lowBucketIdx*2 + 1).GetBytes(0)
			if bytes.Compare(ranLowEncode, bucketUpperEncoded) <= 0 {
				break
			}
		}
		if lowBucketIdx >= idx.Histogram.Len() {
			break
		}
		for i := lowBucketIdx; i < highBucketIdx; i++ {
			newIndexHist.Histogram.Bounds.AppendRow(idx.Histogram.Bounds.GetRow(i * 2))
			newIndexHist.Histogram.Bounds.AppendRow(idx.Histogram.Bounds.GetRow(i*2 + 1))
			totCnt += idx.Histogram.BucketCount(i)
			newIndexHist.Histogram.Buckets = append(newIndexHist.Histogram.Buckets, statistics.Bucket{Repeat: idx.Histogram.Buckets[i].Repeat, Count: totCnt})
			newIndexHist.Histogram.Scalars = append(newIndexHist.Histogram.Scalars, idx.Histogram.Scalars[i])
		}
	}
	return newIndexHist, nil
}
