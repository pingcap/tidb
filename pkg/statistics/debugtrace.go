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

package statistics

import (
	"slices"

	"github.com/pingcap/tidb/pkg/planner/context"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/types"
	"golang.org/x/exp/maps"
)

/*
 Below is debug trace for statistics.Table and types related to fields in statistics.Table.
*/

// StatsTblTraceInfo is simplified from Table and used for debug trace.
type StatsTblTraceInfo struct {
	Columns     []*statsTblColOrIdxInfo
	Indexes     []*statsTblColOrIdxInfo
	PhysicalID  int64
	Version     uint64
	Count       int64
	ModifyCount int64
}

type statsTblColOrIdxInfo struct {
	CMSketchInfo  *cmSketchInfo
	Name          string
	LoadingStatus string

	ID                int64
	NDV               int64
	NullCount         int64
	LastUpdateVersion uint64
	TotColSize        int64
	Correlation       float64
	StatsVer          int64

	HistogramSize int
	TopNSize      int
}

type cmSketchInfo struct {
	Depth        int32
	Width        int32
	Count        uint64
	DefaultValue uint64
}

func traceCMSketchInfo(cms *CMSketch) *cmSketchInfo {
	if cms == nil {
		return nil
	}
	return &cmSketchInfo{
		Depth:        cms.depth,
		Width:        cms.width,
		Count:        cms.count,
		DefaultValue: cms.defaultValue,
	}
}

func traceColStats(colStats *Column, id int64, out *statsTblColOrIdxInfo) {
	if colStats == nil {
		return
	}
	out.ID = id
	if colStats.Info != nil {
		out.Name = colStats.Info.Name.O
	}
	out.NDV = colStats.NDV
	out.NullCount = colStats.NullCount
	out.LastUpdateVersion = colStats.LastUpdateVersion
	out.TotColSize = colStats.TotColSize
	out.Correlation = colStats.Correlation
	out.StatsVer = colStats.StatsVer
	out.LoadingStatus = colStats.StatsLoadedStatus.StatusToString()
	if colStats.Histogram.Buckets == nil {
		out.HistogramSize = -1
	} else {
		out.HistogramSize = len(colStats.Histogram.Buckets)
	}
	if colStats.TopN == nil {
		out.TopNSize = -1
	} else {
		out.TopNSize = len(colStats.TopN.TopN)
	}
	out.CMSketchInfo = traceCMSketchInfo(colStats.CMSketch)
}

func traceIdxStats(idxStats *Index, id int64, out *statsTblColOrIdxInfo) {
	if idxStats == nil {
		return
	}
	out.ID = id
	if idxStats.Info != nil {
		out.Name = idxStats.Info.Name.O
	}
	out.NDV = idxStats.NDV
	out.NullCount = idxStats.NullCount
	out.LastUpdateVersion = idxStats.LastUpdateVersion
	out.TotColSize = idxStats.TotColSize
	out.Correlation = idxStats.Correlation
	out.StatsVer = idxStats.StatsVer
	out.LoadingStatus = idxStats.StatsLoadedStatus.StatusToString()
	if idxStats.Histogram.Buckets == nil {
		out.HistogramSize = -1
	} else {
		out.HistogramSize = len(idxStats.Histogram.Buckets)
	}
	if idxStats.TopN == nil {
		out.TopNSize = -1
	} else {
		out.TopNSize = len(idxStats.TopN.TopN)
	}
	out.CMSketchInfo = traceCMSketchInfo(idxStats.CMSketch)
}

// TraceStatsTbl converts a Table to StatsTblTraceInfo, which is suitable for the debug trace.
func TraceStatsTbl(statsTbl *Table) *StatsTblTraceInfo {
	if statsTbl == nil {
		return nil
	}
	// Collect table level information
	colNum := len(statsTbl.Columns)
	idxNum := len(statsTbl.Indices)
	traceInfo := &StatsTblTraceInfo{
		PhysicalID:  statsTbl.PhysicalID,
		Version:     statsTbl.Version,
		Count:       statsTbl.RealtimeCount,
		ModifyCount: statsTbl.ModifyCount,
		Columns:     make([]*statsTblColOrIdxInfo, colNum),
		Indexes:     make([]*statsTblColOrIdxInfo, idxNum),
	}

	// Collect information for each Column
	colTraces := make([]statsTblColOrIdxInfo, colNum)
	colIDs := maps.Keys(statsTbl.Columns)
	slices.Sort(colIDs)
	for i, id := range colIDs {
		colStatsTrace := &colTraces[i]
		traceInfo.Columns[i] = colStatsTrace
		colStats := statsTbl.Columns[id]
		traceColStats(colStats, id, colStatsTrace)
	}

	// Collect information for each Index
	idxTraces := make([]statsTblColOrIdxInfo, idxNum)
	idxIDs := maps.Keys(statsTbl.Indices)
	slices.Sort(idxIDs)
	for i, id := range idxIDs {
		idxStatsTrace := &idxTraces[i]
		traceInfo.Indexes[i] = idxStatsTrace
		idxStats := statsTbl.Indices[id]
		traceIdxStats(idxStats, id, idxStatsTrace)
	}
	return traceInfo
}

/*
 Below is debug trace for (*Histogram).locateBucket().
*/

type locateBucketInfo struct {
	Value          string
	BucketIdx      int
	Exceed         bool
	InBucket       bool
	MatchLastValue bool
}

func debugTraceLocateBucket(
	s context.PlanContext,
	value *types.Datum,
	exceed bool,
	bucketIdx int,
	inBucket bool,
	matchLastValue bool,
) {
	root := debugtrace.GetOrInitDebugTraceRoot(s)
	traceInfo := &locateBucketInfo{
		Value:          value.String(),
		Exceed:         exceed,
		BucketIdx:      bucketIdx,
		InBucket:       inBucket,
		MatchLastValue: matchLastValue,
	}
	root.AppendStepWithNameToCurrentContext(traceInfo, "Locate value in buckets")
}

/*
 Below is debug trace for used buckets in the histograms during estimation.
*/

type bucketInfo struct {
	Index  int
	Count  int64
	Repeat int64
}

// DebugTraceBuckets is used to trace the buckets used in the histogram.
func DebugTraceBuckets(s context.PlanContext, hg *Histogram, bucketIdxs []int) {
	root := debugtrace.GetOrInitDebugTraceRoot(s)
	buckets := make([]bucketInfo, len(bucketIdxs))
	for i := range buckets {
		idx := bucketIdxs[i]
		buckets[i].Index = idx
		if idx >= len(hg.Buckets) || idx < 0 {
			buckets[i].Repeat = -1
			buckets[i].Count = -1
			continue
		}
		bkt := hg.Buckets[idx]
		buckets[i].Repeat = bkt.Repeat
		buckets[i].Count = bkt.Count
	}
	root.AppendStepWithNameToCurrentContext(buckets, "Related Buckets in Histogram")
}

/*
 Below is debug trace for used TopN range during estimation.
*/

type topNRangeInfo struct {
	FirstEncoded []byte
	LastEncoded  []byte
	Count        []uint64
	FirstIdx     int
	LastIdx      int
}

func debugTraceTopNRange(s context.PlanContext, t *TopN, startIdx, endIdx int) {
	if endIdx <= startIdx {
		return
	}
	root := debugtrace.GetOrInitDebugTraceRoot(s)
	traceInfo := new(topNRangeInfo)
	traceInfo.FirstIdx = startIdx
	traceInfo.LastIdx = endIdx - 1
	if startIdx < len(t.TopN) {
		traceInfo.FirstEncoded = t.TopN[startIdx].Encoded
	}
	if endIdx-1 < len(t.TopN) {
		traceInfo.LastEncoded = t.TopN[endIdx-1].Encoded
	}
	cnts := make([]uint64, endIdx-startIdx)
	for topNIdx, i := startIdx, 0; topNIdx < endIdx; {
		cnts[i] = t.TopN[topNIdx].Count
		topNIdx++
		i++
	}
	traceInfo.Count = cnts
	root.AppendStepWithNameToCurrentContext(traceInfo, "Related TopN Range")
}
