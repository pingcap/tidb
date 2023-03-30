package statistics

import (
	"encoding/json"
	"github.com/pingcap/tidb/planner/util/debug_trace"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/ranger"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

/*
 Below is debug trace for statistics.Table and types related to fields in statistics.Table.
*/

type StatsTblInfo struct {
	PhysicalId  int64
	Version     uint64
	Count       int64
	ModifyCount int64
	Columns     []*statsTblColOrIdxInfo
	Indexes     []*statsTblColOrIdxInfo
}

type statsTblColOrIdxInfo struct {
	ID                int64
	Name              string
	NDV               int64
	NullCount         int64
	LastUpdateVersion uint64
	TotColSize        int64
	Correlation       float64
	StatsVer          int64
	LoadingStatus     string

	HistogramSize int
	TopNSize      int
	CMSketchInfo  *cmSketchInfo
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

func TraceStatsTbl(statsTbl *Table) *StatsTblInfo {
	if statsTbl == nil {
		return nil
	}
	// Collect table level information
	colNum := len(statsTbl.Columns)
	idxNum := len(statsTbl.Indices)
	traceInfo := &StatsTblInfo{
		PhysicalId:  statsTbl.PhysicalID,
		Version:     statsTbl.Version,
		Count:       statsTbl.Count,
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
 Debug trace for GetRowCountByXXX().
*/

type getRowCountInput struct {
	ID     int64
	Ranges []string
}

func debugTraceGetRowCountInput(
	s sessionctx.Context,
	id int64,
	ranges ranger.Ranges,
) {
	root := debug_trace.GetOrInitDebugTraceRoot(s)
	newCtx := &getRowCountInput{
		ID:     id,
		Ranges: make([]string, len(ranges)),
	}
	for i, r := range ranges {
		newCtx.Ranges[i] = r.String()
	}
	root.AppendStepToCurrentContext(newCtx)
}

type startEstimateRangeInfo struct {
	CurrentRowCount  float64
	Range            string
	LowValueEncoded  []byte
	HighValueEncoded []byte
}

func debugTraceStartEstimateRange(
	s sessionctx.Context,
	r *ranger.Range,
	lowBytes, highBytes []byte,
	currentCount float64,
) {
	root := debug_trace.GetOrInitDebugTraceRoot(s)
	traceInfo := &startEstimateRangeInfo{
		CurrentRowCount:  currentCount,
		Range:            r.String(),
		LowValueEncoded:  lowBytes,
		HighValueEncoded: highBytes,
	}
	root.AppendStepWithNameToCurrentContext(traceInfo, "Start estimate range")
}

type debugTraceAddRowCountType int8

const (
	DebugTraceUnknownTypeAddRowCount debugTraceAddRowCountType = iota
	DebugTraceImpossible
	DebugTraceUniquePoint
	DebugTracePoint
	DebugTraceRange
	DebugTraceVer1SmallRange
)

var addRowCountTypeToString = map[debugTraceAddRowCountType]string{
	DebugTraceUnknownTypeAddRowCount: "Unknown",
	DebugTraceImpossible:             "Impossible",
	DebugTraceUniquePoint:            "Unique point",
	DebugTracePoint:                  "Point",
	DebugTraceRange:                  "Range",
	DebugTraceVer1SmallRange:         "Small range in ver1 stats",
}

func (d debugTraceAddRowCountType) MarshalJSON() ([]byte, error) {
	return json.Marshal(addRowCountTypeToString[d])
}

type endEstimateRangeInfo struct {
	RowCount float64
	Type     debugTraceAddRowCountType
}

func debugTraceEndEstimateRange(
	s sessionctx.Context,
	count float64,
	AddType debugTraceAddRowCountType,
) {
	root := debug_trace.GetOrInitDebugTraceRoot(s)
	traceInfo := &endEstimateRangeInfo{
		RowCount: count,
		Type:     AddType,
	}
	root.AppendStepWithNameToCurrentContext(traceInfo, "End estimate range")
}

type locateBucketInfo struct {
	Value          string
	Exceed         bool
	BucketIdx      int
	InBucket       bool
	MatchLastValue bool
}

func debugTraceLocateBucket(
	s sessionctx.Context,
	value *types.Datum,
	exceed bool,
	bucketIdx int,
	inBucket bool,
	matchLastValue bool,
) {
	root := debug_trace.GetOrInitDebugTraceRoot(s)
	traceInfo := &locateBucketInfo{
		Value:          value.String(),
		Exceed:         exceed,
		BucketIdx:      bucketIdx,
		InBucket:       inBucket,
		MatchLastValue: matchLastValue,
	}
	root.AppendStepWithNameToCurrentContext(traceInfo, "Locate value in buckets")
}

type bucketInfo struct {
	Index  int
	Count  int64
	Repeat int64
}

func debugTraceBuckets(s sessionctx.Context, hg *Histogram, bucketIdxs []int) {
	root := debug_trace.GetOrInitDebugTraceRoot(s)
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

type topNInfo struct {
	Idx     int
	Encoded []byte
	Count   uint64
}

func debugTraceTopNRange(s sessionctx.Context, t *TopN, startIdx, endIdx int) {
	root := debug_trace.GetOrInitDebugTraceRoot(s)
	topNRange := make([]topNInfo, endIdx-startIdx+1)
	for topNIdx, i := startIdx, 0; topNIdx < endIdx; {
		topNRange[i].Idx = topNIdx
		topNRange[i].Encoded = t.TopN[topNIdx].Encoded
		topNRange[i].Count = t.TopN[topNIdx].Count
		topNIdx++
		i++
	}
	root.AppendStepWithNameToCurrentContext(topNRange, "Related TopN Range")
}
