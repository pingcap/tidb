// Copyright 2017 PingCAP, Inc.
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
	"bytes"
	"cmp"
	"fmt"
	"math"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/context"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/twmb/murmur3"
)

// topNThreshold is the minimum ratio of the number of topN elements in CMSketch, 10 means 1 / 10 = 10%.
const topNThreshold = uint64(10)

var (
	// ErrQueryInterrupted indicates interrupted
	ErrQueryInterrupted = dbterror.ClassExecutor.NewStd(mysql.ErrQueryInterrupted)
)

// CMSketch is used to estimate point queries.
// Refer: https://en.wikipedia.org/wiki/Count-min_sketch
type CMSketch struct {
	table        [][]uint32
	count        uint64 // TopN is not counted in count
	defaultValue uint64 // In sampled data, if cmsketch returns a small value (less than avg value / 2), then this will returned.
	depth        int32
	width        int32
}

// NewCMSketch returns a new CM sketch.
func NewCMSketch(d, w int32) *CMSketch {
	tbl := make([][]uint32, d)
	// Background: The Go's memory allocator will ask caller to sweep spans in some scenarios.
	// This can cause memory allocation request latency unpredictable, if the list of spans which need sweep is too long.
	// For memory allocation large than 32K, the allocator will never allocate memory from spans list.
	//
	// The memory referenced by the CMSketch will never be freed.
	// If the number of table or index is extremely large, there will be a large amount of spans in global list.
	// The default value of `d` is 5 and `w` is 2048, if we use a single slice for them the size will be 40K.
	// This allocation will be handled by mheap and will never have impact on normal allocations.
	arena := make([]uint32, d*w)
	for i := range tbl {
		tbl[i] = arena[i*int(w) : (i+1)*int(w)]
	}
	return &CMSketch{depth: d, width: w, table: tbl}
}

// topNHelper wraps some variables used when building cmsketch with top n.
type topNHelper struct {
	sorted        []dataCnt
	sampleSize    uint64
	onlyOnceItems uint64
	sumTopN       uint64
	actualNumTop  uint32
}

func newTopNHelper(sample [][]byte, numTop uint32) *topNHelper {
	counter := make(map[hack.MutableString]uint64, len(sample))
	for i := range sample {
		counter[hack.String(sample[i])]++
	}
	sorted, onlyOnceItems := make([]dataCnt, 0, len(counter)), uint64(0)
	for key, cnt := range counter {
		sorted = append(sorted, dataCnt{hack.Slice(string(key)), cnt})
		if cnt == 1 {
			onlyOnceItems++
		}
	}
	slices.SortStableFunc(sorted, func(i, j dataCnt) int { return -cmp.Compare(i.cnt, j.cnt) })
	failpoint.Inject("StabilizeV1AnalyzeTopN", func(val failpoint.Value) {
		if val.(bool) {
			// The earlier TopN entry will modify the CMSketch, therefore influence later TopN entry's row count.
			// So we need to make the order here fully deterministic to make the stats from analyze ver1 stable.
			// See (*SampleCollector).ExtractTopN(), which calls this function, for details
			sort.SliceStable(sorted, func(i, j int) bool {
				return sorted[i].cnt > sorted[j].cnt ||
					(sorted[i].cnt == sorted[j].cnt && string(sorted[i].data) < string(sorted[j].data))
			})
		}
	})

	var (
		sumTopN   uint64
		sampleNDV = uint32(len(sorted))
	)
	numTop = min(sampleNDV, numTop) // Ensure numTop no larger than sampNDV.
	// Only element whose frequency is not smaller than 2/3 multiples the
	// frequency of the n-th element are added to the TopN statistics. We chose
	// 2/3 as an empirical value because the average cardinality estimation
	// error is relatively small compared with 1/2.
	var actualNumTop uint32
	for ; actualNumTop < sampleNDV && actualNumTop < numTop*2; actualNumTop++ {
		if actualNumTop >= numTop && sorted[actualNumTop].cnt*3 < sorted[numTop-1].cnt*2 {
			break
		}
		if sorted[actualNumTop].cnt == 1 {
			break
		}
		sumTopN += sorted[actualNumTop].cnt
	}

	return &topNHelper{sorted, uint64(len(sample)), onlyOnceItems, sumTopN, actualNumTop}
}

// NewCMSketchAndTopN returns a new CM sketch with TopN elements, the estimate NDV and the scale ratio.
func NewCMSketchAndTopN(d, w int32, sample [][]byte, numTop uint32, rowCount uint64) (*CMSketch, *TopN, uint64, uint64) {
	if rowCount == 0 || len(sample) == 0 {
		return nil, nil, 0, 0
	}
	helper := newTopNHelper(sample, numTop)
	// rowCount is not a accurate value when fast analyzing
	// In some cases, if user triggers fast analyze when rowCount is close to sampleSize, unexpected bahavior might happen.
	rowCount = max(rowCount, uint64(len(sample)))
	estimateNDV, scaleRatio := calculateEstimateNDV(helper, rowCount)
	defaultVal := calculateDefaultVal(helper, estimateNDV, scaleRatio, rowCount)
	c, t := buildCMSAndTopN(helper, d, w, scaleRatio, defaultVal)
	return c, t, estimateNDV, scaleRatio
}

func buildCMSAndTopN(helper *topNHelper, d, w int32, scaleRatio uint64, defaultVal uint64) (c *CMSketch, t *TopN) {
	c = NewCMSketch(d, w)
	enableTopN := helper.sampleSize/topNThreshold <= helper.sumTopN
	if enableTopN {
		t = NewTopN(int(helper.actualNumTop))
		for i := uint32(0); i < helper.actualNumTop; i++ {
			data, cnt := helper.sorted[i].data, helper.sorted[i].cnt
			t.AppendTopN(data, cnt*scaleRatio)
		}
		t.Sort()
		helper.sorted = helper.sorted[helper.actualNumTop:]
	}
	c.defaultValue = defaultVal
	for i := range helper.sorted {
		data, cnt := helper.sorted[i].data, helper.sorted[i].cnt
		// If the value only occurred once in the sample, we assumes that there is no difference with
		// value that does not occurred in the sample.
		rowCount := defaultVal
		if cnt > 1 {
			rowCount = cnt * scaleRatio
		}
		c.InsertBytesByCount(data, rowCount)
	}
	return
}

func calculateDefaultVal(helper *topNHelper, estimateNDV, scaleRatio, rowCount uint64) uint64 {
	sampleNDV := uint64(len(helper.sorted))
	if rowCount <= (helper.sampleSize-helper.onlyOnceItems)*scaleRatio {
		return 1
	}
	estimateRemainingCount := rowCount - (helper.sampleSize-helper.onlyOnceItems)*scaleRatio
	return estimateRemainingCount / max(1, estimateNDV-sampleNDV+helper.onlyOnceItems)
}

// MemoryUsage returns the total memory usage of a CMSketch.
// only calc the hashtable size(CMSketch.table) and the CMSketch.topN
// data are not tracked because size of CMSketch.topN take little influence
// We ignore the size of other metadata in CMSketch.
func (c *CMSketch) MemoryUsage() (sum int64) {
	if c == nil {
		return
	}
	sum = int64(c.depth * c.width * 4)
	return
}

// InsertBytes inserts the bytes value into the CM Sketch.
func (c *CMSketch) InsertBytes(bytes []byte) {
	c.InsertBytesByCount(bytes, 1)
}

// InsertBytesByCount adds the bytes value into the TopN (if value already in TopN) or CM Sketch by delta, this does not updates c.defaultValue.
func (c *CMSketch) InsertBytesByCount(bytes []byte, count uint64) {
	h1, h2 := murmur3.Sum128(bytes)
	c.count += count
	for i := range c.table {
		j := (h1 + h2*uint64(i)) % uint64(c.width)
		c.table[i][j] += uint32(count)
	}
}

func (c *CMSketch) considerDefVal(cnt uint64) bool {
	return (cnt == 0 || (cnt > c.defaultValue && cnt < 2*(c.count/uint64(c.width)))) && c.defaultValue > 0
}

func updateValueBytes(c *CMSketch, t *TopN, d []byte, count uint64) {
	h1, h2 := murmur3.Sum128(d)
	if oriCount, ok := t.QueryTopN(nil, d); ok {
		if count > oriCount {
			t.updateTopNWithDelta(d, count-oriCount, true)
		} else {
			t.updateTopNWithDelta(d, oriCount-count, false)
		}
	}
	c.setValue(h1, h2, count)
}

// setValue sets the count for value that hashed into (h1, h2), and update defaultValue if necessary.
func (c *CMSketch) setValue(h1, h2 uint64, count uint64) {
	oriCount := c.queryHashValue(nil, h1, h2)
	if c.considerDefVal(oriCount) {
		// We should update c.defaultValue if we used c.defaultValue when getting the estimate count.
		// This should make estimation better, remove this line if it does not work as expected.
		c.defaultValue = uint64(float64(c.defaultValue)*0.95 + float64(c.defaultValue)*0.05)
		if c.defaultValue == 0 {
			// c.defaultValue never guess 0 since we are using a sampled data.
			c.defaultValue = 1
		}
	}

	c.count += count - oriCount
	// let it overflow naturally
	deltaCount := uint32(count) - uint32(oriCount)
	for i := range c.table {
		j := (h1 + h2*uint64(i)) % uint64(c.width)
		c.table[i][j] = c.table[i][j] + deltaCount
	}
}

// SubValue remove a value from the CMSketch.
func (c *CMSketch) SubValue(h1, h2 uint64, count uint64) {
	c.count -= count
	for i := range c.table {
		j := (h1 + h2*uint64(i)) % uint64(c.width)
		c.table[i][j] = c.table[i][j] - uint32(count)
	}
}

// QueryValue is used to query the count of specified value.
func QueryValue(sctx context.PlanContext, c *CMSketch, t *TopN, val types.Datum) (uint64, error) {
	var sc *stmtctx.StatementContext
	tz := time.UTC
	if sctx != nil {
		sc = sctx.GetSessionVars().StmtCtx
		tz = sc.TimeZone()
	}
	rawData, err := tablecodec.EncodeValue(tz, nil, val)
	if sc != nil {
		err = sc.HandleError(err)
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	h1, h2 := murmur3.Sum128(rawData)
	if ret, ok := t.QueryTopN(sctx, rawData); ok {
		return ret, nil
	}
	return c.queryHashValue(sctx, h1, h2), nil
}

// QueryBytes is used to query the count of specified bytes.
func (c *CMSketch) QueryBytes(d []byte) uint64 {
	failpoint.Inject("mockQueryBytesMaxUint64", func(val failpoint.Value) {
		failpoint.Return(uint64(val.(int)))
	})
	h1, h2 := murmur3.Sum128(d)
	return c.queryHashValue(nil, h1, h2)
}

// The input sctx is just for debug trace, you can pass nil safely if that's not needed.
func (c *CMSketch) queryHashValue(sctx context.PlanContext, h1, h2 uint64) (result uint64) {
	vals := make([]uint32, c.depth)
	originVals := make([]uint32, c.depth)
	minValue := uint32(math.MaxUint32)
	useDefaultValue := false
	if sctx != nil && sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(sctx)
		defer func() {
			debugtrace.RecordAnyValuesWithNames(sctx,
				"Origin Values", originVals,
				"Values", vals,
				"Use default value", useDefaultValue,
				"Result", result,
			)
			debugtrace.LeaveContextCommon(sctx)
		}()
	}
	// We want that when res is 0 before the noise is eliminated, the default value is not used.
	// So we need a temp value to distinguish before and after eliminating noise.
	temp := uint32(1)
	for i := range c.table {
		j := (h1 + h2*uint64(i)) % uint64(c.width)
		originVals[i] = c.table[i][j]
		if minValue > c.table[i][j] {
			minValue = c.table[i][j]
		}
		noise := (c.count - uint64(c.table[i][j])) / (uint64(c.width) - 1)
		if uint64(c.table[i][j]) == 0 {
			vals[i] = 0
		} else if uint64(c.table[i][j]) < noise {
			vals[i] = temp
		} else {
			vals[i] = c.table[i][j] - uint32(noise) + temp
		}
	}
	slices.Sort(vals)
	res := vals[(c.depth-1)/2] + (vals[c.depth/2]-vals[(c.depth-1)/2])/2
	if res > minValue+temp {
		res = minValue + temp
	}
	if res == 0 {
		return uint64(0)
	}
	res = res - temp
	if c.considerDefVal(uint64(res)) {
		useDefaultValue = true
		return c.defaultValue
	}
	return uint64(res)
}

// MergeTopNAndUpdateCMSketch merges the src TopN into the dst, and spilled values will be inserted into the CMSketch.
func MergeTopNAndUpdateCMSketch(dst, src *TopN, c *CMSketch, numTop uint32) []TopNMeta {
	topNs := []*TopN{src, dst}
	mergedTopN, popedTopNPair := MergeTopN(topNs, numTop)
	if mergedTopN == nil {
		// mergedTopN == nil means the total count of the input TopN are equal to zero
		return popedTopNPair
	}
	dst.TopN = mergedTopN.TopN
	for _, topNMeta := range popedTopNPair {
		c.InsertBytesByCount(topNMeta.Encoded, topNMeta.Count)
	}
	return popedTopNPair
}

// MergeCMSketch merges two CM Sketch.
func (c *CMSketch) MergeCMSketch(rc *CMSketch) error {
	if c == nil || rc == nil {
		return nil
	}
	if c.depth != rc.depth || c.width != rc.width {
		return errors.New("Dimensions of Count-Min Sketch should be the same")
	}
	c.count += rc.count
	for i := range c.table {
		for j := range c.table[i] {
			c.table[i][j] += rc.table[i][j]
		}
	}
	return nil
}

// CMSketchToProto converts CMSketch to its protobuf representation.
func CMSketchToProto(c *CMSketch, topn *TopN) *tipb.CMSketch {
	protoSketch := &tipb.CMSketch{}
	if c != nil {
		protoSketch.Rows = make([]*tipb.CMSketchRow, c.depth)
		for i := range c.table {
			protoSketch.Rows[i] = &tipb.CMSketchRow{Counters: make([]uint32, c.width)}
			copy(protoSketch.Rows[i].Counters, c.table[i])
		}
		protoSketch.DefaultValue = c.defaultValue
	}
	if topn != nil {
		for _, dataMeta := range topn.TopN {
			protoSketch.TopN = append(protoSketch.TopN, &tipb.CMSketchTopN{Data: dataMeta.Encoded, Count: dataMeta.Count})
		}
	}
	return protoSketch
}

// CMSketchAndTopNFromProto converts CMSketch and TopN from its protobuf representation.
func CMSketchAndTopNFromProto(protoSketch *tipb.CMSketch) (*CMSketch, *TopN) {
	if protoSketch == nil {
		return nil, nil
	}
	retTopN := TopNFromProto(protoSketch.TopN)
	if len(protoSketch.Rows) == 0 {
		return nil, retTopN
	}
	c := NewCMSketch(int32(len(protoSketch.Rows)), int32(len(protoSketch.Rows[0].Counters)))
	for i, row := range protoSketch.Rows {
		c.count = 0
		for j, counter := range row.Counters {
			c.table[i][j] = counter
			c.count = c.count + uint64(counter)
		}
	}
	c.defaultValue = protoSketch.DefaultValue
	return c, retTopN
}

// TopNFromProto converts TopN from its protobuf representation.
func TopNFromProto(protoTopN []*tipb.CMSketchTopN) *TopN {
	if len(protoTopN) == 0 {
		return nil
	}
	topN := NewTopN(len(protoTopN))
	for _, e := range protoTopN {
		d := make([]byte, len(e.Data))
		copy(d, e.Data)
		topN.AppendTopN(d, e.Count)
	}
	topN.Sort()
	return topN
}

// EncodeCMSketchWithoutTopN encodes the given CMSketch to byte slice.
// Note that it does not include the topN.
func EncodeCMSketchWithoutTopN(c *CMSketch) ([]byte, error) {
	if c == nil {
		return nil, nil
	}
	p := CMSketchToProto(c, nil)
	p.TopN = nil
	protoData, err := p.Marshal()
	return protoData, err
}

// DecodeCMSketchAndTopN decode a CMSketch from the given byte slice.
func DecodeCMSketchAndTopN(data []byte, topNRows []chunk.Row) (*CMSketch, *TopN, error) {
	if data == nil && len(topNRows) == 0 {
		return nil, nil, nil
	}
	if len(data) == 0 {
		return nil, DecodeTopN(topNRows), nil
	}
	cm, err := DecodeCMSketch(data)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return cm, DecodeTopN(topNRows), nil
}

// DecodeTopN decodes a TopN from the given byte slice.
func DecodeTopN(topNRows []chunk.Row) *TopN {
	if len(topNRows) == 0 {
		return nil
	}
	topN := NewTopN(len(topNRows))
	for _, row := range topNRows {
		data := make([]byte, len(row.GetBytes(0)))
		copy(data, row.GetBytes(0))
		topN.AppendTopN(data, row.GetUint64(1))
	}
	topN.Sort()
	return topN
}

// DecodeCMSketch encodes the given CMSketch to byte slice.
func DecodeCMSketch(data []byte) (*CMSketch, error) {
	if len(data) == 0 {
		return nil, nil
	}
	protoSketch := &tipb.CMSketch{}
	err := protoSketch.Unmarshal(data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(protoSketch.Rows) == 0 {
		return nil, nil
	}
	c := NewCMSketch(int32(len(protoSketch.Rows)), int32(len(protoSketch.Rows[0].Counters)))
	for i, row := range protoSketch.Rows {
		c.count = 0
		for j, counter := range row.Counters {
			c.table[i][j] = counter
			c.count = c.count + uint64(counter)
		}
	}
	c.defaultValue = protoSketch.DefaultValue
	return c, nil
}

// TotalCount returns the total count in the sketch, it is only used for test.
func (c *CMSketch) TotalCount() uint64 {
	if c == nil {
		return 0
	}
	return c.count
}

// Equal tests if two CM Sketch equal, it is only used for test.
func (c *CMSketch) Equal(rc *CMSketch) bool {
	return reflect.DeepEqual(c, rc)
}

// Copy makes a copy for current CMSketch.
func (c *CMSketch) Copy() *CMSketch {
	if c == nil {
		return nil
	}
	tbl := make([][]uint32, c.depth)
	for i := range tbl {
		tbl[i] = make([]uint32, c.width)
		copy(tbl[i], c.table[i])
	}
	return &CMSketch{count: c.count, width: c.width, depth: c.depth, table: tbl, defaultValue: c.defaultValue}
}

// GetWidthAndDepth returns the width and depth of CM Sketch.
func (c *CMSketch) GetWidthAndDepth() (width, depth int32) {
	return c.width, c.depth
}

// CalcDefaultValForAnalyze calculate the default value for Analyze.
// The value of it is count / NDV in CMSketch. This means count and NDV are not include topN.
func (c *CMSketch) CalcDefaultValForAnalyze(ndv uint64) {
	c.defaultValue = c.count / max(1, ndv)
}

// TopN stores most-common values, which is used to estimate point queries.
type TopN struct {
	TopN []TopNMeta
}

// Scale scales the TopN by the given factor.
func (c *TopN) Scale(scaleFactor float64) {
	for i := range c.TopN {
		c.TopN[i].Count = uint64(float64(c.TopN[i].Count) * scaleFactor)
	}
}

// AppendTopN appends a topn into the TopN struct.
func (c *TopN) AppendTopN(data []byte, count uint64) {
	if c == nil {
		return
	}
	c.TopN = append(c.TopN, TopNMeta{data, count})
}

func (c *TopN) String() string {
	if c == nil {
		return "EmptyTopN"
	}
	builder := &strings.Builder{}
	fmt.Fprintf(builder, "TopN{length: %v, ", len(c.TopN))
	fmt.Fprint(builder, "[")
	for i := 0; i < len(c.TopN); i++ {
		fmt.Fprintf(builder, "(%v, %v)", c.TopN[i].Encoded, c.TopN[i].Count)
		if i+1 != len(c.TopN) {
			fmt.Fprint(builder, ", ")
		}
	}
	fmt.Fprint(builder, "]")
	fmt.Fprint(builder, "}")
	return builder.String()
}

// Num returns the ndv of the TopN.
//
//	TopN is declared directly in Histogram. So the Len is occupied by the Histogram. We use Num instead.
func (c *TopN) Num() int {
	if c == nil {
		return 0
	}
	return len(c.TopN)
}

// DecodedString returns the value with decoded result.
func (c *TopN) DecodedString(ctx sessionctx.Context, colTypes []byte) (string, error) {
	if c == nil {
		return "", nil
	}
	builder := &strings.Builder{}
	fmt.Fprintf(builder, "TopN{length: %v, ", len(c.TopN))
	fmt.Fprint(builder, "[")
	var tmpDatum types.Datum
	for i := 0; i < len(c.TopN); i++ {
		tmpDatum.SetBytes(c.TopN[i].Encoded)
		valStr, err := ValueToString(ctx.GetSessionVars(), &tmpDatum, len(colTypes), colTypes)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(builder, "(%v, %v)", valStr, c.TopN[i].Count)
		if i+1 != len(c.TopN) {
			fmt.Fprint(builder, ", ")
		}
	}
	fmt.Fprint(builder, "]")
	fmt.Fprint(builder, "}")
	return builder.String(), nil
}

// Copy makes a copy for current TopN.
func (c *TopN) Copy() *TopN {
	if c == nil {
		return nil
	}
	topN := make([]TopNMeta, len(c.TopN))
	for i, t := range c.TopN {
		topN[i].Encoded = make([]byte, len(t.Encoded))
		copy(topN[i].Encoded, t.Encoded)
		topN[i].Count = t.Count
	}
	return &TopN{
		TopN: topN,
	}
}

// TopNMeta stores the unit of the TopN.
type TopNMeta struct {
	Encoded []byte
	Count   uint64
}

// QueryTopN returns the results for (h1, h2) in murmur3.Sum128(), if not exists, return (0, false).
// The input sctx is just for debug trace, you can pass nil safely if that's not needed.
func (c *TopN) QueryTopN(sctx context.PlanContext, d []byte) (result uint64, found bool) {
	if sctx != nil && sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(sctx)
		defer func() {
			debugtrace.RecordAnyValuesWithNames(sctx, "Result", result, "Found", found)
			debugtrace.LeaveContextCommon(sctx)
		}()
	}
	if c == nil {
		return 0, false
	}
	idx := c.FindTopN(d)
	if sctx != nil && sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.RecordAnyValuesWithNames(sctx, "FindTopN idx", idx)
	}
	if idx < 0 {
		return 0, false
	}
	return c.TopN[idx].Count, true
}

// FindTopN finds the index of the given value in the TopN.
func (c *TopN) FindTopN(d []byte) int {
	if c == nil {
		return -1
	}
	if len(c.TopN) == 0 {
		return -1
	}
	if len(c.TopN) == 1 {
		if bytes.Equal(c.TopN[0].Encoded, d) {
			return 0
		}
		return -1
	}
	if bytes.Compare(c.TopN[len(c.TopN)-1].Encoded, d) < 0 {
		return -1
	}
	if bytes.Compare(c.TopN[0].Encoded, d) > 0 {
		return -1
	}
	idx, match := slices.BinarySearchFunc(c.TopN, d, func(a TopNMeta, b []byte) int {
		return bytes.Compare(a.Encoded, b)
	})
	if !match {
		return -1
	}
	return idx
}

// LowerBound searches on the sorted top-n items,
// returns the smallest index i such that the value at element i is not less than `d`.
func (c *TopN) LowerBound(d []byte) (idx int, match bool) {
	if c == nil {
		return 0, false
	}
	idx, match = slices.BinarySearchFunc(c.TopN, d, func(a TopNMeta, b []byte) int {
		return bytes.Compare(a.Encoded, b)
	})
	return idx, match
}

// BetweenCount estimates the row count for interval [l, r).
// The input sctx is just for debug trace, you can pass nil safely if that's not needed.
func (c *TopN) BetweenCount(sctx context.PlanContext, l, r []byte) (result uint64) {
	if sctx != nil && sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(sctx)
		defer func() {
			debugtrace.RecordAnyValuesWithNames(sctx, "Result", result)
			debugtrace.LeaveContextCommon(sctx)
		}()
	}
	if c == nil {
		return 0
	}
	lIdx, _ := c.LowerBound(l)
	rIdx, _ := c.LowerBound(r)
	ret := uint64(0)
	for i := lIdx; i < rIdx; i++ {
		ret += c.TopN[i].Count
	}
	if sctx != nil && sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugTraceTopNRange(sctx, c, lIdx, rIdx)
	}
	return ret
}

// Sort sorts the topn items.
func (c *TopN) Sort() {
	if c == nil {
		return
	}
	slices.SortFunc(c.TopN, func(i, j TopNMeta) int {
		return bytes.Compare(i.Encoded, j.Encoded)
	})
}

// TotalCount returns how many data is stored in TopN.
func (c *TopN) TotalCount() uint64 {
	if c == nil {
		return 0
	}
	total := uint64(0)
	for _, t := range c.TopN {
		total += t.Count
	}
	return total
}

// Equal checks whether the two TopN are equal.
func (c *TopN) Equal(cc *TopN) bool {
	if c.TotalCount() == 0 && cc.TotalCount() == 0 {
		return true
	} else if c.TotalCount() != cc.TotalCount() {
		return false
	}
	if len(c.TopN) != len(cc.TopN) {
		return false
	}
	for i := range c.TopN {
		if !bytes.Equal(c.TopN[i].Encoded, cc.TopN[i].Encoded) {
			return false
		}
		if c.TopN[i].Count != cc.TopN[i].Count {
			return false
		}
	}
	return true
}

// RemoveVal remove the val from TopN if it exists.
func (c *TopN) RemoveVal(val []byte) {
	if c == nil {
		return
	}
	pos := c.FindTopN(val)
	if pos == -1 {
		return
	}
	c.TopN = append(c.TopN[:pos], c.TopN[pos+1:]...)
}

// MemoryUsage returns the total memory usage of a topn.
func (c *TopN) MemoryUsage() (sum int64) {
	if c == nil {
		return
	}
	sum = 32 // size of array (24) + reference (8)
	for _, meta := range c.TopN {
		sum += 32 + int64(cap(meta.Encoded)) // 32 is size of byte array (24) + size of uint64 (8)
	}
	return
}

// queryAddTopN TopN adds count to CMSketch.topN if exists, and returns the count of such elements after insert.
// If such elements does not in topn elements, nothing will happen and false will be returned.
func (c *TopN) updateTopNWithDelta(d []byte, delta uint64, increase bool) bool {
	if c == nil || c.TopN == nil {
		return false
	}
	idx := c.FindTopN(d)
	if idx >= 0 {
		if increase {
			c.TopN[idx].Count += delta
		} else {
			c.TopN[idx].Count -= delta
		}
		return true
	}
	return false
}

// NewTopN creates the new TopN struct by the given size.
func NewTopN(n int) *TopN {
	return &TopN{TopN: make([]TopNMeta, 0, n)}
}

// MergeTopN is used to merge more TopN structures to generate a new TopN struct by the given size.
// The input parameters are multiple TopN structures to be merged and the size of the new TopN that will be generated.
// The output parameters are the newly generated TopN structure and the remaining numbers.
// Notice: The n can be 0. So n has no default value, we must explicitly specify this value.
func MergeTopN(topNs []*TopN, n uint32) (*TopN, []TopNMeta) {
	if CheckEmptyTopNs(topNs) {
		return nil, nil
	}
	// Different TopN structures may hold the same value, we have to merge them.
	counter := make(map[hack.MutableString]uint64)
	for _, topN := range topNs {
		if topN.TotalCount() == 0 {
			continue
		}
		for _, val := range topN.TopN {
			counter[hack.String(val.Encoded)] += val.Count
		}
	}
	numTop := len(counter)
	if numTop == 0 {
		return nil, nil
	}
	sorted := make([]TopNMeta, 0, numTop)
	for value, cnt := range counter {
		data := hack.Slice(string(value))
		sorted = append(sorted, TopNMeta{Encoded: data, Count: cnt})
	}
	return GetMergedTopNFromSortedSlice(sorted, n)
}

// CheckEmptyTopNs checks whether all TopNs are empty.
func CheckEmptyTopNs(topNs []*TopN) bool {
	for _, topN := range topNs {
		if topN.TotalCount() != 0 {
			return false
		}
	}
	return true
}

// SortTopnMeta sort topnMeta
func SortTopnMeta(topnMetas []TopNMeta) {
	slices.SortFunc(topnMetas, func(i, j TopNMeta) int {
		if i.Count != j.Count {
			return cmp.Compare(j.Count, i.Count)
		}
		return bytes.Compare(i.Encoded, j.Encoded)
	})
}

// TopnMetaCompare compare topnMeta
func TopnMetaCompare(i, j TopNMeta) int {
	c := cmp.Compare(j.Count, i.Count)
	if c != 0 {
		return c
	}
	return bytes.Compare(i.Encoded, j.Encoded)
}

// GetMergedTopNFromSortedSlice returns merged topn
func GetMergedTopNFromSortedSlice(sorted []TopNMeta, n uint32) (*TopN, []TopNMeta) {
	SortTopnMeta(sorted)
	n = min(uint32(len(sorted)), n)

	var finalTopN TopN
	finalTopN.TopN = sorted[:n]
	finalTopN.Sort()
	return &finalTopN, sorted[n:]
}
