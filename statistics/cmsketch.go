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
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"

	"github.com/pingcap/tidb/sessionctx"

	"github.com/cznic/sortutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/twmb/murmur3"
)

// topNThreshold is the minimum ratio of the number of topn elements in CMSketch, 10 means 1 / 10 = 10%.
const topNThreshold = uint64(10)

// CMSketch is used to estimate point queries.
// Refer: https://en.wikipedia.org/wiki/Count-min_sketch
type CMSketch struct {
	depth        int32
	width        int32
	count        uint64 // TopN is not counted in count
	defaultValue uint64 // In sampled data, if cmsketch returns a small value (less than avg value / 2), then this will returned.
	table        [][]uint32
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
	sampleSize    uint64
	sorted        []dataCnt
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
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].cnt > sorted[j].cnt })

	var (
		sumTopN   uint64
		sampleNDV = uint32(len(sorted))
	)
	numTop = mathutil.Min(sampleNDV, numTop) // Ensure numTop no larger than sampNDV.
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

	return &topNHelper{uint64(len(sample)), sorted, onlyOnceItems, sumTopN, actualNumTop}
}

// NewCMSketchAndTopN returns a new CM sketch with TopN elements, the estimate NDV and the scale ratio.
func NewCMSketchAndTopN(d, w int32, sample [][]byte, numTop uint32, rowCount uint64) (*CMSketch, *TopN, uint64, uint64) {
	if rowCount == 0 || len(sample) == 0 {
		return nil, nil, 0, 0
	}
	helper := newTopNHelper(sample, numTop)
	// rowCount is not a accurate value when fast analyzing
	// In some cases, if user triggers fast analyze when rowCount is close to sampleSize, unexpected bahavior might happen.
	rowCount = mathutil.Max(rowCount, uint64(len(sample)))
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
	return estimateRemainingCount / mathutil.Max(1, estimateNDV-sampleNDV+helper.onlyOnceItems)
}

// MemoryUsage returns the total memory usage of a CMSketch.
// only calc the hashtable size(CMSketch.table) and the CMSketch.topN
// data are not tracked because size of CMSketch.topN take little influence
// We ignore the size of other metadata in CMSketch.
func (c *CMSketch) MemoryUsage() (sum int64) {
	sum = int64(c.depth * c.width * 4)
	return
}

// queryAddTopN TopN adds count to CMSketch.topN if exists, and returns the count of such elements after insert.
// If such elements does not in topn elements, nothing will happen and false will be returned.
func (c *TopN) updateTopNWithDelta(d []byte, delta uint64, increase bool) bool {
	if c == nil || c.TopN == nil {
		return false
	}
	idx := c.findTopN(d)
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
	if oriCount, ok := t.QueryTopN(d); ok {
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
	oriCount := c.queryHashValue(h1, h2)
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

func queryValue(sc *stmtctx.StatementContext, c *CMSketch, t *TopN, val types.Datum) (uint64, error) {
	bytes, err := tablecodec.EncodeValue(sc, nil, val)
	if err != nil {
		return 0, errors.Trace(err)
	}
	h1, h2 := murmur3.Sum128(bytes)
	if ret, ok := t.QueryTopN(bytes); ok {
		return ret, nil
	}
	return c.queryHashValue(h1, h2), nil
}

// QueryBytes is used to query the count of specified bytes.
func (c *CMSketch) QueryBytes(d []byte) uint64 {
	failpoint.Inject("mockQueryBytesMaxUint64", func(val failpoint.Value) {
		failpoint.Return(uint64(val.(int)))
	})
	h1, h2 := murmur3.Sum128(d)
	return c.queryHashValue(h1, h2)
}

func (c *CMSketch) queryHashValue(h1, h2 uint64) uint64 {
	vals := make([]uint32, c.depth)
	min := uint32(math.MaxUint32)
	// We want that when res is 0 before the noise is eliminated, the default value is not used.
	// So we need a temp value to distinguish before and after eliminating noise.
	temp := uint32(1)
	for i := range c.table {
		j := (h1 + h2*uint64(i)) % uint64(c.width)
		if min > c.table[i][j] {
			min = c.table[i][j]
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
	sort.Sort(sortutil.Uint32Slice(vals))
	res := vals[(c.depth-1)/2] + (vals[c.depth/2]-vals[(c.depth-1)/2])/2
	if res > min+temp {
		res = min + temp
	}
	if res == 0 {
		return uint64(0)
	}
	res = res - temp
	if c.considerDefVal(uint64(res)) {
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

// MergeCMSketch4IncrementalAnalyze merges two CM Sketch for incremental analyze. Since there is no value
// that appears partially in `c` and `rc` for incremental analyze, it uses `max` to merge them.
// Here is a simple proof: when we query from the CM sketch, we use the `min` to get the answer:
//   (1): For values that only appears in `c, using `max` to merge them affects the `min` query result less than using `sum`;
//   (2): For values that only appears in `rc`, it is the same as condition (1);
//   (3): For values that appears both in `c` and `rc`, if they do not appear partially in `c` and `rc`, for example,
//        if `v` appears 5 times in the table, it can appears 5 times in `c` and 3 times in `rc`, then `max` also gives the correct answer.
// So in fact, if we can know the number of appearances of each value in the first place, it is better to use `max` to construct the CM sketch rather than `sum`.
func (c *CMSketch) MergeCMSketch4IncrementalAnalyze(rc *CMSketch, numTopN uint32) error {
	if c.depth != rc.depth || c.width != rc.width {
		return errors.New("Dimensions of Count-Min Sketch should be the same")
	}
	for i := range c.table {
		c.count = 0
		for j := range c.table[i] {
			c.table[i][j] = mathutil.Max(c.table[i][j], rc.table[i][j])
			c.count += uint64(c.table[i][j])
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
	topN := NewTopN(32)
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
	pbTopN := make([]*tipb.CMSketchTopN, 0, len(topNRows))
	for _, row := range topNRows {
		data := make([]byte, len(row.GetBytes(0)))
		copy(data, row.GetBytes(0))
		pbTopN = append(pbTopN, &tipb.CMSketchTopN{
			Data:  data,
			Count: row.GetUint64(1),
		})
	}
	if len(data) == 0 {
		return nil, TopNFromProto(pbTopN), nil
	}
	p := &tipb.CMSketch{}
	err := p.Unmarshal(data)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	p.TopN = pbTopN
	cm, topN := CMSketchAndTopNFromProto(p)
	return cm, topN, nil
}

// TotalCount returns the total count in the sketch, it is only used for test.
func (c *CMSketch) TotalCount() uint64 {
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

// AppendTopN appends a topn into the TopN struct.
func (c *TopN) AppendTopN(data []byte, count uint64) {
	c.TopN = append(c.TopN, TopNMeta{data, count})
}

// GetWidthAndDepth returns the width and depth of CM Sketch.
func (c *CMSketch) GetWidthAndDepth() (int32, int32) {
	return c.width, c.depth
}

// CalcDefaultValForAnalyze calculate the default value for Analyze.
// The value of it is count / NDV in CMSketch. This means count and NDV are not include topN.
func (c *CMSketch) CalcDefaultValForAnalyze(NDV uint64) {
	c.defaultValue = c.count / mathutil.Max(1, NDV)
}

// TopN stores most-common values, which is used to estimate point queries.
type TopN struct {
	TopN []TopNMeta
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
//   TopN is declared directly in Histogram. So the Len is occupied by the Histogram. We use Num instead.
func (c *TopN) Num() int {
	if c == nil {
		return 0
	}
	return len(c.TopN)
}

// DecodedString returns the value with decoded result.
func (c *TopN) DecodedString(ctx sessionctx.Context, colTypes []byte) (string, error) {
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
func (c *TopN) QueryTopN(d []byte) (uint64, bool) {
	if c == nil {
		return 0, false
	}
	idx := c.findTopN(d)
	if idx < 0 {
		return 0, false
	}
	return c.TopN[idx].Count, true
}

func (c *TopN) findTopN(d []byte) int {
	if c == nil {
		return -1
	}
	match := false
	idx := sort.Search(len(c.TopN), func(i int) bool {
		cmp := bytes.Compare(c.TopN[i].Encoded, d)
		if cmp == 0 {
			match = true
		}
		return cmp >= 0
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
	idx = sort.Search(len(c.TopN), func(i int) bool {
		cmp := bytes.Compare(c.TopN[i].Encoded, d)
		if cmp == 0 {
			match = true
		}
		return cmp >= 0
	})
	return idx, match
}

// BetweenCount estimates the row count for interval [l, r).
func (c *TopN) BetweenCount(l, r []byte) uint64 {
	if c == nil {
		return 0
	}
	lIdx, _ := c.LowerBound(l)
	rIdx, _ := c.LowerBound(r)
	ret := uint64(0)
	for i := lIdx; i < rIdx; i++ {
		ret += c.TopN[i].Count
	}
	return ret
}

// Sort sorts the topn items.
func (c *TopN) Sort() {
	if c == nil {
		return
	}
	sort.Slice(c.TopN, func(i, j int) bool {
		return bytes.Compare(c.TopN[i].Encoded, c.TopN[j].Encoded) < 0
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
	pos := c.findTopN(val)
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

// NewTopN creates the new TopN struct by the given size.
func NewTopN(n int) *TopN {
	return &TopN{TopN: make([]TopNMeta, 0, n)}
}

// MergePartTopN2GlobalTopN is used to merge the partition-level topN to global-level topN.
// The input parameters:
//     1. `topNs` are the partition-level topNs to be merged.
//     2. `n` is the size of the global-level topN. Notice: This value can be 0 and has no default value, we must explicitly specify this value.
//     3. `hists` are the partition-level histograms. Some values not in topN may be placed in the histogram. We need it here to make the value in the global-level TopN more accurate.
// The output parameters:
//     1. `*TopN` is the final global-level topN.
//     2. `[]TopNMeta` is the left topN value from the partition-level TopNs, but is not placed to global-level TopN. We should put them back to histogram latter.
//     3. `[]*Histogram` are the partition-level histograms which just delete some values when we merge the global-level topN.
func MergePartTopN2GlobalTopN(sc *stmtctx.StatementContext, version int, topNs []*TopN, n uint32, hists []*Histogram, isIndex bool) (*TopN, []TopNMeta, []*Histogram, error) {
	if checkEmptyTopNs(topNs) {
		return nil, nil, hists, nil
	}

	partNum := len(topNs)
	topNsNum := make([]int, partNum)
	removeVals := make([][]TopNMeta, partNum)
	for i, topN := range topNs {
		if topN == nil {
			topNsNum[i] = 0
			continue
		}
		topNsNum[i] = len(topN.TopN)
	}
	// Different TopN structures may hold the same value, we have to merge them.
	counter := make(map[hack.MutableString]float64)
	// datumMap is used to store the mapping from the string type to datum type.
	// The datum is used to find the value in the histogram.
	datumMap := make(map[hack.MutableString]types.Datum)
	for i, topN := range topNs {
		if topN.TotalCount() == 0 {
			continue
		}
		for _, val := range topN.TopN {
			encodedVal := hack.String(val.Encoded)
			_, exists := counter[encodedVal]
			counter[encodedVal] += float64(val.Count)
			if exists {
				// We have already calculated the encodedVal from the histogram, so just continue to next topN value.
				continue
			}
			// We need to check whether the value corresponding to encodedVal is contained in other partition-level stats.
			// 1. Check the topN first.
			// 2. If the topN doesn't contain the value corresponding to encodedVal. We should check the histogram.
			for j := 0; j < partNum; j++ {
				if (j == i && version >= 2) || topNs[j].findTopN(val.Encoded) != -1 {
					continue
				}
				// Get the encodedVal from the hists[j]
				datum, exists := datumMap[encodedVal]
				if !exists {
					// If the datumMap does not have the encodedVal datum,
					// we should generate the datum based on the encoded value.
					// This part is copied from the function MergePartitionHist2GlobalHist.
					var d types.Datum
					if isIndex {
						d.SetBytes(val.Encoded)
					} else {
						var err error
						if types.IsTypeTime(hists[0].Tp.GetType()) {
							// handle datetime values specially since they are encoded to int and we'll get int values if using DecodeOne.
							_, d, err = codec.DecodeAsDateTime(val.Encoded, hists[0].Tp.GetType(), sc.TimeZone)
						} else {
							_, d, err = codec.DecodeOne(val.Encoded)
						}
						if err != nil {
							return nil, nil, nil, err
						}
					}
					datumMap[encodedVal] = d
					datum = d
				}
				// Get the row count which the value is equal to the encodedVal from histogram.
				count, _ := hists[j].equalRowCount(datum, isIndex)
				if count != 0 {
					counter[encodedVal] += count
					// Remove the value corresponding to encodedVal from the histogram.
					removeVals[j] = append(removeVals[j], TopNMeta{Encoded: datum.GetBytes(), Count: uint64(count)})
				}
			}
		}
	}
	// Remove the value from the Hists.
	for i := 0; i < partNum; i++ {
		if len(removeVals[i]) > 0 {
			tmp := removeVals[i]
			sort.Slice(tmp, func(i, j int) bool {
				cmpResult := bytes.Compare(tmp[i].Encoded, tmp[j].Encoded)
				return cmpResult < 0
			})
			hists[i].RemoveVals(tmp)
		}
	}
	numTop := len(counter)
	if numTop == 0 {
		return nil, nil, hists, nil
	}
	sorted := make([]TopNMeta, 0, numTop)
	for value, cnt := range counter {
		data := hack.Slice(string(value))
		sorted = append(sorted, TopNMeta{Encoded: data, Count: uint64(cnt)})
	}
	globalTopN, leftTopN := getMergedTopNFromSortedSlice(sorted, n)
	return globalTopN, leftTopN, hists, nil
}

// MergeTopN is used to merge more TopN structures to generate a new TopN struct by the given size.
// The input parameters are multiple TopN structures to be merged and the size of the new TopN that will be generated.
// The output parameters are the newly generated TopN structure and the remaining numbers.
// Notice: The n can be 0. So n has no default value, we must explicitly specify this value.
func MergeTopN(topNs []*TopN, n uint32) (*TopN, []TopNMeta) {
	if checkEmptyTopNs(topNs) {
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
	return getMergedTopNFromSortedSlice(sorted, n)
}

func checkEmptyTopNs(topNs []*TopN) bool {
	count := uint64(0)
	for _, topN := range topNs {
		count += topN.TotalCount()
	}
	return count == 0
}

func getMergedTopNFromSortedSlice(sorted []TopNMeta, n uint32) (*TopN, []TopNMeta) {
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Count != sorted[j].Count {
			return sorted[i].Count > sorted[j].Count
		}
		return bytes.Compare(sorted[i].Encoded, sorted[j].Encoded) < 0
	})
	n = mathutil.Min(uint32(len(sorted)), n)

	var finalTopN TopN
	finalTopN.TopN = sorted[:n]
	finalTopN.Sort()
	return &finalTopN, sorted[n:]
}
