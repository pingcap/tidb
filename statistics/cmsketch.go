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
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"bytes"
	"math"
	"reflect"
	"sort"

	"github.com/cznic/mathutil"
	"github.com/cznic/sortutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hack"
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

// TopNMeta is a simple counter used by BuildTopN.
type TopNMeta struct {
	h2    uint64 // h2 is the second part of `murmur3.Sum128()`, it is always used with the first part `h1`.
	Data  []byte
	Count uint64
}

// GetH2 get the the second part of `murmur3.Sum128()`, just for test.
func (t *TopNMeta) GetH2() uint64 {
	return t.h2
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
	numTop = mathutil.MinUint32(sampleNDV, numTop) // Ensure numTop no larger than sampNDV.
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
	rowCount = mathutil.MaxUint64(rowCount, uint64(len(sample)))
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
			h1, h2 := murmur3.Sum128(data)
			t.topN[h1] = append(t.topN[h1], &TopNMeta{h2, data, cnt * scaleRatio})
		}
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
		c.insertBytesByCount(data, rowCount)
	}
	return
}

func calculateDefaultVal(helper *topNHelper, estimateNDV, scaleRatio, rowCount uint64) uint64 {
	sampleNDV := uint64(len(helper.sorted))
	if rowCount <= (helper.sampleSize-helper.onlyOnceItems)*scaleRatio {
		return 1
	}
	estimateRemainingCount := rowCount - (helper.sampleSize-helper.onlyOnceItems)*scaleRatio
	return estimateRemainingCount / mathutil.MaxUint64(1, estimateNDV-sampleNDV+helper.onlyOnceItems)
}

func (c *TopN) findTopNMeta(h1, h2 uint64, d []byte) *TopNMeta {
	for _, meta := range c.topN[h1] {
		if meta.h2 == h2 && bytes.Equal(d, meta.Data) {
			return meta
		}
	}
	return nil
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
func (c *TopN) updateTopNWithDelta(h1, h2 uint64, d []byte, delta uint64) bool {
	if c == nil || c.topN == nil {
		return false
	}
	meta := c.findTopNMeta(h1, h2, d)
	if meta != nil {
		meta.Count += delta
		return true
	}
	return false
}

// QueryTopN returns the results for (h1, h2) in murmur3.Sum128(), if not exists, return (0, false).
func (c *TopN) QueryTopN(h1, h2 uint64, d []byte) (uint64, bool) {
	if c == nil {
		return 0, false
	}
	meta := c.findTopNMeta(h1, h2, d)
	if meta != nil {
		return meta.Count, true
	}
	return 0, false
}

// InsertBytes inserts the bytes value into the CM Sketch.
func (c *CMSketch) InsertBytes(bytes []byte) {
	c.insertBytesByCount(bytes, 1)
}

// InsertBytesByCount adds the bytes value into the TopN (if value already in TopN) or CM Sketch by delta, this does not updates c.defaultValue.
func (c *CMSketch) insertBytesByCount(bytes []byte, count uint64) {
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
	if oriCount, ok := t.QueryTopN(h1, h2, d); ok {
		deltaCount := count - oriCount
		t.updateTopNWithDelta(h1, h2, d, deltaCount)
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

func (c *CMSketch) subValue(h1, h2 uint64, count uint64) {
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
	if ret, ok := t.QueryTopN(h1, h2, bytes); ok {
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

// MergeTopN merges the src TopN into the dst, and spilled values will be inserted into the CMSketch.
func MergeTopN(dst, src *TopN, c *CMSketch, numTop uint32, usingMax bool) {
	if dst.TotalCount() == 0 || src.TotalCount() == 0 {
		return
	}
	counter := make(map[hack.MutableString]uint64)
	for _, metas := range dst.topN {
		for _, meta := range metas {
			counter[hack.String(meta.Data)] += meta.Count
		}
	}
	for _, metas := range src.topN {
		for _, meta := range metas {
			if usingMax {
				counter[hack.String(meta.Data)] = mathutil.MaxUint64(counter[hack.String(meta.Data)], meta.Count)
			} else {
				counter[hack.String(meta.Data)] += meta.Count
			}
		}
	}
	sorted := make([]uint64, len(counter))
	for _, cnt := range counter {
		sorted = append(sorted, cnt)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] > sorted[j]
	})
	numTop = mathutil.MinUint32(uint32(len(counter)), numTop)
	lastTopCnt := sorted[numTop-1]
	dst.topN = make(map[uint64][]*TopNMeta)
	for value, cnt := range counter {
		data := hack.Slice(string(value))
		if cnt >= lastTopCnt {
			h1, h2 := murmur3.Sum128(data)
			dst.topN[h1] = append(dst.topN[h1], &TopNMeta{h2, data, cnt})
		} else {
			c.insertBytesByCount(data, cnt)
		}
	}
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
			c.table[i][j] = mathutil.MaxUint32(c.table[i][j], rc.table[i][j])
			c.count += uint64(c.table[i][j])
		}
	}
	return nil
}

// CMSketchToProto converts CMSketch to its protobuf representation.
func CMSketchToProto(c *CMSketch) *tipb.CMSketch {
	protoSketch := &tipb.CMSketch{Rows: make([]*tipb.CMSketchRow, c.depth)}
	for i := range c.table {
		protoSketch.Rows[i] = &tipb.CMSketchRow{Counters: make([]uint32, c.width)}
		for j := range c.table[i] {
			protoSketch.Rows[i].Counters[j] = c.table[i][j]
		}
	}
	protoSketch.DefaultValue = c.defaultValue
	return protoSketch
}

// CMSketchAndTopNFromProto converts CMSketch and TopN from its protobuf representation.
func CMSketchAndTopNFromProto(protoSketch *tipb.CMSketch) (*CMSketch, *TopN) {
	if protoSketch == nil || len(protoSketch.Rows) == 0 {
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
	if len(protoSketch.TopN) == 0 {
		return c, nil
	}
	return c, TopNFromProto(protoSketch.TopN)
}

// TopNFromProto converts TopN from its protobuf representation.
func TopNFromProto(protoTopN []*tipb.CMSketchTopN) *TopN {
	topN := NewTopN(32)
	for _, e := range protoTopN {
		h1, h2 := murmur3.Sum128(e.Data)
		topN.topN[h1] = append(topN.topN[h1], &TopNMeta{h2, e.Data, e.Count})
	}
	return topN
}

// EncodeCMSketchWithoutTopN encodes the given CMSketch to byte slice.
// Note that it does not include the topN.
func EncodeCMSketchWithoutTopN(c *CMSketch) ([]byte, error) {
	if c == nil {
		return nil, nil
	}
	p := CMSketchToProto(c)
	p.TopN = nil
	protoData, err := p.Marshal()
	return protoData, err
}

// DecodeCMSketchAndTopN decode a CMSketch from the given byte slice.
func DecodeCMSketchAndTopN(data []byte, topNRows []chunk.Row) (*CMSketch, *TopN, error) {
	if data == nil {
		return nil, nil, nil
	}
	p := &tipb.CMSketch{}
	err := p.Unmarshal(data)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	for _, row := range topNRows {
		data := make([]byte, len(row.GetBytes(0)))
		copy(data, row.GetBytes(0))
		p.TopN = append(p.TopN, &tipb.CMSketchTopN{Data: data, Count: row.GetUint64(1)})
	}
	cm, topN := CMSketchAndTopNFromProto(p)
	return cm, topN, nil
}

// TotalCount returns the total count in the sketch, it is only used for test.
func (c *CMSketch) TotalCount() uint64 {
	return c.count
}

// TotalCount returns the total count in the TopN.
func (c *TopN) TotalCount() uint64 {
	if c == nil {
		return 0
	}
	var cnt uint64
	for _, metas := range c.topN {
		for _, meta := range metas {
			cnt += meta.Count
		}
	}
	return cnt
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

// Copy makes a copy for current TopN.
func (c *TopN) Copy() *TopN {
	if c == nil {
		return nil
	}
	topN := make(map[uint64][]*TopNMeta, len(c.topN))
	for h1, vals := range c.topN {
		newVals := make([]*TopNMeta, 0, len(vals))
		for _, val := range vals {
			newVal := TopNMeta{h2: val.h2, Count: val.Count, Data: make([]byte, len(val.Data))}
			copy(newVal.Data, val.Data)
			newVals = append(newVals, &newVal)
		}
		topN[h1] = newVals
	}
	return &TopN{topN}
}

// TopN gets all the topN meta.
func (c *TopN) TopN() []*TopNMeta {
	if c == nil {
		return nil
	}
	topN := make([]*TopNMeta, 0, len(c.topN))
	for _, meta := range c.topN {
		topN = append(topN, meta...)
	}
	return topN
}

// TopNMap gets the origin topN map.
func (c *TopN) TopNMap() map[uint64][]*TopNMeta {
	return c.topN
}

// AppendTopN appends a topn into the cm sketch.
func (c *TopN) AppendTopN(data []byte, count uint64) {
	if c.topN == nil {
		c.topN = make(map[uint64][]*TopNMeta)
	}
	h1, h2 := murmur3.Sum128(data)
	c.topN[h1] = append(c.topN[h1], &TopNMeta{h2, data, count})
}

// GetWidthAndDepth returns the width and depth of CM Sketch.
func (c *CMSketch) GetWidthAndDepth() (int32, int32) {
	return c.width, c.depth
}

// CalcDefaultValForAnalyze calculate the default value for Analyze.
// The value of it is count / NDV in CMSketch. This means count and NDV are not include topN.
func (c *CMSketch) CalcDefaultValForAnalyze(NDV uint64) {
	c.defaultValue = c.count / mathutil.MaxUint64(1, NDV)
}

// TopN stores most-common values, which is used to estimate point queries.
type TopN struct {
	topN map[uint64][]*TopNMeta
}

// NewTopN returns a new TopN with n slots.
func NewTopN(n int) *TopN {
	return &TopN{topN: make(map[uint64][]*TopNMeta, n)}
}
