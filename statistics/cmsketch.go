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
	"fmt"
	"math"
	"sort"

	"github.com/cznic/mathutil"
	"github.com/cznic/sortutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/spaolacci/murmur3"
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
	topN         map[uint64][]*TopNMeta
}

// TopNMeta is a simple counter used by BuildTopN.
type TopNMeta struct {
	h2    uint64 // h2 is the second part of `murmur3.Sum128()`, it is always used with the first part `h1`.
	Data  []byte
	Count uint64
}

// NewCMSketch returns a new CM sketch.
func NewCMSketch(d, w int32) *CMSketch {
	tbl := make([][]uint32, d)
	for i := range tbl {
		tbl[i] = make([]uint32, w)
	}
	return &CMSketch{depth: d, width: w, table: tbl}
}

// topNHelper wraps some variables used when building cmsketch with top n.
type topNHelper struct {
	sampleSize    uint64
	counter       map[hack.MutableString]uint64
	sorted        []uint64
	onlyOnceItems uint64
	sumTopN       uint64
	lastVal       uint64
}

func newTopNHelper(sample [][]byte, numTop uint32) *topNHelper {
	counter := make(map[hack.MutableString]uint64)
	for i := range sample {
		counter[hack.String(sample[i])]++
	}
	sorted, onlyOnceItems := make([]uint64, 0, len(counter)), uint64(0)
	for _, cnt := range counter {
		sorted = append(sorted, cnt)
		if cnt == 1 {
			onlyOnceItems++
		}
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] > sorted[j]
	})

	var (
		// last is the last element in top N index should occurres atleast `last` times.
		last      uint64
		sumTopN   uint64
		sampleNDV = uint32(len(sorted))
	)
	numTop = mathutil.MinUint32(sampleNDV, numTop) // Ensure numTop no larger than sampNDV.
	// Only element whose frequency is not smaller than 2/3 multiples the
	// frequency of the n-th element are added to the TopN statistics. We chose
	// 2/3 as an empirical value because the average cardinality estimation
	// error is relatively small compared with 1/2.
	for i := uint32(0); i < sampleNDV && i < numTop*2; i++ {
		if i >= numTop && sorted[i]*3 < sorted[numTop-1]*2 && last != sorted[i] {
			break
		}
		last = sorted[i]
		sumTopN += sorted[i]
	}

	return &topNHelper{uint64(len(sample)), counter, sorted, onlyOnceItems, sumTopN, last}
}

// NewCMSketchWithTopN returns a new CM sketch with TopN elements, the estimate NDV and the scale ratio.
func NewCMSketchWithTopN(d, w int32, sample [][]byte, numTop uint32, rowCount uint64) (*CMSketch, uint64, uint64) {
	helper := newTopNHelper(sample, numTop)
	// rowCount is not a accurate value when fast analyzing
	// In some cases, if user triggers fast analyze when rowCount is close to sampleSize, unexpected bahavior might happen.
	rowCount = mathutil.MaxUint64(rowCount, uint64(len(sample)))
	estimateNDV, scaleRatio := calculateEstimateNDV(helper, rowCount)
	c := buildCMSWithTopN(helper, d, w, scaleRatio)
	c.calculateDefaultVal(helper, estimateNDV, scaleRatio, rowCount)
	return c, estimateNDV, scaleRatio
}

func buildCMSWithTopN(helper *topNHelper, d, w int32, scaleRatio uint64) (c *CMSketch) {
	c = NewCMSketch(d, w)
	enableTopN := helper.sampleSize/topNThreshold <= helper.sumTopN
	if enableTopN {
		c.topN = make(map[uint64][]*TopNMeta)
	}
	for counterKey, cnt := range helper.counter {
		data, scaledCount := hack.Slice(string(counterKey)), cnt*scaleRatio
		if enableTopN && cnt >= helper.lastVal {
			h1, h2 := murmur3.Sum128(data)
			c.topN[h1] = append(c.topN[h1], &TopNMeta{h2, data, scaledCount})
		} else {
			c.insertBytesByCount(data, scaledCount)
		}
	}
	return
}

func (c *CMSketch) calculateDefaultVal(helper *topNHelper, estimateNDV, scaleRatio, rowCount uint64) {
	sampleNDV := uint64(len(helper.sorted))
	if rowCount <= (helper.sampleSize-uint64(helper.onlyOnceItems))*scaleRatio {
		c.defaultValue = 1
	} else {
		estimateRemainingCount := rowCount - (helper.sampleSize-uint64(helper.onlyOnceItems))*scaleRatio
		c.defaultValue = estimateRemainingCount / (estimateNDV - uint64(sampleNDV) + helper.onlyOnceItems)
	}
}

func (c *CMSketch) findTopNMeta(h1, h2 uint64, d []byte) *TopNMeta {
	for _, meta := range c.topN[h1] {
		if meta.h2 == h2 && bytes.Equal(d, meta.Data) {
			return meta
		}
	}
	return nil
}

// queryAddTopN TopN adds count to CMSketch.topN if exists, and returns the count of such elements after insert.
// If such elements does not in topn elements, nothing will happen and false will be returned.
func (c *CMSketch) updateTopNWithDelta(h1, h2 uint64, d []byte, delta uint64) bool {
	if c.topN == nil {
		return false
	}
	meta := c.findTopNMeta(h1, h2, d)
	if meta != nil {
		meta.Count += delta
		return true
	}
	return false
}

func (c *CMSketch) queryTopN(h1, h2 uint64, d []byte) (uint64, bool) {
	if c.topN == nil {
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

// insertBytesByCount adds the bytes value into the TopN (if value already in TopN) or CM Sketch by delta, this does not updates c.defaultValue.
func (c *CMSketch) insertBytesByCount(bytes []byte, count uint64) {
	h1, h2 := murmur3.Sum128(bytes)
	if c.updateTopNWithDelta(h1, h2, bytes, count) {
		return
	}
	c.count += count
	for i := range c.table {
		j := (h1 + h2*uint64(i)) % uint64(c.width)
		c.table[i][j] += uint32(count)
	}
}

func (c *CMSketch) considerDefVal(cnt uint64) bool {
	return (cnt == 0 || (cnt > c.defaultValue && cnt < 2*(c.count/uint64(c.width)))) && c.defaultValue > 0
}

// updateValueBytes updates value of d to count.
func (c *CMSketch) updateValueBytes(d []byte, count uint64) {
	h1, h2 := murmur3.Sum128(d)
	if oriCount, ok := c.queryTopN(h1, h2, d); ok {
		deltaCount := count - oriCount
		c.updateTopNWithDelta(h1, h2, d, deltaCount)
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

func (c *CMSketch) queryValue(sc *stmtctx.StatementContext, val types.Datum) (uint64, error) {
	bytes, err := codec.EncodeValue(sc, nil, val)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return c.QueryBytes(bytes), nil
}

// QueryBytes is used to query the count of specified bytes.
func (c *CMSketch) QueryBytes(d []byte) uint64 {
	h1, h2 := murmur3.Sum128(d)
	if count, ok := c.queryTopN(h1, h2, d); ok {
		return count
	}
	return c.queryHashValue(h1, h2)
}

func (c *CMSketch) queryHashValue(h1, h2 uint64) uint64 {
	vals := make([]uint32, c.depth)
	min := uint32(math.MaxUint32)
	for i := range c.table {
		j := (h1 + h2*uint64(i)) % uint64(c.width)
		if min > c.table[i][j] {
			min = c.table[i][j]
		}
		noise := (c.count - uint64(c.table[i][j])) / (uint64(c.width) - 1)
		if uint64(c.table[i][j]) < noise {
			vals[i] = 0
		} else {
			vals[i] = c.table[i][j] - uint32(noise)
		}
	}
	sort.Sort(sortutil.Uint32Slice(vals))
	res := vals[(c.depth-1)/2] + (vals[c.depth/2]-vals[(c.depth-1)/2])/2
	if res > min {
		res = min
	}
	if c.considerDefVal(uint64(res)) {
		return c.defaultValue
	}
	return uint64(res)
}

// MergeCMSketch merges two CM Sketch.
// Call with CMSketch with Top-N initialized may downgrade the result
func (c *CMSketch) MergeCMSketch(rc *CMSketch) error {
	if c.depth != rc.depth || c.width != rc.width {
		return errors.New("Dimensions of Count-Min Sketch should be the same")
	}
	if c.topN != nil || rc.topN != nil {
		return errors.New("CMSketch with Top-N does not support merge")
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
func CMSketchToProto(c *CMSketch) *tipb.CMSketch {
	protoSketch := &tipb.CMSketch{Rows: make([]*tipb.CMSketchRow, c.depth)}
	for i := range c.table {
		protoSketch.Rows[i] = &tipb.CMSketchRow{Counters: make([]uint32, c.width)}
		for j := range c.table[i] {
			protoSketch.Rows[i].Counters[j] = c.table[i][j]
		}
	}
	for _, dataSlice := range c.topN {
		for _, dataMeta := range dataSlice {
			protoSketch.TopN = append(protoSketch.TopN, &tipb.CMSketchTopN{Data: dataMeta.Data, Count: dataMeta.Count})
		}
	}
	protoSketch.DefaultValue = c.defaultValue
	return protoSketch
}

// CMSketchFromProto converts CMSketch from its protobuf representation.
func CMSketchFromProto(protoSketch *tipb.CMSketch) *CMSketch {
	if protoSketch == nil {
		return nil
	}
	c := NewCMSketch(int32(len(protoSketch.Rows)), int32(len(protoSketch.Rows[0].Counters)))
	for i, row := range protoSketch.Rows {
		c.count = 0
		for j, counter := range row.Counters {
			c.table[i][j] = counter
			c.count = c.count + uint64(counter)
		}
	}
	if len(protoSketch.TopN) == 0 {
		return c
	}
	c.defaultValue = protoSketch.DefaultValue
	c.topN = make(map[uint64][]*TopNMeta)
	for _, e := range protoSketch.TopN {
		h1, h2 := murmur3.Sum128(e.Data)
		c.topN[h1] = append(c.topN[h1], &TopNMeta{h2, e.Data, e.Count})
	}
	return c
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

// decodeCMSketch decode a CMSketch from the given byte slice.
func decodeCMSketch(data []byte, topN []*TopNMeta) (*CMSketch, error) {
	if data == nil {
		return nil, nil
	}
	p := &tipb.CMSketch{}
	err := p.Unmarshal(data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(p.Rows) == 0 && len(topN) == 0 {
		return nil, nil
	}
	for _, meta := range topN {
		p.TopN = append(p.TopN, &tipb.CMSketchTopN{Data: meta.Data, Count: meta.Count})
	}
	return CMSketchFromProto(p), nil
}

// LoadCMSketchWithTopN loads the CM sketch with topN from storage.
func LoadCMSketchWithTopN(exec sqlexec.RestrictedSQLExecutor, tableID, isIndex, histID int64, cms []byte) (*CMSketch, error) {
	sql := fmt.Sprintf("select HIGH_PRIORITY value, count from mysql.stats_top_n where table_id = %d and is_index = %d and hist_id = %d", tableID, isIndex, histID)
	topNRows, _, err := exec.ExecRestrictedSQL(nil, sql)
	if err != nil {
		return nil, err
	}
	topN := make([]*TopNMeta, 0, len(topNRows))
	for _, row := range topNRows {
		topN = append(topN, &TopNMeta{Data: row.GetBytes(0), Count: row.GetUint64(1)})
	}
	return decodeCMSketch(cms, topN)
}

// TotalCount returns the count, it is only used for test.
func (c *CMSketch) TotalCount() uint64 {
	return c.count
}

// Equal tests if two CM Sketch equal, it is only used for test.
func (c *CMSketch) Equal(rc *CMSketch) bool {
	if c == nil || rc == nil {
		return c == nil && rc == nil
	}
	if c.width != rc.width || c.depth != rc.depth || c.count != rc.count || c.defaultValue != rc.defaultValue {
		return false
	}
	for i := range c.table {
		for j := range c.table[i] {
			if c.table[i][j] != rc.table[i][j] {
				return false
			}
		}
	}
	if len(c.topN) != len(rc.topN) {
		return false
	}
	for h1, topNData := range c.topN {
		if len(topNData) != len(rc.topN[h1]) {
			return false
		}
		for _, val := range topNData {
			meta := rc.findTopNMeta(h1, val.h2, val.Data)
			if meta == nil || meta.Count != val.Count {
				return false
			}
		}
	}
	return true
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
	var topN map[uint64][]*TopNMeta
	if c.topN != nil {
		topN = make(map[uint64][]*TopNMeta)
		for h1, vals := range c.topN {
			newVals := make([]*TopNMeta, 0, len(vals))
			for _, val := range vals {
				newVal := TopNMeta{h2: val.h2, Count: val.Count, Data: make([]byte, len(val.Data))}
				copy(newVal.Data, val.Data)
				newVals = append(newVals, &newVal)
			}
			topN[h1] = newVals
		}
	}
	return &CMSketch{count: c.count, width: c.width, depth: c.depth, table: tbl, defaultValue: c.defaultValue, topN: topN}
}

// TopN gets all the topN meta.
func (c *CMSketch) TopN() []*TopNMeta {
	if c == nil {
		return nil
	}
	topN := make([]*TopNMeta, 0, len(c.topN))
	for _, meta := range c.topN {
		topN = append(topN, meta...)
	}
	return topN
}

// GetWidthAndDepth returns the width and depth of CM Sketch.
func (c *CMSketch) GetWidthAndDepth() (int32, int32) {
	return c.width, c.depth
}
