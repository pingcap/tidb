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
	"sort"

	"github.com/cznic/sortutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/spaolacci/murmur3"
)

// TopNThreshold is the minimum ratio of the number of topn elements in CMSketch, 20 means 1 / 20 = 5%.
const TopNThreshold = 20

// CMSketch is used to estimate point queries.
// Refer: https://en.wikipedia.org/wiki/Count-min_sketch
// If topnlimit is 0, topn will be readonly
// Once constructed with topn, no new values should be inserted, or statistic data will be wrong.
type CMSketch struct {
	depth     int32
	width     int32
	count     uint64
	table     [][]uint32
	topn      []cmscount
	topnindex map[hack.MutableString]uint32
}

type cmscount struct {
	data  []byte
	count uint32
}

// NewCMSketch returns a new CM sketch.
func NewCMSketch(d, w int32) *CMSketch {
	tbl := make([][]uint32, d)
	for i := range tbl {
		tbl[i] = make([]uint32, w)
	}
	return &CMSketch{depth: d, width: w, table: tbl}
}

// NewCMSketchWithTopN returns a new CM sketch with TopN elements.
// Only when the number of topn elements exceeded len(data) / topnThreshold will store topn elements
func NewCMSketchWithTopN(d, w int32, data [][]byte, n uint32) *CMSketch {
	tbl := make([][]uint32, d)
	for i := range tbl {
		tbl[i] = make([]uint32, w)
	}
	c := &CMSketch{depth: d, width: w, table: tbl}
	c.BuildTopN(data, n, TopNThreshold)
	return c
}

// BuildTopN builds table of top N elements.
// elements in data should not be modified after this call.
// See https://pdfs.semanticscholar.org/5ae2/6358989c6595798da8f8fd6c2a517536bcae.pdf
func (c *CMSketch) BuildTopN(data [][]byte, n uint32, topnThreshold uint32) {
	c.topn = make([]cmscount, n)
	for k := range data {
		found, pos := false, -1
		for i := range c.topn {
			if bytes.Equal(c.topn[i].data, data[k]) {
				found = true
				pos = i
				break
			} else if c.topn[i].count == 0 {
				pos = i
			}
		}
		if found {
			c.topn[pos].count++
		} else if !found && pos != -1 {
			c.topn[pos] = cmscount{data[k], 1}
		} else {
			for i := range c.topn {
				c.topn[i].count--
			}
		}
	}

	for i := range c.topn {
		c.topn[i].count = 0
	}

	topncount := uint32(0)

	for k := range data {
		found := false
		for i := range c.topn {
			if bytes.Equal(c.topn[i].data, data[k]) {
				found = true
				c.topn[i].count++
				c.count++
				topncount++
				break
			}
		}
		if !found {
			c.InsertBytes(data[k])
		}
	}

	if n/topnThreshold > topncount {
		for i := range c.topn {
			c.InsertBytesN(c.topn[i].data, c.topn[i].count)
		}
	} else {
		c.topnindex = make(map[hack.MutableString]uint32)
		for i := range c.topn {
			c.topnindex[hack.String(c.topn[i].data)] = c.topn[i].count
		}
	}

	c.topn = nil
}

// InsertBytes inserts the bytes value into the CM Sketch.
func (c *CMSketch) InsertBytes(bytes []byte) {
	c.count++
	h1, h2 := murmur3.Sum128(bytes)
	for i := range c.table {
		j := (h1 + h2*uint64(i)) % uint64(c.width)
		c.table[i][j]++
	}
}

// InsertBytesN adds the bytes value into the CM Sketch by n.
func (c *CMSketch) InsertBytesN(bytes []byte, n uint32) {
	c.count += uint64(n)
	h1, h2 := murmur3.Sum128(bytes)
	for i := range c.table {
		j := (h1 + h2*uint64(i)) % uint64(c.width)
		c.table[i][j] += n
	}
}

// setValue sets the count for value that hashed into (h1, h2).
func (c *CMSketch) setValue(h1, h2 uint64, count uint32) {
	oriCount := c.queryHashValue(h1, h2)
	c.count += uint64(count) - uint64(oriCount)
	// let it overflow naturally
	deltaCount := count - oriCount
	for i := range c.table {
		j := (h1 + h2*uint64(i)) % uint64(c.width)
		c.table[i][j] = c.table[i][j] + deltaCount
	}
}

func (c *CMSketch) queryValue(sc *stmtctx.StatementContext, val types.Datum) (uint32, error) {
	bytes, err := codec.EncodeValue(sc, nil, val)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return c.QueryBytes(bytes), nil
}

// QueryBytes is used to query the count of specified bytes.
func (c *CMSketch) QueryBytes(d []byte) uint32 {
	if c.topnindex != nil {
		if cnt, ok := c.topnindex[hack.String(d)]; ok {
			return cnt
		}
	}
	h1, h2 := murmur3.Sum128(d)
	return c.queryHashValue(h1, h2)
}

func (c *CMSketch) queryHashValue(h1, h2 uint64) uint32 {
	// [TODO/cms-topn]: get estimate number of elements not presented in sample.
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
		return min
	}
	return res
}

// MergeCMSketch merges two CM Sketch.
// Do not call with CMSketch with top N initialized
func (c *CMSketch) MergeCMSketch(rc *CMSketch) error {
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
func CMSketchToProto(c *CMSketch) (*tipb.CMSketch, []hack.MutableString) {
	protoSketch := &tipb.CMSketch{Rows: make([]*tipb.CMSketchRow, c.depth)}
	for i := range c.table {
		protoSketch.Rows[i] = &tipb.CMSketchRow{Counters: make([]uint32, c.width)}
		for j := range c.table[i] {
			protoSketch.Rows[i].Counters[j] = c.table[i][j]
		}
	}
	var topn []hack.MutableString
	if c.topnindex != nil {
		topn = make([]hack.MutableString, len(c.topnindex))
		protoSketch.TopN = make([]uint32, len(c.topnindex))
		seq := uint32(0)
		for k, v := range c.topnindex {
			h1, h2 := murmur3.Sum128(hack.Slice(string(k)))
			for i := range c.table {
				j := (h1 + h2*uint64(i)) % uint64(c.width)
				protoSketch.Rows[i].Counters[j] += v
			}
			protoSketch.TopN[seq] = v
			topn[seq] = k
			seq++
		}
	}
	return protoSketch, topn
}

// CMSketchFromProto converts CMSketch from its protobuf representation.
// Even if topn is broken, we can still get downgraded CMSketch.
func CMSketchFromProto(protoSketch *tipb.CMSketch, topn []hack.MutableString) *CMSketch {
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
	if protoSketch.TopN != nil && len(topn) == len(protoSketch.TopN) {
		c.topnindex = make(map[hack.MutableString]uint32)
		for p, v := range protoSketch.TopN {
			h1, h2 := murmur3.Sum128(hack.Slice(string(topn[p])))
			for i := range c.table {
				j := (h1 + h2*uint64(i)) % uint64(c.width)
				c.table[i][j] -= v
			}
			c.topnindex[topn[p]] = v
		}
	}
	return c
}

func encodeCMSketch(c *CMSketch) ([]byte, []hack.MutableString, error) {
	if c == nil || c.count == 0 {
		return nil, nil, nil
	}
	p, topn := CMSketchToProto(c)
	r, err := p.Marshal()
	return r, topn, err
}

func decodeCMSketch(data []byte, topn []hack.MutableString) (*CMSketch, error) {
	if data == nil {
		return nil, nil
	}
	p := &tipb.CMSketch{}
	err := p.Unmarshal(data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(p.Rows) == 0 {
		return nil, nil
	}
	return CMSketchFromProto(p, topn), nil
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
	if c.width != rc.width || c.depth != rc.depth || c.count != rc.count {
		return false
	}
	for i := range c.table {
		for j := range c.table[i] {
			if c.table[i][j] != rc.table[i][j] {
				return false
			}
		}
	}
	if len(rc.topnindex) > 0 || len(c.topnindex) > 0 {
		if len(rc.topnindex) != len(c.topnindex) {
			return false
		}
		for k, v := range c.topnindex {
			if v2, ok := rc.topnindex[k]; !ok || v != v2 {
				return false
			}
		}
	}
	return true
}

func (c *CMSketch) copy() *CMSketch {
	if c == nil {
		return nil
	}
	tbl := make([][]uint32, c.depth)
	for i := range tbl {
		tbl[i] = make([]uint32, c.width)
		copy(tbl[i], c.table[i])
	}
	var ntopn map[hack.MutableString]uint32
	if c.topnindex != nil {
		ntopn = make(map[hack.MutableString]uint32)
		for k, v := range c.topnindex {
			ntopn[k] = v
		}
	}
	return &CMSketch{count: c.count, width: c.width, depth: c.depth, table: tbl, topnindex: ntopn}
}
