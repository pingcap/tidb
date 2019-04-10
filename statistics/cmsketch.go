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
	"github.com/pingcap/tipb/go-tipb"
	"github.com/spaolacci/murmur3"
)

// topNThreshold is the minimum ratio of the number of topn elements in CMSketch, 20 means 1 / 20 = 5%.
const topNThreshold = 20

// CMSketch is used to estimate point queries.
// Refer: https://en.wikipedia.org/wiki/Count-min_sketch
// If topnlimit is 0, topn will be readonly
// Once constructed with topn, no new values should be inserted, or statistic data will be wrong.
type CMSketch struct {
	depth     int32
	width     int32
	count     uint64
	table     [][]uint32
	topnindex map[uint64][]cmscount
}

type cmscount struct {
	h1    uint64
	h2    uint64
	data  []byte
	count uint64
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
	c.BuildTopN(data, n, topNThreshold)
	return c
}

// BuildTopN builds table of top N elements.
// elements in data should not be modified after this call.
// See https://pdfs.semanticscholar.org/5ae2/6358989c6595798da8f8fd6c2a517536bcae.pdf
func (c *CMSketch) BuildTopN(data [][]byte, n uint32, topnThreshold uint32) {
	topn := make([]cmscount, n)
	for k := range data {
		found, pos := false, -1
		for i := range topn {
			if bytes.Equal(topn[i].data, data[k]) {
				found = true
				pos = i
				break
			} else if topn[i].count == 0 {
				pos = i
			}
		}
		if found {
			topn[pos].count++
		} else if !found && pos != -1 {
			topn[pos] = cmscount{data: data[k], count: 1}
		} else {
			for i := range topn {
				topn[i].count--
			}
		}
	}

	for i := range topn {
		topn[i].count = 0
	}

	topncount := uint64(0)

	for k := range data {
		found := false
		for i := range topn {
			if bytes.Equal(topn[i].data, data[k]) {
				found = true
				topn[i].count++
				c.count++
				topncount++
				break
			}
		}
		if !found {
			c.InsertBytes(data[k])
		}
	}

	if uint64(n/topnThreshold) > topncount {
		for i := range topn {
			c.InsertBytesN(topn[i].data, topn[i].count)
		}
	} else {
		for i := range topn {
			if topn[i].data == nil {
				continue
			}
			topn[i].h1, topn[i].h2 = murmur3.Sum128(topn[i].data)
		}
		c.buildTopNMap(topn)
	}
}

func (c *CMSketch) buildTopNMap(topn []cmscount) {
	c.topnindex = make(map[uint64][]cmscount)
	for i := range topn {
		if topn[i].data == nil {
			continue
		}
		h1, h2 := murmur3.Sum128(topn[i].data)
		sl, ok := c.topnindex[h1]
		if !ok {
			sl = make([]cmscount, 0)
		}
		sl = append(sl, cmscount{h1, h2, topn[i].data, topn[i].count})
		c.topnindex[h1] = sl
	}
}

// flatten merges top n data into cmsketch and disables top n
func (c *CMSketch) flatten() {
	if c.topnindex == nil {
		return
	}
	for _, v1 := range c.topnindex {
		for _, v := range v1 {
			for i := range c.table {
				j := (v.h1 + v.h2*uint64(i)) % uint64(c.width)
				c.table[i][j] += uint32(v.count)
			}
		}
	}
	c.topnindex = nil
}

// InsertBytes inserts the bytes value into the CM Sketch.
func (c *CMSketch) InsertBytes(bytes []byte) {
	c.InsertBytesN(bytes, 1)
}

// InsertBytesN adds the bytes value into the CM Sketch by n.
func (c *CMSketch) InsertBytesN(bytes []byte, n uint64) {
	c.flatten()
	c.count += n
	h1, h2 := murmur3.Sum128(bytes)
	for i := range c.table {
		j := (h1 + h2*uint64(i)) % uint64(c.width)
		c.table[i][j] += uint32(n)
	}
}

// setValue sets the count for value that hashed into (h1, h2).
func (c *CMSketch) setValue(h1, h2 uint64, count uint32) {
	c.flatten()
	oriCount := c.queryHashValue(h1, h2)
	c.count += uint64(count) - uint64(oriCount)
	// let it overflow naturally
	deltaCount := count - oriCount
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
	if c.topnindex != nil {
		cnt, ok := c.topnindex[h1]
		if ok {
			for k := range cnt {
				if cnt[k].h2 == h2 && bytes.Equal(d, cnt[k].data) {
					return cnt[k].count
				}
			}
		}
	}
	return uint64(c.queryHashValue(h1, h2))
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
	c.flatten()
	if c.depth != rc.depth || c.width != rc.width {
		return errors.New("Dimensions of Count-Min Sketch should be the same")
	}
	c.count += rc.count
	for i := range c.table {
		for j := range c.table[i] {
			c.table[i][j] += rc.table[i][j]
		}
	}
	if rc.topnindex {
		for _, v1 := range rc.topnindex {
			for _, v := range v1 {
				for i := range c.table {
					j := (v.h1 + v.h2*uint64(i)) % uint64(c.width)
					c.table[i][j] += uint32(v.count)
				}
			}
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
	if c.topnindex != nil {
		protoSketch.TopN = make([]*tipb.CMSketchTopN, len(c.topnindex))
		seq := uint32(0)
		for _, v1 := range c.topnindex {
			for _, v := range v1 {
				h1, h2 := murmur3.Sum128(v.data)
				for i := range c.table {
					j := (h1 + h2*uint64(i)) % uint64(c.width)
					protoSketch.Rows[i].Counters[j] += uint32(v.count)
				}
				protoSketch.TopN[seq] = &tipb.CMSketchTopN{Data: v.data, Count: v.count}
				seq++
			}
		}
	}
	return protoSketch
}

// CMSketchFromProto converts CMSketch from its protobuf representation.
// Even if topn is broken, we can still get downgraded CMSketch.
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
	if protoSketch.TopN != nil {
		// Check if we fill the Data field
		for p := range protoSketch.TopN {
			if protoSketch.TopN[p].Data == nil {
				// Downgrade
				return c
			}
		}
		c.topnindex = make(map[uint64][]cmscount)
		topn := make([]cmscount, len(protoSketch.TopN))
		for p, v := range protoSketch.TopN {
			h1, h2 := murmur3.Sum128(v.Data)
			for i := range c.table {
				j := (h1 + h2*uint64(i)) % uint64(c.width)
				c.table[i][j] -= uint32(v.Count)
			}
			topn[p] = cmscount{h1, h2, v.Data, v.Count}
		}
		c.buildTopNMap(topn)
	}
	return c
}

func encodeCMSketch(c *CMSketch) ([]byte, [][]byte, error) {
	if c == nil || c.count == 0 {
		return nil, nil, nil
	}
	p := CMSketchToProto(c)
	var topn [][]byte
	if p.TopN != nil {
		topn = make([][]byte, len(p.TopN))
		for i := range p.TopN {
			topn[i] = p.TopN[i].Data
			// Do not store actual Top N data into protobuf.
			p.TopN[i].Data = nil
		}
	}
	r, err := p.Marshal()
	return r, topn, err
}

func decodeCMSketch(data []byte, topn [][]byte) (*CMSketch, error) {
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
	if len(topn) == len(p.TopN) {
		for i := range topn {
			p.TopN[i].Data = topn[i]
		}
	}
	return CMSketchFromProto(p), nil
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
			v2, ok := rc.topnindex[k]
			if !ok || len(v) != len(v2) {
				return false
			}
			for i := range v {
				hasMatch := false
				for j := range v2 {
					if v[i].h2 == v2[j].h2 && bytes.Equal(v[i].data, v2[j].data) && v[i].count == v2[j].count {
						hasMatch = true
						break
					}
				}
				if !hasMatch {
					return false
				}
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
	var ntopn map[uint64][]cmscount
	if c.topnindex != nil {
		ntopn = make(map[uint64][]cmscount)
		for k, v := range c.topnindex {
			newSlice := make([]cmscount, len(v))
			for i := range v {
				newSlice[i] = cmscount{v[i].h1, v[i].h2, make([]byte, len(v[i].data)), v[i].count}
				copy(newSlice[i].data, v[i].data)
			}
			ntopn[k] = newSlice
		}
	}
	return &CMSketch{count: c.count, width: c.width, depth: c.depth, table: tbl, topnindex: ntopn}
}
