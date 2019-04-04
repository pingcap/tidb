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

// CMSketch is used to estimate point queries.
// Refer: https://en.wikipedia.org/wiki/Count-min_sketch
// If topnlimit is 0, topn will be readonly
// Once constructed with topn, no new values should be inserted, or statistic data will be wrong.
type CMSketch struct {
	depth     int32
	width     int32
	count     uint64
	table     [][]uint32
	topn      map[cmshash]uint32
	topnlimit int32
}

type cmshash struct {
	h1 uint64
	h2 uint64
}

type cmscount struct {
	cmshash
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

// NewCMSketchWithTopN returns a new CM sketch with initialized topn map.
// Warning: This will count all samples, Only use with small samples
func NewCMSketchWithTopN(d, w, topn int32) *CMSketch {
	tbl := make([][]uint32, d)
	for i := range tbl {
		tbl[i] = make([]uint32, w)
	}
	return &CMSketch{depth: d, width: w, table: tbl, topn: make(map[cmshash]uint32), topnlimit: topn}
}

// InsertBytes inserts the bytes value into the CM Sketch.
func (c *CMSketch) InsertBytes(bytes []byte) {
	c.count++
	h1, h2 := murmur3.Sum128(bytes)
	if c.topnlimit > 0 {
		c.topn[cmshash{h1, h2}]++
		return
	}
	for i := range c.table {
		j := (h1 + h2*uint64(i)) % uint64(c.width)
		c.table[i][j]++
	}
}

// setValue sets the count for value that hashed into (h1, h2).
func (c *CMSketch) setValue(h1, h2 uint64, count uint32) {
	oriCount := c.queryHashValue(h1, h2)
	c.count += uint64(count) - uint64(oriCount)
	// let it overflow naturally
	deltaCount := count - oriCount
	if c.topnlimit > 0 {
		c.topn[cmshash{h1, h2}] += deltaCount
		return
	}
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
func (c *CMSketch) QueryBytes(bytes []byte) uint32 {
	h1, h2 := murmur3.Sum128(bytes)
	return c.queryHashValue(h1, h2)
}

func (c *CMSketch) queryHashValue(h1, h2 uint64) uint32 {
	vals := make([]uint32, c.depth)
	if c.topnlimit >= 0 {
		if v, ok := c.topn[cmshash{h1, h2}]; ok {
			return v
		}
	}
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
// Merge two CM Sketch with TopN will downgrand to normal CMSketch
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
	if c.topn != nil {
		for k, v := range c.topn {
			for i := range c.table {
				j := (k.h1 + k.h2*uint64(i)) % uint64(c.width)
				c.table[i][j] += v
			}
		}
		c.topn = nil
		c.topnlimit = 0
	}
	if rc.topn != nil {
		for k, v := range rc.topn {
			for i := range c.table {
				j := (k.h1 + k.h2*uint64(i)) % uint64(c.width)
				c.table[i][j] += v
			}
		}
		c.topn = nil
		c.topnlimit = 0
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
	if len(c.topn) > 0 {
		protoSketch.Topn = make([]*tipb.CMSketchTopN, 0, c.topnlimit)
		tcnt := make([]cmscount, len(c.topn))
		maxTopN := int32(len(c.topn))
		if c.topnlimit > 0 {
			maxTopN = c.topnlimit
		}

		cnt := 0

		// Marshalled CMS always includes topk
		for k, v := range c.topn {
			tcnt[cnt] = cmscount{k, v}
			for i := range c.table {
				j := (k.h1 + k.h2*uint64(i)) % uint64(c.width)
				protoSketch.Rows[i].Counters[j] = protoSketch.Rows[i].Counters[j] + v
			}
			cnt++
		}

		sort.Slice(tcnt, func(i, j int) bool {
			return tcnt[i].count > tcnt[j].count
		})

		for k := 0; k < len(tcnt) && int32(k) < maxTopN; k++ {
			protoSketch.Topn = append(protoSketch.Topn, &tipb.CMSketchTopN{
				H1:    tcnt[k].h1,
				H2:    tcnt[k].h2,
				Count: tcnt[k].count,
			})
		}
	}
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
	c.topn = make(map[cmshash]uint32)
	for _, v := range protoSketch.Topn {
		c.topn[cmshash{v.H1, v.H2}] = v.Count
		for i := range c.table {
			j := (v.H1 + v.H2*uint64(i)) % uint64(c.width)
			c.table[i][j] = c.table[i][j] - v.Count
		}
	}
	return c
}

func encodeCMSketch(c *CMSketch) ([]byte, error) {
	if c == nil || c.count == 0 {
		return nil, nil
	}
	p := CMSketchToProto(c)
	return p.Marshal()
}

func decodeCMSketch(data []byte) (*CMSketch, error) {
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
	if c.width != rc.width || c.depth != rc.depth || c.count != rc.count || c.topnlimit != rc.topnlimit {
		return false
	}
	for i := range c.table {
		for j := range c.table[i] {
			if c.table[i][j] != rc.table[i][j] {
				return false
			}
		}
	}
	if len(rc.topn) > 0 || len(c.topn) > 0 {
		if len(rc.topn) != len(c.topn) {
			return false
		}
		for rk, rv := range rc.topn {
			if v, ok := c.topn[rk]; !ok || v != rv {
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
	var nmap map[cmshash]uint32
	if c.topnlimit > 0 {
		nmap = make(map[cmshash]uint32)
		for k, v := range c.topn {
			nmap[k] = v
		}
	}
	return &CMSketch{count: c.count, width: c.width, depth: c.depth, table: tbl, topnlimit: c.topnlimit, topn: nmap}
}
