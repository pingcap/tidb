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
	"math/rand"
	"strconv"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/spaolacci/murmur3"
)

func (c *CMSketch) insert(val *types.Datum) error {
	bytes, err := codec.EncodeValue(nil, nil, *val)
	if err != nil {
		return errors.Trace(err)
	}
	c.InsertBytes(bytes)
	return nil
}

func buildCMSketchAndMap(d, w int32, seed int64, total, imax uint64, s float64) (*CMSketch, map[int64]uint32, error) {
	cms := NewCMSketch(d, w)
	mp := make(map[int64]uint32)
	zipf := rand.NewZipf(rand.New(rand.NewSource(seed)), s, 1, imax)
	for i := uint64(0); i < total; i++ {
		val := types.NewIntDatum(int64(zipf.Uint64()))
		err := cms.insert(&val)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		mp[val.GetInt64()]++
	}
	return cms, mp, nil
}

func buildCMSketchTopNAndMap(d, w, n int32, seed int64, total, imax uint64, s float64) (*CMSketch, map[int64]uint32, error) {
	cms := NewCMSketchWithTopN(d, w, n)
	mp := make(map[int64]uint32)
	zipf := rand.NewZipf(rand.New(rand.NewSource(seed)), s, 1, imax)
	for i := uint64(0); i < total; i++ {
		val := types.NewIntDatum(int64(zipf.Uint64()))
		err := cms.insert(&val)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		mp[val.GetInt64()]++
	}
	bytes, err := encodeCMSketch(cms)
	if err != nil {
		return nil, nil, errors.Trace(nil)
	}
	rSketch, err := decodeCMSketch(bytes)
	if err != nil {
		return nil, nil, errors.Trace(nil)
	}
	return rSketch, mp, nil
}

func averageAbsoluteError(cms *CMSketch, mp map[int64]uint32) (uint64, error) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	var total uint64
	for num, count := range mp {
		estimate, err := cms.queryValue(sc, types.NewIntDatum(num))
		if err != nil {
			return 0, errors.Trace(err)
		}
		var diff uint32
		if count > estimate {
			diff = count - estimate
		} else {
			diff = estimate - count
		}
		total += uint64(diff)
	}
	return total / uint64(len(mp)), nil
}

func (s *testStatisticsSuite) TestCMSketch(c *C) {
	tests := []struct {
		zipfFactor float64
		avgError   uint64
	}{
		{
			zipfFactor: 1.1,
			avgError:   3,
		},
		{
			zipfFactor: 2,
			avgError:   24,
		},
		{
			zipfFactor: 3,
			avgError:   63,
		},
	}
	d, w := int32(5), int32(2048)
	total, imax := uint64(100000), uint64(1000000)
	for _, t := range tests {
		lSketch, lMap, err := buildCMSketchAndMap(d, w, 0, total, imax, t.zipfFactor)
		c.Check(err, IsNil)
		avg, err := averageAbsoluteError(lSketch, lMap)
		c.Assert(err, IsNil)
		c.Check(avg, LessEqual, t.avgError)

		rSketch, rMap, err := buildCMSketchAndMap(d, w, 1, total, imax, t.zipfFactor)
		c.Check(err, IsNil)
		avg, err = averageAbsoluteError(rSketch, rMap)
		c.Assert(err, IsNil)
		c.Check(avg, LessEqual, t.avgError)

		err = lSketch.MergeCMSketch(rSketch)
		c.Assert(err, IsNil)
		for val, count := range rMap {
			lMap[val] += count
		}
		avg, err = averageAbsoluteError(lSketch, lMap)
		c.Assert(err, IsNil)
		c.Check(avg, Less, t.avgError*2)
	}
}

func (s *testStatisticsSuite) TestCMSketchCoding(c *C) {
	lSketch := NewCMSketch(5, 2048)
	lSketch.count = 2048 * math.MaxUint32
	for i := range lSketch.table {
		for j := range lSketch.table[i] {
			lSketch.table[i][j] = math.MaxUint32
		}
	}
	bytes, err := encodeCMSketch(lSketch)
	c.Assert(err, IsNil)
	c.Assert(len(bytes), Equals, 61455)
	rSketch, err := decodeCMSketch(bytes)
	c.Assert(err, IsNil)
	c.Assert(lSketch.Equal(rSketch), IsTrue)
}

// Zipf distributuon
var hashCnt = []uint32{
	6947953, 3473976, 2315984, 1736988, 1389590, 1157992, 992564, 868494, 771994, 694795, 631632, 578996, 534457, 496282, 463196, 434247,
	408703, 385997, 365681, 347397, 330854, 315816, 302084, 289498, 277918, 267228, 257331, 248141, 239584, 231598, 224127, 217123,
	210544, 204351, 198512, 192998, 187782, 182840, 178152, 173698, 169462, 165427, 161580, 157908, 154398, 151042, 147828, 144749,
	141794, 138959, 136234, 133614, 131093, 128665, 126326, 124070, 121893, 119792, 117761, 115799, 113900, 112063, 110284, 108561,
	106891, 105272, 103700, 102175, 100694, 99256, 97858, 96499, 95177, 93891, 92639, 91420, 90233, 89076, 87948, 86849,
	85777, 84731, 83710, 82713, 81740, 80790, 79861, 78954, 78066, 77199, 76351, 75521, 74709, 73914, 73136, 72374,
	71628, 70897, 70181, 69479, 68791, 68117, 67455, 66807, 66170, 65546, 64934, 64332, 63742, 63163, 62594, 62035,
	61486, 60946, 60416, 59896, 59384, 58880, 58386, 57899, 57421, 56950, 56487, 56031, 55583, 55142, 54708, 54280,
}

func (s *testStatisticsSuite) TestCMSketchTopN(c *C) {
	tests := []struct {
		zipfFactor float64
		avgError   uint64
	}{
		{
			zipfFactor: 1.1,
			avgError:   3,
		},
		{
			zipfFactor: 2,
			avgError:   24,
		},
		{
			zipfFactor: 3,
			avgError:   63,
		},
	}
	d, w := int32(5), int32(2048)
	total, imax := uint64(100000), uint64(1000000)
	for _, t := range tests {
		lSketch, lMap, err := buildCMSketchTopNAndMap(d, w, 100, 0, total, imax, t.zipfFactor)
		c.Check(err, IsNil)
		avg, err := averageAbsoluteError(lSketch, lMap)
		c.Assert(err, IsNil)
		c.Check(avg, LessEqual, t.avgError)
	}
}

func (s *testStatisticsSuite) TestCMSketchTopNMerge(c *C) {
	tests := []struct {
		zipfFactor float64
		avgError   uint64
	}{
		{
			zipfFactor: 1.1,
			avgError:   3,
		},
		{
			zipfFactor: 2,
			avgError:   24,
		},
		{
			zipfFactor: 3,
			avgError:   63,
		},
	}
	d, w := int32(5), int32(2048)
	total, imax := uint64(100000), uint64(1000000)
	for _, t := range tests {
		lSketch, lMap, err := buildCMSketchTopNAndMap(d, w, 100, 0, total, imax, t.zipfFactor)
		c.Check(err, IsNil)
		avg, err := averageAbsoluteError(lSketch, lMap)
		c.Assert(err, IsNil)
		c.Check(avg, LessEqual, t.avgError)

		rSketch, rMap, err := buildCMSketchTopNAndMap(d, w, 100, 1, total, imax, t.zipfFactor)
		c.Check(err, IsNil)
		avg, err = averageAbsoluteError(rSketch, rMap)
		c.Assert(err, IsNil)
		c.Check(avg, LessEqual, t.avgError)

		err = lSketch.MergeCMSketch(rSketch)
		c.Assert(err, IsNil)
		for val, count := range rMap {
			lMap[val] += count
		}
		avg, err = averageAbsoluteError(lSketch, lMap)
		c.Assert(err, IsNil)
		c.Check(avg, Less, t.avgError*2)
		c.Assert(lSketch.topn, IsNil)
		c.Assert(lSketch.topnlimit, Equals, int32(0))
	}
}

func (s *testStatisticsSuite) TestCMSketchCodingTopN(c *C) {
	lSketch := NewCMSketchWithTopN(3, 16, 16)
	lSketch.count = 16 * math.MaxUint32
	for i, v := range hashCnt {
		h1, h2 := murmur3.Sum128([]byte(strconv.Itoa(i)))
		lSketch.setValue(h1, h2, v)
	}
	bytes, err := encodeCMSketch(lSketch)
	c.Assert(err, IsNil)
	c.Assert(len(bytes), Equals, 649)
	rSketch, err := decodeCMSketch(bytes)
	c.Assert(err, IsNil)
	c.Assert(rSketch.topnlimit, Equals, int32(0))
	bytes2, err := encodeCMSketch(rSketch)
	c.Assert(err, IsNil)
	c.Assert(len(bytes2), Equals, 649)
	tSketch, err := decodeCMSketch(bytes2)
	c.Assert(err, IsNil)
	c.Assert(tSketch.Equal(rSketch), IsTrue)
}

func (s *testStatisticsSuite) TestCMSketchCodingTopNUsful(c *C) {
	lSketch := NewCMSketchWithTopN(3, 4, 128)
	lSketch.count = 16 * math.MaxUint32
	for i, v := range hashCnt {
		h1, h2 := murmur3.Sum128([]byte(strconv.Itoa(i)))
		lSketch.setValue(h1, h2, v)
	}
	bytes, err := encodeCMSketch(lSketch)
	c.Assert(err, IsNil)
	c.Assert(len(bytes), Equals, 3533)
	rSketch, err := decodeCMSketch(bytes)
	c.Assert(err, IsNil)
	for i := range rSketch.table {
		for j := range rSketch.table[i] {
			c.Assert(rSketch.table[i][j], Equals, uint32(0))
		}
	}
}
