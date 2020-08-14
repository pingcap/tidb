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
	"fmt"
	"math"
	"math/rand"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/twmb/murmur3"
)

func (c *CMSketch) insert(val *types.Datum) error {
	bytes, err := codec.EncodeValue(nil, nil, *val)
	if err != nil {
		return errors.Trace(err)
	}
	c.InsertBytes(bytes)
	return nil
}

func prepareCMSWithTopN(d, w int32, vals []*types.Datum, n uint32, total uint64) (*CMSketch, error) {
	data := make([][]byte, 0, len(vals))
	for _, v := range vals {
		bytes, err := codec.EncodeValue(nil, nil, *v)
		if err != nil {
			return nil, errors.Trace(err)
		}
		data = append(data, bytes)
	}
	cms, _, _ := NewCMSketchWithTopN(d, w, data, n, total)
	return cms, nil
}

// buildCMSketchAndMapWithOffset builds cm sketch using zipf and the generated values starts from `offset`.
func buildCMSketchAndMapWithOffset(d, w int32, seed int64, total, imax uint64, s float64, offset int64) (*CMSketch, map[int64]uint32, error) {
	cms := NewCMSketch(d, w)
	mp := make(map[int64]uint32)
	zipf := rand.NewZipf(rand.New(rand.NewSource(seed)), s, 1, imax)
	for i := uint64(0); i < total; i++ {
		val := types.NewIntDatum(int64(zipf.Uint64()) + offset)
		err := cms.insert(&val)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		mp[val.GetInt64()]++
	}
	return cms, mp, nil
}

func buildCMSketchAndMap(d, w int32, seed int64, total, imax uint64, s float64) (*CMSketch, map[int64]uint32, error) {
	return buildCMSketchAndMapWithOffset(d, w, seed, total, imax, s, 0)
}

func buildCMSketchTopNAndMap(d, w, n, sample int32, seed int64, total, imax uint64, s float64) (*CMSketch, map[int64]uint32, error) {
	mp := make(map[int64]uint32)
	zipf := rand.NewZipf(rand.New(rand.NewSource(seed)), s, 1, imax)
	vals := make([]*types.Datum, 0)
	for i := uint64(0); i < total; i++ {
		val := types.NewIntDatum(int64(zipf.Uint64()))
		mp[val.GetInt64()]++
		if i < uint64(sample) {
			vals = append(vals, &val)
		}
	}
	cms, err := prepareCMSWithTopN(d, w, vals, uint32(n), total)
	return cms, mp, err
}

func averageAbsoluteError(cms *CMSketch, mp map[int64]uint32) (uint64, error) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	var total uint64
	for num, count := range mp {
		estimate, err := cms.queryValue(sc, types.NewIntDatum(num))
		if err != nil {
			return 0, errors.Trace(err)
		}
		var diff uint64
		if uint64(count) > estimate {
			diff = uint64(count) - estimate
		} else {
			diff = estimate - uint64(count)
		}
		total += diff
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

		err = lSketch.MergeCMSketch(rSketch, 0)
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
	bytes, err := EncodeCMSketchWithoutTopN(lSketch)
	c.Assert(err, IsNil)
	c.Assert(len(bytes), Equals, 61457)
	rSketch, err := DecodeCMSketch(bytes, nil)
	c.Assert(err, IsNil)
	c.Assert(lSketch.Equal(rSketch), IsTrue)
}

func (s *testStatisticsSuite) TestCMSketchTopN(c *C) {
	tests := []struct {
		zipfFactor float64
		avgError   uint64
	}{
		// If no significant most items, TopN may will produce results worse than normal algorithm.
		// The first two tests produces almost same avg.
		{
			zipfFactor: 1.0000001,
			avgError:   30,
		},
		{
			zipfFactor: 1.1,
			avgError:   30,
		},
		{
			zipfFactor: 2,
			avgError:   89,
		},
		// If the most data lies in a narrow range, our guess may have better result.
		// The error mainly comes from huge numbers.
		{
			zipfFactor: 5,
			avgError:   208,
		},
	}
	d, w := int32(5), int32(2048)
	total, imax := uint64(1000000), uint64(1000000)
	for _, t := range tests {
		lSketch, lMap, err := buildCMSketchTopNAndMap(d, w, 20, 1000, 0, total, imax, t.zipfFactor)
		c.Check(err, IsNil)
		c.Assert(len(lSketch.TopN()), LessEqual, 40)
		avg, err := averageAbsoluteError(lSketch, lMap)
		c.Assert(err, IsNil)
		c.Check(avg, LessEqual, t.avgError)
	}
}

func (s *testStatisticsSuite) TestMergeCMSketch4IncrementalAnalyze(c *C) {
	tests := []struct {
		zipfFactor float64
		avgError   uint64
	}{
		{
			zipfFactor: 1.0000001,
			avgError:   48,
		},
		{
			zipfFactor: 1.1,
			avgError:   48,
		},
		{
			zipfFactor: 2,
			avgError:   128,
		},
		{
			zipfFactor: 5,
			avgError:   256,
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

		rSketch, rMap, err := buildCMSketchAndMapWithOffset(d, w, 1, total, imax, t.zipfFactor, int64(imax))
		c.Check(err, IsNil)
		avg, err = averageAbsoluteError(rSketch, rMap)
		c.Assert(err, IsNil)
		c.Check(avg, LessEqual, t.avgError)

		for key, val := range rMap {
			lMap[key] += val
		}
		c.Assert(lSketch.MergeCMSketch4IncrementalAnalyze(rSketch, 0), IsNil)
		avg, err = averageAbsoluteError(lSketch, lMap)
		c.Assert(err, IsNil)
		c.Check(avg, LessEqual, t.avgError)
		width, depth := lSketch.GetWidthAndDepth()
		c.Assert(width, Equals, int32(2048))
		c.Assert(depth, Equals, int32(5))
	}
}

func (s *testStatisticsSuite) TestCMSketchTopNUniqueData(c *C) {
	d, w := int32(5), int32(2048)
	total := uint64(1000000)
	mp := make(map[int64]uint32)
	vals := make([]*types.Datum, 0)
	for i := uint64(0); i < total; i++ {
		val := types.NewIntDatum(int64(i))
		mp[val.GetInt64()]++
		if i < uint64(1000) {
			vals = append(vals, &val)
		}
	}
	cms, err := prepareCMSWithTopN(d, w, vals, uint32(20), total)
	c.Assert(err, IsNil)
	avg, err := averageAbsoluteError(cms, mp)
	c.Assert(err, IsNil)
	c.Check(cms.defaultValue, Equals, uint64(1))
	c.Check(avg, Equals, uint64(0))
	c.Check(len(cms.topN), Equals, 0)
}

func (s *testStatisticsSuite) TestCMSketchCodingTopN(c *C) {
	lSketch := NewCMSketch(5, 2048)
	lSketch.count = 2048 * (math.MaxUint32)
	for i := range lSketch.table {
		for j := range lSketch.table[i] {
			lSketch.table[i][j] = math.MaxUint32
		}
	}
	lSketch.topN = make(map[uint64][]*TopNMeta)
	unsignedLong := types.NewFieldType(mysql.TypeLonglong)
	unsignedLong.Flag |= mysql.UnsignedFlag
	chk := chunk.New([]*types.FieldType{types.NewFieldType(mysql.TypeBlob), unsignedLong}, 20, 20)
	var rows []chunk.Row
	for i := 0; i < 20; i++ {
		tString := []byte(fmt.Sprintf("%20000d", i))
		h1, h2 := murmur3.Sum128(tString)
		lSketch.topN[h1] = []*TopNMeta{{h2, tString, math.MaxUint64}}
		chk.AppendBytes(0, tString)
		chk.AppendUint64(1, math.MaxUint64)
		rows = append(rows, chk.GetRow(i))
	}

	bytes, err := EncodeCMSketchWithoutTopN(lSketch)
	c.Assert(err, IsNil)
	c.Assert(len(bytes), Equals, 61457)
	rSketch, err := DecodeCMSketch(bytes, rows)
	c.Assert(err, IsNil)
	c.Assert(lSketch.Equal(rSketch), IsTrue)
	// do not panic
	DecodeCMSketch([]byte{}, rows)
}
