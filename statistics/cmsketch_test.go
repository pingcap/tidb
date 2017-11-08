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
	"math/rand"
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

func (c *CMSketch) insert(val *types.Datum) error {
	bytes, err := codec.EncodeValue(nil, *val)
	if err != nil {
		return errors.Trace(err)
	}
	c.InsertBytes(bytes)
	return nil
}

func buildCMSketchAndMap(d, w int32, total, imax uint64, s float64) (*CMSketch, map[int64]uint32, error) {
	cms := NewCMSketch(d, w)
	mp := make(map[int64]uint32)
	zipf := rand.NewZipf(rand.New(rand.NewSource(time.Now().UnixNano())), s, 1, imax)
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

func averageAbsoluteError(cms *CMSketch, mp map[int64]uint32) (uint64, error) {
	var total uint64
	for num, count := range mp {
		estimate, err := cms.queryValue(types.NewIntDatum(num))
		if err != nil {
			return 0, errors.Trace(err)
		}
		diff := uint32(0)
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
			avgError:   6,
		},
		{
			zipfFactor: 2,
			avgError:   33,
		},
		{
			zipfFactor: 3,
			avgError:   90,
		},
	}
	d, w := int32(8), int32(2048)
	total, imax := uint64(1000000), uint64(10000000)
	for _, t := range tests {
		lSketch, lMap, err := buildCMSketchAndMap(d, w, total, imax, t.zipfFactor)
		c.Check(err, IsNil)
		avg, err := averageAbsoluteError(lSketch, lMap)
		c.Assert(err, IsNil)
		c.Check(avg, Less, t.avgError)

		rSketch, rMap, err := buildCMSketchAndMap(d, w, total, imax, t.zipfFactor)
		c.Check(err, IsNil)
		avg, err = averageAbsoluteError(rSketch, rMap)
		c.Assert(err, IsNil)
		c.Check(avg, Less, t.avgError)

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
	lSketch, _, err := buildCMSketchAndMap(8, 2048, 1000, 1000, 1.1)
	c.Assert(err, IsNil)
	bytes, err := encodeCMSketch(lSketch)
	c.Assert(err, IsNil)
	rSketch, err := decodeCMSketch(bytes)
	c.Assert(err, IsNil)
	c.Assert(lSketch.Equal(rSketch), IsTrue)
}
