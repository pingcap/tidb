// Copyright 2019 PingCAP, Inc.
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

package executor

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/rand"
	"sort"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
)

var _ = Suite(&testSplitIndex{})

type testSplitIndex struct {
}

func (s *testSplitIndex) SetUpSuite(c *C) {
}

func (s *testSplitIndex) TearDownSuite(c *C) {
}

func (s *testSplitIndex) TestLongestCommonPrefixLen(c *C) {
	cases := []struct {
		s1 string
		s2 string
		l  int
	}{
		{"", "", 0},
		{"", "a", 0},
		{"a", "", 0},
		{"a", "a", 1},
		{"ab", "a", 1},
		{"a", "ab", 1},
		{"b", "ab", 0},
		{"ba", "ab", 0},
	}

	for _, ca := range cases {
		re := longestCommonPrefixLen([]byte(ca.s1), []byte(ca.s2))
		c.Assert(re, Equals, ca.l)
	}
}

func (s *testSplitIndex) TestgetStepValue(c *C) {
	cases := []struct {
		lower []byte
		upper []byte
		l     int
		v     uint64
	}{
		{[]byte{}, []byte{}, 0, math.MaxUint64},
		{[]byte{0}, []byte{128}, 0, binary.BigEndian.Uint64([]byte{128, 255, 255, 255, 255, 255, 255, 255})},
		{[]byte{'a'}, []byte{'z'}, 0, binary.BigEndian.Uint64([]byte{'z' - 'a', 255, 255, 255, 255, 255, 255, 255})},
		{[]byte("abc"), []byte{'z'}, 0, binary.BigEndian.Uint64([]byte{'z' - 'a', 255 - 'b', 255 - 'c', 255, 255, 255, 255, 255})},
		{[]byte("abc"), []byte("xyz"), 0, binary.BigEndian.Uint64([]byte{'x' - 'a', 'y' - 'b', 'z' - 'c', 255, 255, 255, 255, 255})},
		{[]byte("abc"), []byte("axyz"), 1, binary.BigEndian.Uint64([]byte{'x' - 'b', 'y' - 'c', 'z', 255, 255, 255, 255, 255})},
		{[]byte("abc0123456"), []byte("xyz01234"), 0, binary.BigEndian.Uint64([]byte{'x' - 'a', 'y' - 'b', 'z' - 'c', 0, 0, 0, 0, 0})},
	}

	for _, ca := range cases {
		l := longestCommonPrefixLen(ca.lower, ca.upper)
		c.Assert(l, Equals, ca.l)
		v0 := getStepValue(ca.lower[l:], ca.upper[l:], 1)
		c.Assert(v0, Equals, ca.v)
	}
}

func (s *testSplitIndex) TestSplitIndex(c *C) {
	tbInfo := &model.TableInfo{
		Name: model.NewCIStr("t1"),
		ID:   rand.Int63(),
		Columns: []*model.ColumnInfo{
			{
				Name:         model.NewCIStr("c0"),
				ID:           1,
				Offset:       1,
				DefaultValue: 0,
				State:        model.StatePublic,
				FieldType:    *types.NewFieldType(mysql.TypeLong),
			},
		},
	}
	idxCols := []*model.IndexColumn{{Name: tbInfo.Columns[0].Name, Offset: 0, Length: types.UnspecifiedLength}}
	idxInfo := &model.IndexInfo{
		ID:      2,
		Name:    model.NewCIStr("idx1"),
		Table:   model.NewCIStr("t1"),
		Columns: idxCols,
		State:   model.StatePublic,
	}
	firstIdxInfo0 := idxInfo.Clone()
	firstIdxInfo0.ID = 1
	firstIdxInfo0.Name = model.NewCIStr("idx")
	tbInfo.Indices = []*model.IndexInfo{firstIdxInfo0, idxInfo}

	// Test for int index.
	// range is 0 ~ 100, and split into 10 region.
	// So 10 regions range is like below, left close right open interval:
	// region1: [-inf ~ 10)
	// region2: [10 ~ 20)
	// region3: [20 ~ 30)
	// region4: [30 ~ 40)
	// region5: [40 ~ 50)
	// region6: [50 ~ 60)
	// region7: [60 ~ 70)
	// region8: [70 ~ 80)
	// region9: [80 ~ 90)
	// region10: [90 ~ +inf)
	ctx := mock.NewContext()
	e := &SplitIndexRegionExec{
		baseExecutor: newBaseExecutor(ctx, nil, 0),
		tableInfo:    tbInfo,
		indexInfo:    idxInfo,
		lower:        []types.Datum{types.NewDatum(0)},
		upper:        []types.Datum{types.NewDatum(100)},
		num:          10,
	}
	valueList, err := e.getSplitIdxKeys()
	sort.Slice(valueList, func(i, j int) bool { return bytes.Compare(valueList[i], valueList[j]) < 0 })
	c.Assert(err, IsNil)
	c.Assert(len(valueList), Equals, e.num+1)

	cases := []struct {
		value        int
		lessEqualIdx int
	}{
		{-1, 0},
		{0, 0},
		{1, 0},
		{10, 1},
		{11, 1},
		{20, 2},
		{21, 2},
		{31, 3},
		{41, 4},
		{51, 5},
		{61, 6},
		{71, 7},
		{81, 8},
		{91, 9},
		{100, 9},
		{1000, 9},
	}

	index := tables.NewIndex(tbInfo.ID, tbInfo, idxInfo)
	for _, ca := range cases {
		// test for minInt64 handle
		idxValue, _, err := index.GenIndexKey(ctx.GetSessionVars().StmtCtx, []types.Datum{types.NewDatum(ca.value)}, math.MinInt64, nil)
		c.Assert(err, IsNil)
		idx := searchLessEqualIdx(valueList, idxValue)
		c.Assert(idx, Equals, ca.lessEqualIdx, Commentf("%#v", ca))

		// Test for max int64 handle.
		idxValue, _, err = index.GenIndexKey(ctx.GetSessionVars().StmtCtx, []types.Datum{types.NewDatum(ca.value)}, math.MaxInt64, nil)
		c.Assert(err, IsNil)
		idx = searchLessEqualIdx(valueList, idxValue)
		c.Assert(idx, Equals, ca.lessEqualIdx, Commentf("%#v", ca))
	}
	// Test for varchar index.
	// range is a ~ z, and split into 26 region.
	// So 26 regions range is like below:
	// region1: [-inf ~ b)
	// region2: [b ~ c)
	// .
	// .
	// .
	// region26: [y ~ +inf)
	e.lower = []types.Datum{types.NewDatum("a")}
	e.upper = []types.Datum{types.NewDatum("z")}
	e.num = 26
	// change index column type to varchar
	tbInfo.Columns[0].FieldType = *types.NewFieldType(mysql.TypeVarchar)

	valueList, err = e.getSplitIdxKeys()
	sort.Slice(valueList, func(i, j int) bool { return bytes.Compare(valueList[i], valueList[j]) < 0 })
	c.Assert(err, IsNil)
	c.Assert(len(valueList), Equals, e.num+1)

	cases2 := []struct {
		value        string
		lessEqualIdx int
	}{
		{"", 0},
		{"a", 0},
		{"abcde", 0},
		{"b", 1},
		{"bzzzz", 1},
		{"c", 2},
		{"czzzz", 2},
		{"z", 25},
		{"zabcd", 25},
	}

	for _, ca := range cases2 {
		// test for minInt64 handle
		idxValue, _, err := index.GenIndexKey(ctx.GetSessionVars().StmtCtx, []types.Datum{types.NewDatum(ca.value)}, math.MinInt64, nil)
		c.Assert(err, IsNil)
		idx := searchLessEqualIdx(valueList, idxValue)
		c.Assert(idx, Equals, ca.lessEqualIdx, Commentf("%#v", ca))

		// Test for max int64 handle.
		idxValue, _, err = index.GenIndexKey(ctx.GetSessionVars().StmtCtx, []types.Datum{types.NewDatum(ca.value)}, math.MaxInt64, nil)
		c.Assert(err, IsNil)
		idx = searchLessEqualIdx(valueList, idxValue)
		c.Assert(idx, Equals, ca.lessEqualIdx, Commentf("%#v", ca))
	}

	// Test for timestamp index.
	// range is 2010-01-01 00:00:00 ~ 2020-01-01 00:00:00, and split into 10 region.
	// So 10 regions range is like below:
	// region1: [-inf					~ 2011-01-01 00:00:00)
	// region2: [2011-01-01 00:00:00 	~ 2012-01-01 00:00:00)
	// .
	// .
	// .
	// region10: [2019-01-01 00:00:00 	~ +inf)
	lowerTime := types.NewTime(types.FromDate(2010, 1, 1, 0, 0, 0, 0), mysql.TypeTimestamp, types.DefaultFsp)
	upperTime := types.NewTime(types.FromDate(2020, 1, 1, 0, 0, 0, 0), mysql.TypeTimestamp, types.DefaultFsp)
	e.lower = []types.Datum{types.NewDatum(lowerTime)}
	e.upper = []types.Datum{types.NewDatum(upperTime)}
	e.num = 10

	// change index column type to timestamp
	tbInfo.Columns[0].FieldType = *types.NewFieldType(mysql.TypeTimestamp)

	valueList, err = e.getSplitIdxKeys()
	sort.Slice(valueList, func(i, j int) bool { return bytes.Compare(valueList[i], valueList[j]) < 0 })
	c.Assert(err, IsNil)
	c.Assert(len(valueList), Equals, e.num+1)

	cases3 := []struct {
		value        types.CoreTime
		lessEqualIdx int
	}{
		{types.FromDate(2009, 11, 20, 12, 50, 59, 0), 0},
		{types.FromDate(2010, 1, 1, 0, 0, 0, 0), 0},
		{types.FromDate(2011, 12, 31, 23, 59, 59, 0), 1},
		{types.FromDate(2011, 2, 1, 0, 0, 0, 0), 1},
		{types.FromDate(2012, 3, 1, 0, 0, 0, 0), 2},
		{types.FromDate(2013, 4, 1, 0, 0, 0, 0), 3},
		{types.FromDate(2014, 5, 1, 0, 0, 0, 0), 4},
		{types.FromDate(2015, 6, 1, 0, 0, 0, 0), 5},
		{types.FromDate(2016, 8, 1, 0, 0, 0, 0), 6},
		{types.FromDate(2017, 9, 1, 0, 0, 0, 0), 7},
		{types.FromDate(2018, 10, 1, 0, 0, 0, 0), 8},
		{types.FromDate(2019, 11, 1, 0, 0, 0, 0), 9},
		{types.FromDate(2020, 12, 1, 0, 0, 0, 0), 9},
		{types.FromDate(2030, 12, 1, 0, 0, 0, 0), 9},
	}

	for _, ca := range cases3 {
		value := types.NewTime(ca.value, mysql.TypeTimestamp, types.DefaultFsp)
		// test for min int64 handle
		idxValue, _, err := index.GenIndexKey(ctx.GetSessionVars().StmtCtx, []types.Datum{types.NewDatum(value)}, math.MinInt64, nil)
		c.Assert(err, IsNil)
		idx := searchLessEqualIdx(valueList, idxValue)
		c.Assert(idx, Equals, ca.lessEqualIdx, Commentf("%#v", ca))

		// Test for max int64 handle.
		idxValue, _, err = index.GenIndexKey(ctx.GetSessionVars().StmtCtx, []types.Datum{types.NewDatum(value)}, math.MaxInt64, nil)
		c.Assert(err, IsNil)
		idx = searchLessEqualIdx(valueList, idxValue)
		c.Assert(idx, Equals, ca.lessEqualIdx, Commentf("%#v", ca))
	}
}

func (s *testSplitIndex) TestSplitTable(c *C) {
	tbInfo := &model.TableInfo{
		Name: model.NewCIStr("t1"),
		ID:   rand.Int63(),
		Columns: []*model.ColumnInfo{
			{
				Name:         model.NewCIStr("c0"),
				ID:           1,
				Offset:       1,
				DefaultValue: 0,
				State:        model.StatePublic,
				FieldType:    *types.NewFieldType(mysql.TypeLong),
			},
		},
	}
	defer func(originValue int64) {
		minRegionStepValue = originValue
	}(minRegionStepValue)
	minRegionStepValue = 10
	// range is 0 ~ 100, and split into 10 region.
	// So 10 regions range is like below:
	// region1: [-inf ~ 10)
	// region2: [10 ~ 20)
	// region3: [20 ~ 30)
	// region4: [30 ~ 40)
	// region5: [40 ~ 50)
	// region6: [50 ~ 60)
	// region7: [60 ~ 70)
	// region8: [70 ~ 80)
	// region9: [80 ~ 90 )
	// region10: [90 ~ +inf)
	ctx := mock.NewContext()
	e := &SplitTableRegionExec{
		baseExecutor: newBaseExecutor(ctx, nil, 0),
		tableInfo:    tbInfo,
		lower:        types.NewDatum(0),
		upper:        types.NewDatum(100),
		num:          10,
	}
	valueList, err := e.getSplitTableKeys()
	c.Assert(err, IsNil)
	c.Assert(len(valueList), Equals, e.num-1)

	cases := []struct {
		value        int
		lessEqualIdx int
	}{
		{-1, -1},
		{0, -1},
		{1, -1},
		{10, 0},
		{11, 0},
		{20, 1},
		{21, 1},
		{31, 2},
		{41, 3},
		{51, 4},
		{61, 5},
		{71, 6},
		{81, 7},
		{91, 8},
		{100, 8},
		{1000, 8},
	}

	recordPrefix := tablecodec.GenTableRecordPrefix(e.tableInfo.ID)
	for _, ca := range cases {
		// test for minInt64 handle
<<<<<<< HEAD
		key := tablecodec.EncodeRecordKey(recordPrefix, int64(ca.value))
=======
		key := tablecodec.EncodeRecordKey(recordPrefix, kv.IntHandle(ca.value))
		c.Assert(err, IsNil)
		idx := searchLessEqualIdx(valueList, key)
		c.Assert(idx, Equals, ca.lessEqualIdx, Commentf("%#v", ca))
	}
}

func (s *testSplitIndex) TestClusterIndexSplitTable(c *C) {
	tbInfo := &model.TableInfo{
		Name:           model.NewCIStr("t"),
		ID:             1,
		IsCommonHandle: true,
		Indices: []*model.IndexInfo{
			{
				ID:      1,
				Primary: true,
				State:   model.StatePublic,
				Columns: []*model.IndexColumn{
					{Offset: 1},
					{Offset: 2},
				},
			},
		},
		Columns: []*model.ColumnInfo{
			{
				Name:      model.NewCIStr("c0"),
				ID:        1,
				Offset:    0,
				State:     model.StatePublic,
				FieldType: *types.NewFieldType(mysql.TypeDouble),
			},
			{
				Name:      model.NewCIStr("c1"),
				ID:        2,
				Offset:    1,
				State:     model.StatePublic,
				FieldType: *types.NewFieldType(mysql.TypeLonglong),
			},
			{
				Name:      model.NewCIStr("c2"),
				ID:        3,
				Offset:    2,
				State:     model.StatePublic,
				FieldType: *types.NewFieldType(mysql.TypeLonglong),
			},
		},
	}
	defer func(originValue int64) {
		minRegionStepValue = originValue
	}(minRegionStepValue)
	minRegionStepValue = 3
	ctx := mock.NewContext()
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	e := &SplitTableRegionExec{
		baseExecutor: newBaseExecutor(ctx, nil, 0),
		tableInfo:    tbInfo,
		handleCols:   buildHandleColsForSplit(sc, tbInfo),
		lower:        types.MakeDatums(1, 0),
		upper:        types.MakeDatums(1, 100),
		num:          10,
	}
	valueList, err := e.getSplitTableKeys()
	c.Assert(err, IsNil)
	c.Assert(len(valueList), Equals, e.num-1)

	cases := []struct {
		value        []types.Datum
		lessEqualIdx int
	}{
		// For lower-bound and upper-bound, because 0 and 100 are padding with 7 zeros,
		// the split points are not (i * 10) but approximation.
		{types.MakeDatums(1, -1), -1},
		{types.MakeDatums(1, 0), -1},
		{types.MakeDatums(1, 10), -1},
		{types.MakeDatums(1, 11), 0},
		{types.MakeDatums(1, 20), 0},
		{types.MakeDatums(1, 21), 1},

		{types.MakeDatums(1, 31), 2},
		{types.MakeDatums(1, 41), 3},
		{types.MakeDatums(1, 51), 4},
		{types.MakeDatums(1, 61), 5},
		{types.MakeDatums(1, 71), 6},
		{types.MakeDatums(1, 81), 7},
		{types.MakeDatums(1, 91), 8},
		{types.MakeDatums(1, 100), 8},
		{types.MakeDatums(1, 101), 8},
	}

	recordPrefix := tablecodec.GenTableRecordPrefix(e.tableInfo.ID)
	for _, ca := range cases {
		h, err := e.handleCols.BuildHandleByDatums(ca.value)
		c.Assert(err, IsNil)
		key := tablecodec.EncodeRecordKey(recordPrefix, h)
>>>>>>> a2e2ce6... *: use int instead of fmt.Stringer as executor id (#19207)
		c.Assert(err, IsNil)
		idx := searchLessEqualIdx(valueList, key)
		c.Assert(idx, Equals, ca.lessEqualIdx, Commentf("%#v", ca))
	}
}

func searchLessEqualIdx(valueList [][]byte, value []byte) int {
	idx := -1
	for i, v := range valueList {
		if bytes.Compare(value, v) >= 0 {
			idx = i
			continue
		}
		break
	}
	return idx
}
