// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/ttl/session"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

func TestNewTTLTable(t *testing.T) {
	cases := []struct {
		db      string
		tbl     string
		def     string
		timeCol string
		keyCols []string
	}{
		{
			db:  "test",
			tbl: "t1",
			def: "(a int)",
		},
		{
			db:      "test",
			tbl:     "ttl1",
			def:     "(a int, t datetime) ttl = `t` + interval 2 hour",
			timeCol: "t",
			keyCols: []string{"_tidb_rowid"},
		},
		{
			db:      "test",
			tbl:     "ttl2",
			def:     "(id int primary key, t datetime) ttl = `t` + interval 3 hour",
			timeCol: "t",
			keyCols: []string{"id"},
		},
		{
			db:      "test",
			tbl:     "ttl3",
			def:     "(a int, b varchar(32), c binary(32), t datetime, primary key (a, b, c)) ttl = `t` + interval 1 month",
			timeCol: "t",
			keyCols: []string{"a", "b", "c"},
		},
		{
			db:  "test",
			tbl: "ttl4",
			def: "(id int primary key, t datetime) " +
				"ttl = `t` + interval 1 day " +
				"PARTITION BY RANGE (id) (" +
				"	PARTITION p0 VALUES LESS THAN (10)," +
				"	PARTITION p1 VALUES LESS THAN (100)," +
				"	PARTITION p2 VALUES LESS THAN (1000)," +
				"	PARTITION p3 VALUES LESS THAN MAXVALUE)",
			timeCol: "t",
			keyCols: []string{"id"},
		},
		{
			db:      "test",
			tbl:     "ttl5",
			def:     "(id int primary key nonclustered, t datetime) ttl = `t` + interval 3 hour",
			timeCol: "t",
			keyCols: []string{"_tidb_rowid"},
		},
	}

	store, do := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	for _, c := range cases {
		tk.MustExec("use " + c.db)
		tk.MustExec("create table " + c.tbl + c.def)
	}

	for _, c := range cases {
		is := do.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr(c.db), ast.NewCIStr(c.tbl))
		require.NoError(t, err)
		tblInfo := tbl.Meta()
		var physicalTbls []*cache.PhysicalTable
		if tblInfo.Partition == nil {
			ttlTbl, err := cache.NewPhysicalTable(ast.NewCIStr(c.db), tblInfo, ast.NewCIStr(""))
			if c.timeCol == "" {
				require.Error(t, err)
				continue
			}
			require.NoError(t, err)
			physicalTbls = append(physicalTbls, ttlTbl)
		} else {
			for _, partition := range tblInfo.Partition.Definitions {
				ttlTbl, err := cache.NewPhysicalTable(ast.NewCIStr(c.db), tblInfo, partition.Name)
				if c.timeCol == "" {
					require.Error(t, err)
					continue
				}
				require.NoError(t, err)
				physicalTbls = append(physicalTbls, ttlTbl)
			}
			if c.timeCol == "" {
				continue
			}
		}

		for i, ttlTbl := range physicalTbls {
			require.Equal(t, c.db, ttlTbl.Schema.O)
			require.Same(t, tblInfo, ttlTbl.TableInfo)
			timeColumn := tblInfo.FindPublicColumnByName(c.timeCol)
			require.NotNil(t, timeColumn)
			require.Same(t, timeColumn, ttlTbl.TimeColumn)

			if tblInfo.Partition == nil {
				require.Equal(t, ttlTbl.TableInfo.ID, ttlTbl.ID)
				require.Equal(t, "", ttlTbl.Partition.L)
				require.Nil(t, ttlTbl.PartitionDef)
			} else {
				def := tblInfo.Partition.Definitions[i]
				require.Equal(t, def.ID, ttlTbl.ID)
				require.Equal(t, def.Name.L, ttlTbl.Partition.L)
				require.Equal(t, def, *(ttlTbl.PartitionDef))
			}

			require.Equal(t, len(c.keyCols), len(ttlTbl.KeyColumns))
			require.Equal(t, len(c.keyCols), len(ttlTbl.KeyColumnTypes))

			for j, keyCol := range c.keyCols {
				msg := fmt.Sprintf("%s, col: %s", c.tbl, keyCol)
				var col *model.ColumnInfo
				if keyCol == model.ExtraHandleName.L {
					col = model.NewExtraHandleColInfo()
				} else {
					col = tblInfo.FindPublicColumnByName(keyCol)
				}
				colJ := ttlTbl.KeyColumns[j]
				colFieldJ := ttlTbl.KeyColumnTypes[j]

				require.NotNil(t, col, msg)
				require.Equal(t, col.ID, colJ.ID, msg)
				require.Equal(t, col.Name.L, colJ.Name.L, msg)
				require.Equal(t, col.FieldType, colJ.FieldType, msg)
				require.Equal(t, col.FieldType, *colFieldJ, msg)
			}
		}
	}
}

func TestTableEvalTTLExpireTime(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@time_zone='Asia/Tokyo'")

	tk.MustExec("create table test.t(a int, t datetime) ttl = `t` + interval 1 month")
	tb, err := do.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tb.Meta()
	ttlTbl, err := cache.NewPhysicalTable(ast.NewCIStr("test"), tblInfo, ast.NewCIStr(""))
	require.NoError(t, err)

	se := session.NewSession(tk.Session(), func() {})
	// the global timezone set to +02:00
	tz1 := time.FixedZone("", 2*3600)
	_, err = se.ExecuteSQL(context.TODO(), "SET @@global.time_zone = '+02:00'")
	require.NoError(t, err)
	// the timezone of now argument is set to -02:00
	tz2 := time.FixedZone("-02:00", -2*3600)
	now, err := time.ParseInLocation(time.DateTime, "1999-02-28 23:00:00", tz2)
	require.NoError(t, err)
	tm, err := ttlTbl.EvalExpireTime(context.TODO(), se, now)
	require.NoError(t, err)
	// The expired time should be calculated according to the global time zone
	require.Equal(t, "1999-02-01 03:00:00", tm.In(tz1).Format(time.DateTime))
	// The location of the expired time should be the same with the input argument `now`
	require.Same(t, tz2, tm.Location())

	// should support a string format interval
	tk.MustExec("create table test.t2(a int, t datetime) ttl = `t` + interval '1:3' hour_minute")
	tb2, err := do.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t2"))
	require.NoError(t, err)
	tblInfo2 := tb2.Meta()
	ttlTbl2, err := cache.NewPhysicalTable(ast.NewCIStr("test"), tblInfo2, ast.NewCIStr(""))
	require.NoError(t, err)
	now, err = time.ParseInLocation(time.DateTime, "2020-01-01 15:00:00", tz1)
	require.NoError(t, err)
	tm, err = ttlTbl2.EvalExpireTime(context.TODO(), se, now)
	require.NoError(t, err)
	require.Equal(t, "2020-01-01 13:57:00", tm.Format(time.DateTime))
	require.Same(t, tz1, tm.Location())

	// session time zone should keep unchanged
	tk.MustQuery("select @@time_zone").Check(testkit.Rows("Asia/Tokyo"))
}

func TestEvalTTLExpireTime(t *testing.T) {
	tzShanghai, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	tzBerlin, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)

	tm, err := cache.EvalExpireTime(time.UnixMilli(0).In(tzShanghai), "1", ast.TimeUnitDay)
	require.NoError(t, err)
	require.Equal(t, time.UnixMilli(0).Add(-time.Hour*24).Unix(), tm.Unix())
	require.Equal(t, "1969-12-31 08:00:00", tm.Format(time.DateTime))
	require.Same(t, tzShanghai, tm.Location())

	tm, err = cache.EvalExpireTime(time.UnixMilli(0).In(tzBerlin), "1", ast.TimeUnitDay)
	require.NoError(t, err)
	require.Equal(t, time.UnixMilli(0).Add(-time.Hour*24).Unix(), tm.Unix())
	require.Equal(t, "1969-12-31 01:00:00", tm.In(tzBerlin).Format(time.DateTime))
	require.Same(t, tzBerlin, tm.Location())

	tm, err = cache.EvalExpireTime(time.UnixMilli(0).In(tzShanghai), "3", ast.TimeUnitMonth)
	require.NoError(t, err)
	require.Equal(t, "1969-10-01 08:00:00", tm.In(tzShanghai).Format(time.DateTime))
	require.Same(t, tzShanghai, tm.Location())

	tm, err = cache.EvalExpireTime(time.UnixMilli(0).In(tzBerlin), "3", ast.TimeUnitMonth)
	require.NoError(t, err)
	require.Equal(t, "1969-10-01 01:00:00", tm.In(tzBerlin).Format(time.DateTime))
	require.Same(t, tzBerlin, tm.Location())

	// test cases for daylight saving time.
	// When local standard time was about to reach Sunday, 10 March 2024, 02:00:00 clocks were turned forward 1 hour to
	// Sunday, 10 March 2024, 03:00:00 local daylight time instead.
	tzLosAngeles, err := time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err)
	now, err := time.ParseInLocation(time.DateTime, "2024-03-11 19:49:59", tzLosAngeles)
	require.NoError(t, err)
	tm, err = cache.EvalExpireTime(now, "90", ast.TimeUnitMinute)
	require.NoError(t, err)
	require.Equal(t, "2024-03-11 18:19:59", tm.Format(time.DateTime))
	require.Same(t, tzLosAngeles, tm.Location())

	// across day light-saving time
	now, err = time.ParseInLocation(time.DateTime, "2024-03-10 03:01:00", tzLosAngeles)
	require.NoError(t, err)
	tm, err = cache.EvalExpireTime(now, "90", ast.TimeUnitMinute)
	require.NoError(t, err)
	require.Equal(t, "2024-03-10 00:31:00", tm.Format(time.DateTime))
	require.Same(t, tzLosAngeles, tm.Location())

	now, err = time.ParseInLocation(time.DateTime, "2024-03-10 04:01:00", tzLosAngeles)
	require.NoError(t, err)
	tm, err = cache.EvalExpireTime(now, "90", ast.TimeUnitMinute)
	require.NoError(t, err)
	require.Equal(t, "2024-03-10 01:31:00", tm.Format(time.DateTime))
	require.Same(t, tzLosAngeles, tm.Location())

	now, err = time.ParseInLocation(time.DateTime, "2024-11-03 03:00:00", tzLosAngeles)
	require.NoError(t, err)
	tm, err = cache.EvalExpireTime(now, "90", ast.TimeUnitMinute)
	require.NoError(t, err)
	require.Equal(t, "2024-11-03 01:30:00", tm.Format(time.DateTime))
	require.Same(t, tzLosAngeles, tm.Location())
	// 2024-11-03 01:30:00 in America/Los_Angeles has two related time points:
	// 2024-11-03 01:30:00 -0700 PDT
	// 2024-11-03 01:30:00 -0800 PST
	// We must use the earlier one to avoid deleting some unexpected rows.
	require.Equal(t, int64(5400), now.Unix()-tm.Unix())

	// time should be truncated to second to make the result simple
	now, err = time.ParseInLocation("2006-01-02 15:04:05.000000", "2023-01-02 15:00:01.986542", time.UTC)
	require.NoError(t, err)
	tm, err = cache.EvalExpireTime(now, "1", ast.TimeUnitDay)
	require.NoError(t, err)
	require.Equal(t, "2023-01-01 15:00:01.000000", tm.Format("2006-01-02 15:04:05.000000"))
	require.Same(t, time.UTC, tm.Location())

	// test for string interval format
	tm, err = cache.EvalExpireTime(time.Unix(0, 0).In(tzBerlin), "'1:3'", ast.TimeUnitHourMinute)
	require.NoError(t, err)
	require.Equal(t, "1969-12-31 22:57:00", tm.In(time.UTC).Format(time.DateTime))
	require.Same(t, tzBerlin, tm.Location())
}

func TestFindTTLIndex(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	cases := []struct {
		def       string
		indexName string
	}{
		{
			def: "(id int primary key, t datetime) ttl = `t` + interval 1 day",
		},
		{
			// A clustered common handle is the table path itself, so it should keep using the existing PK scan path.
			def: "(t datetime not null, id bigint not null, primary key(t, id) clustered) ttl = `t` + interval 1 day",
		},
		{
			def:       "(id int primary key, t datetime, a int, index idx_bad(t, a), index idx_t(t)) ttl = `t` + interval 1 day",
			indexName: "idx_t",
		},
		{
			def:       "(id int primary key, t datetime, a int, index idx_wide(t, a), index idx_key(t, id)) ttl = `t` + interval 1 day",
			indexName: "idx_key",
		},
		{
			def: "(id varchar(32), t datetime, primary key(id(4)) clustered, index idx_t(t)) ttl = `t` + interval 1 day",
		},
		{
			def: "(id int primary key, t datetime, index idx_t(id, t)) ttl = `t` + interval 1 day",
		},
		{
			def:       "(id int primary key, t datetime, index idx_a(id), index idx_t(t)) ttl = `t` + interval 1 day",
			indexName: "idx_t",
		},
	}

	for i, c := range cases {
		tblName := fmt.Sprintf("ttl_idx_%d", i)
		tk.MustExec(fmt.Sprintf("create table %s %s", tblName, c.def))
		tb, err := do.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(tblName))
		require.NoError(t, err)
		tblInfo := tb.Meta()
		ttlTbl, err := cache.NewPhysicalTable(ast.NewCIStr("test"), tblInfo, ast.NewCIStr(""))
		require.NoError(t, err)

		idx := ttlTbl.FindTTLIndex()
		if c.indexName != "" {
			require.NotNil(t, idx, "table %s should have TTL index", tblName)
			require.Equal(t, c.indexName, idx.Name.O)
		} else {
			require.Nil(t, idx, "table %s should not have TTL index", tblName)
		}
	}

	tk.MustExec("create table ttl_idx_nil_time(id int primary key, t datetime, index idx_t(t)) ttl = `t` + interval 1 day")
	tb, err := do.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ttl_idx_nil_time"))
	require.NoError(t, err)
	ttlTbl, err := cache.NewPhysicalTable(ast.NewCIStr("test"), tb.Meta(), ast.NewCIStr(""))
	require.NoError(t, err)
	ttlTbl.TimeColumn = nil
	require.Nil(t, ttlTbl.FindTTLIndex())

	// Some unsupported index properties cannot be expressed by ordinary CREATE
	// TABLE statements in this test environment. Start from one valid index and
	// mutate a fresh metadata clone for each rejection path.
	tk.MustExec("create table ttl_idx_metadata(id int primary key, t datetime, a int, index idx_t(t, a)) ttl = `t` + interval 1 day")
	tb, err = do.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ttl_idx_metadata"))
	require.NoError(t, err)
	metadataCases := []struct {
		name   string
		mutate func(*model.TableInfo, *model.IndexInfo)
	}{
		{"non-public", func(_ *model.TableInfo, idx *model.IndexInfo) { idx.State = model.StateWriteOnly }},
		{"invisible", func(_ *model.TableInfo, idx *model.IndexInfo) { idx.Invisible = true }},
		{"global", func(_ *model.TableInfo, idx *model.IndexInfo) { idx.Global = true }},
		{"multi-valued", func(_ *model.TableInfo, idx *model.IndexInfo) { idx.MVIndex = true }},
		{"columnar", func(_ *model.TableInfo, idx *model.IndexInfo) { idx.VectorInfo = &model.VectorIndexInfo{} }},
		{"conditional", func(_ *model.TableInfo, idx *model.IndexInfo) { idx.ConditionExprString = "a > 0" }},
		{"without-columns", func(_ *model.TableInfo, idx *model.IndexInfo) { idx.Columns = nil }},
		{"invalid-column-offset", func(tbl *model.TableInfo, idx *model.IndexInfo) {
			idx.Columns[1].Offset = len(tbl.Columns)
		}},
		{"hidden-column", func(tbl *model.TableInfo, idx *model.IndexInfo) {
			tbl.Columns[idx.Columns[1].Offset].Hidden = true
		}},
	}
	for _, c := range metadataCases {
		t.Run(c.name, func(t *testing.T) {
			tblInfo := tb.Meta().Clone()
			require.Len(t, tblInfo.Indices, 1)
			idx := tblInfo.Indices[0]
			c.mutate(tblInfo, idx)

			ttlTbl, err := cache.NewPhysicalTable(ast.NewCIStr("test"), tblInfo, ast.NewCIStr(""))
			require.NoError(t, err)
			_, err = ttlTbl.BuildTTLIndexScanPlan(idx)
			require.Error(t, err)
			require.Nil(t, ttlTbl.FindTTLIndex())
		})
	}
}

func TestSplitIndexScanRanges(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table test.ttl_split(id int primary key, t datetime, index idx_t(t)) ttl = `t` + interval 1 day")

	tb, err := do.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("ttl_split"))
	require.NoError(t, err)
	ttlTbl, err := cache.NewPhysicalTable(ast.NewCIStr("test"), tb.Meta(), ast.NewCIStr(""))
	require.NoError(t, err)
	idx := ttlTbl.FindTTLIndex()
	require.NotNil(t, idx)

	indexKey := func(s string) []byte {
		tm, err := time.ParseInLocation(time.DateTime, s, time.UTC)
		require.NoError(t, err)
		ft := ttlTbl.TimeColumn.FieldType
		datum := types.NewTimeDatum(types.NewTime(types.FromGoTime(tm), ft.GetType(), ft.GetDecimal()))
		encoded, err := codec.EncodeKey(time.UTC, nil, datum)
		require.NoError(t, err)
		encoded = codec.EncodeInt(encoded, 1)
		return tablecodec.EncodeIndexSeekKey(ttlTbl.ID, idx.ID, encoded)
	}
	partialIndexKey := func(s string) []byte {
		tm, err := time.ParseInLocation(time.DateTime, s, time.UTC)
		require.NoError(t, err)
		ft := ttlTbl.TimeColumn.FieldType
		datum := types.NewTimeDatum(types.NewTime(types.FromGoTime(tm), ft.GetType(), ft.GetDecimal()))
		encoded, err := codec.EncodeKey(time.UTC, nil, datum)
		require.NoError(t, err)
		require.Len(t, encoded, 9)
		// A Region boundary is not required to be a complete row key. Truncate
		// the packed temporal value itself to exercise arbitrary binary splits.
		return tablecodec.EncodeIndexSeekKey(ttlTbl.ID, idx.ID, encoded[:len(encoded)-1])
	}
	minNotNullIndexKey := func() []byte {
		encoded, err := codec.EncodeKey(time.UTC, nil, types.MinNotNullDatum())
		require.NoError(t, err)
		return tablecodec.EncodeIndexSeekKey(ttlTbl.ID, idx.ID, encoded)
	}
	requireScanRange := func(r cache.ScanRange, start, end string) {
		if start == "" {
			require.Empty(t, r.Start)
		} else {
			require.Len(t, r.Start, 1)
			require.Equal(t, start, r.Start[0].GetMysqlTime().String())
		}
		if end == "" {
			require.Empty(t, r.End)
		} else {
			require.Len(t, r.End, 1)
			require.Equal(t, end, r.End[0].GetMysqlTime().String())
		}
	}

	expireTime := time.Date(2025, 5, 14, 0, 0, 0, 0, time.UTC)
	tikvStore := newMockTiKVStore(t)

	ranges, err := ttlTbl.SplitIndexScanRanges(context.TODO(), tikvStore, idx, expireTime, time.UTC, 4)
	require.NoError(t, err)
	require.Len(t, ranges, 1)
	require.Empty(t, ranges[0].Start)
	require.Empty(t, ranges[0].End)

	indexPrefix := tablecodec.EncodeIndexSeekKey(ttlTbl.ID, idx.ID, nil)
	startKey := minNotNullIndexKey()
	endKey := indexKey(expireTime.Format(time.DateTime))
	tikvStore.clearRegions()
	tikvStore.addRegion(indexPrefix, startKey)
	tikvStore.addRegion(startKey, indexKey("2020-01-01 00:00:00"))
	tikvStore.addRegion(indexKey("2020-01-01 00:00:00"), indexKey("2021-01-01 00:00:00"))
	tikvStore.addRegion(indexKey("2021-01-01 00:00:00"), indexKey("2022-01-01 00:00:00"))
	tikvStore.addRegion(indexKey("2022-01-01 00:00:00"), endKey)
	// The previous Region crosses the expire key because endKey also contains
	// a handle suffix. It must be retained, while this wholly unexpired Region
	// must not participate in subtask grouping.
	tikvStore.addRegion(endKey, indexKey("2030-01-01 00:00:00"))

	ranges, err = ttlTbl.SplitIndexScanRanges(context.TODO(), tikvStore, idx, expireTime, time.UTC, 4)
	require.NoError(t, err)
	require.Len(t, ranges, 4)
	requireScanRange(ranges[0], "", "2020-01-01 00:00:00")
	requireScanRange(ranges[1], "2020-01-01 00:00:00", "2021-01-01 00:00:00")
	requireScanRange(ranges[2], "2021-01-01 00:00:00", "2022-01-01 00:00:00")
	requireScanRange(ranges[3], "2022-01-01 00:00:00", "")

	// Every intermediate Region boundary is truncated inside the first index
	// datum. They still map to monotonic SQL boundaries instead of collapsing
	// the whole scan into one full range.
	tikvStore.clearRegions()
	tikvStore.addRegion(indexPrefix, startKey)
	tikvStore.addRegion(startKey, partialIndexKey("2020-01-01 00:00:00"))
	tikvStore.addRegion(partialIndexKey("2020-01-01 00:00:00"), partialIndexKey("2021-01-01 00:00:00"))
	tikvStore.addRegion(partialIndexKey("2021-01-01 00:00:00"), partialIndexKey("2022-01-01 00:00:00"))
	tikvStore.addRegion(partialIndexKey("2022-01-01 00:00:00"), endKey)

	ranges, err = ttlTbl.SplitIndexScanRanges(context.TODO(), tikvStore, idx, expireTime, time.UTC, 4)
	require.NoError(t, err)
	require.Len(t, ranges, 4)
	requireScanRange(ranges[0], "", "2019-12-31 23:59:59")
	requireScanRange(ranges[1], "2019-12-31 23:59:59", "2020-12-31 23:59:59")
	requireScanRange(ranges[2], "2020-12-31 23:59:59", "2021-12-31 23:59:59")
	requireScanRange(ranges[3], "2021-12-31 23:59:59", "")

	ttlTbl.TimeColumn = nil
	ranges, err = ttlTbl.SplitIndexScanRanges(context.TODO(), tikvStore, idx, expireTime, time.UTC, 4)
	require.NoError(t, err)
	require.Len(t, ranges, 1)
	require.Empty(t, ranges[0].Start)
	require.Empty(t, ranges[0].End)

	for i, tc := range []struct {
		columnType string
		loc        *time.Location
		boundary   time.Time
		expected   string
	}{
		{
			columnType: "date",
			loc:        time.UTC,
			boundary:   time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
			expected:   "2019-12-31",
		},
		{
			columnType: "datetime(6)",
			loc:        time.UTC,
			boundary:   time.Date(2020, 1, 1, 0, 0, 0, 123392000, time.UTC),
			expected:   "2020-01-01 00:00:00.123391",
		},
		{
			columnType: "timestamp(3)",
			loc:        time.FixedZone("UTC+8", 8*60*60),
			boundary:   time.Date(2020, 1, 1, 0, 0, 0, 123000000, time.FixedZone("UTC+8", 8*60*60)),
			expected:   "2020-01-01 00:00:00.122",
		},
	} {
		t.Run(tc.columnType, func(t *testing.T) {
			tableName := fmt.Sprintf("ttl_split_temporal_%d", i)
			tk.MustExec(fmt.Sprintf("create table test.%s(id int primary key, t %s, index idx_t(t)) ttl = `t` + interval 1 day", tableName, tc.columnType))

			tb, err := do.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(tableName))
			require.NoError(t, err)
			tbl, err := cache.NewPhysicalTable(ast.NewCIStr("test"), tb.Meta(), ast.NewCIStr(""))
			require.NoError(t, err)
			idx := tbl.FindTTLIndex()
			require.NotNil(t, idx)

			encodeTime := func(tm time.Time) []byte {
				ft := tbl.TimeColumn.FieldType
				datum := types.NewTimeDatum(types.NewTime(types.FromGoTime(tm), ft.GetType(), ft.GetDecimal()))
				encoded, err := codec.EncodeKey(tc.loc, nil, datum)
				require.NoError(t, err)
				return encoded
			}
			indexPrefix := tablecodec.EncodeIndexSeekKey(tbl.ID, idx.ID, nil)
			encodedBoundary := encodeTime(tc.boundary)
			require.Len(t, encodedBoundary, 9)
			partialBoundary := tablecodec.EncodeIndexSeekKey(tbl.ID, idx.ID, encodedBoundary[:len(encodedBoundary)-1])
			encodedMinNotNull, err := codec.EncodeKey(tc.loc, nil, types.MinNotNullDatum())
			require.NoError(t, err)
			startKey := tablecodec.EncodeIndexSeekKey(tbl.ID, idx.ID, encodedMinNotNull)
			expireTime := time.Date(2025, 5, 14, 0, 0, 0, 0, tc.loc)
			endKey := tablecodec.EncodeIndexSeekKey(tbl.ID, idx.ID, encodeTime(expireTime))

			tikvStore := newMockTiKVStore(t)
			tikvStore.addRegion(indexPrefix, startKey)
			tikvStore.addRegion(startKey, partialBoundary)
			tikvStore.addRegion(partialBoundary, endKey)

			ranges, err := tbl.SplitIndexScanRanges(context.TODO(), tikvStore, idx, expireTime, tc.loc, 2)
			require.NoError(t, err)
			require.Len(t, ranges, 2)
			require.Len(t, ranges[0].End, 1)
			require.Equal(t, tc.expected, ranges[0].End[0].GetMysqlTime().String())
			require.Len(t, ranges[1].Start, 1)
			require.Equal(t, tc.expected, ranges[1].Start[0].GetMysqlTime().String())

			assertBoundaryFloor := func(encoded []byte) {
				boundary := tablecodec.EncodeIndexSeekKey(tbl.ID, idx.ID, encoded)
				require.Positive(t, bytes.Compare(boundary, startKey))
				require.Negative(t, bytes.Compare(boundary, endKey))

				tikvStore.clearRegions()
				tikvStore.addRegion(indexPrefix, startKey)
				tikvStore.addRegion(startKey, boundary)
				tikvStore.addRegion(boundary, endKey)
				ranges, err := tbl.SplitIndexScanRanges(context.TODO(), tikvStore, idx, expireTime, tc.loc, 2)
				require.NoError(t, err)
				require.Len(t, ranges, 2)
				require.Len(t, ranges[0].End, 1)

				floor, err := codec.EncodeKey(tc.loc, nil, ranges[0].End[0])
				require.NoError(t, err)
				floorKey := tablecodec.EncodeIndexSeekKey(tbl.ID, idx.ID, floor)
				require.LessOrEqual(t, bytes.Compare(floorKey, boundary), 0)
			}

			// Check every non-empty truncation inside the fixed-width temporal
			// payload, not only a boundary missing its final byte.
			for cut := 2; cut < len(encodedBoundary); cut++ {
				assertBoundaryFloor(encodedBoundary[:cut])
			}

			// Region boundaries may also contain a complete 8-byte payload that
			// does not describe a legal calendar/clock value. Verify that those
			// boundaries are rounded down instead of being discarded.
			packTime := func(year, month, day, hour, minute, second, microsecond int) uint64 {
				ymd := ((uint64(year)*13+uint64(month))<<5 | uint64(day))
				hms := uint64(hour)<<12 | uint64(minute)<<6 | uint64(second)
				return ((ymd<<17 | hms) << 24) | uint64(microsecond)
			}
			for _, packed := range []uint64{
				packTime(2020, 2, 31, 0, 0, 0, 0),
				packTime(2020, 3, 1, 31, 0, 0, 0),
				packTime(2020, 3, 1, 12, 63, 0, 0),
				packTime(2020, 3, 1, 12, 30, 63, 0),
				packTime(2020, 3, 1, 12, 30, 30, 1_500_000),
			} {
				encoded := append([]byte{encodedBoundary[0]}, codec.EncodeUint(nil, packed)...)
				assertBoundaryFloor(encoded)
			}
		})
	}
}
