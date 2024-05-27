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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/ttl/session"
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
		tbl, err := is.TableByName(model.NewCIStr(c.db), model.NewCIStr(c.tbl))
		require.NoError(t, err)
		tblInfo := tbl.Meta()
		var physicalTbls []*cache.PhysicalTable
		if tblInfo.Partition == nil {
			ttlTbl, err := cache.NewPhysicalTable(model.NewCIStr(c.db), tblInfo, model.NewCIStr(""))
			if c.timeCol == "" {
				require.Error(t, err)
				continue
			}
			require.NoError(t, err)
			physicalTbls = append(physicalTbls, ttlTbl)
		} else {
			for _, partition := range tblInfo.Partition.Definitions {
				ttlTbl, err := cache.NewPhysicalTable(model.NewCIStr(c.db), tblInfo, partition.Name)
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
	tb, err := do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tb.Meta()
	ttlTbl, err := cache.NewPhysicalTable(model.NewCIStr("test"), tblInfo, model.NewCIStr(""))
	require.NoError(t, err)

	se := session.NewSession(tk.Session(), tk.Session(), nil)
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
	tb2, err := do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	tblInfo2 := tb2.Meta()
	ttlTbl2, err := cache.NewPhysicalTable(model.NewCIStr("test"), tblInfo2, model.NewCIStr(""))
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
