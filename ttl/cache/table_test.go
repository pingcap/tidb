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

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/ttl/session"
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

func TestEvalTTLExpireTime(t *testing.T) {
	store, do := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create table test.t(a int, t datetime) ttl = `t` + interval 1 day")
	tk.MustExec("create table test.t2(a int, t datetime) ttl = `t` + interval 3 month")

	tb, err := do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tb.Meta()
	ttlTbl, err := cache.NewPhysicalTable(model.NewCIStr("test"), tblInfo, model.NewCIStr(""))
	require.NoError(t, err)

	tb2, err := do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	tblInfo2 := tb2.Meta()
	ttlTbl2, err := cache.NewPhysicalTable(model.NewCIStr("test"), tblInfo2, model.NewCIStr(""))
	require.NoError(t, err)

	se := session.NewSession(tk.Session(), tk.Session(), nil)

	now := time.UnixMilli(0)
	tz1, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	tz2, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)

	se.GetSessionVars().TimeZone = tz1
	tm, err := ttlTbl.EvalExpireTime(context.TODO(), se, now)
	require.NoError(t, err)
	require.Equal(t, now.Add(-time.Hour*24).Unix(), tm.Unix())
	require.Equal(t, "1969-12-31 08:00:00", tm.Format("2006-01-02 15:04:05"))
	require.Equal(t, tz1.String(), tm.Location().String())

	se.GetSessionVars().TimeZone = tz2
	tm, err = ttlTbl.EvalExpireTime(context.TODO(), se, now)
	require.NoError(t, err)
	require.Equal(t, now.Add(-time.Hour*24).Unix(), tm.Unix())
	require.Equal(t, "1969-12-31 01:00:00", tm.Format("2006-01-02 15:04:05"))
	require.Equal(t, tz2.String(), tm.Location().String())

	se.GetSessionVars().TimeZone = tz1
	tm, err = ttlTbl2.EvalExpireTime(context.TODO(), se, now)
	require.NoError(t, err)
	require.Equal(t, "1969-10-01 08:00:00", tm.Format("2006-01-02 15:04:05"))
	require.Equal(t, tz1.String(), tm.Location().String())

	se.GetSessionVars().TimeZone = tz2
	tm, err = ttlTbl2.EvalExpireTime(context.TODO(), se, now)
	require.NoError(t, err)
	require.Equal(t, "1969-10-01 01:00:00", tm.Format("2006-01-02 15:04:05"))
	require.Equal(t, tz2.String(), tm.Location().String())
}
