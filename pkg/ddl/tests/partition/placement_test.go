// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package partition

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/stretchr/testify/require"
)

// Test Partition By cases:
// - Schema has / has not placement
//   - Table has / has not placement
//   - Partitions has / has not placment (should not matter!)
func TestPartitionByWithPlacement(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)

	dbName := "PartitionWithPlacement"
	tk1.MustExec(`create placement policy pp1 followers=1`)
	tk1.MustExec(`create placement policy pp2 followers=2`)
	tk1.MustExec(`create placement policy pp3 followers=3`)

	type testCase struct {
		schemaPP           string
		createTableExtra   string
		alterSQL           string
		beforeTablePP      string
		beforePartitionsPP []string
		afterTablePP       string
		afterPartitionsPP  []string
		errStr             string
	}

	cases := []testCase{
		{
			"",
			"",
			`alter table t partition by range (a) (partition p0 values less than (88), partition p1 values less than (222), partition p2 values less than (288), partition pMax values less than (maxvalue))`,
			"",
			nil,
			"",
			nil,
			"",
		},
		{ // 1
			" placement policy pp1",
			"",
			`alter table t partition by range (a) (partition p0 values less than (88), partition p1 values less than (222), partition p2 values less than (288), partition pMax values less than (maxvalue))`,
			"pp1",
			nil,
			"pp1",
			nil,
			"",
		},
		{ // 2
			" placement policy pp1",
			" placement policy pp2",
			`alter table t partition by range (a) (partition p0 values less than (88), partition p1 values less than (222), partition p2 values less than (288), partition pMax values less than (maxvalue))`,
			"pp2",
			nil,
			"pp2",
			nil,
			"",
		},
		{ // 3
			"",
			" placement policy pp1",
			`alter table t partition by range (a) (partition p0 values less than (88), partition p1 values less than (222), partition p2 values less than (288), partition pMax values less than (maxvalue))`,
			"pp1",
			nil,
			"pp1",
			nil,
			"",
		}, // 4
		{
			" placement policy pp1",
			" placement policy pp2 partition by hash(a) partitions 3",
			`alter table t partition by range (a) (partition p0 values less than (88), partition p1 values less than (222), partition p2 values less than (288), partition pMax values less than (maxvalue))`,
			"pp2",
			nil,
			"pp2",
			nil,
			"",
		},
		{ // 5
			" placement policy pp1",
			" partition by hash(a) (partition p0, partition p1 placement policy pp3, partition p2)",
			`alter table t partition by range (a) (partition p0 values less than (88), partition p1 values less than (222), partition p2 values less than (288) placement policy pp3, partition pMax values less than (maxvalue))`,
			"pp1",
			[]string{"", "pp3", ""},
			"pp1",
			[]string{"", "", "pp3", ""},
			"",
		},
		{ // 6
			" placement policy pp1",
			" placement policy pp2 partition by hash(a) (partition p0, partition p1 placement policy pp3, partition p2)",
			`alter table t partition by range (a) (partition p0 values less than (88), partition p1 values less than (222), partition p2 values less than (288), partition pMax values less than (maxvalue))`,
			"pp2",
			[]string{"", "pp3", ""},
			"pp2",
			nil,
			"",
		},
		{ // 7
			" placement policy pp2",
			" placement policy pp3 partition by hash(a) (partition p0, partition p1 placement policy pp1, partition p2 placement policy pp2)",
			"alter table t placement policy pp1 partition by range(a) (partition p0 values less than (100) placement policy pp1, partition p1 values less than (200) placement policy pp2, partition p2 values less than (maxvalue))",
			"pp3",
			[]string{"", "pp1", "pp2"},
			"",
			nil,
			"[ddl:8200]Unsupported multi schema change for alter table placement",
		},
		{ // 8
			" placement policy pp2",
			" placement policy pp3 partition by hash(a) (partition p0, partition p1 placement policy pp1, partition p2 placement policy pp2)",
			"alter table t partition by range(a) (partition p0 values less than (100) placement policy pp1, partition p1 values less than (200) placement policy pp2, partition p2 values less than (maxvalue))",
			"pp3",
			[]string{"", "pp1", "pp2"},
			"pp3",
			[]string{"pp1", "pp2", ""},
			"",
		},
		{ // 9
			" placement policy pp2",
			" placement policy pp3 partition by hash(a) (partition p0, partition p1 placement policy pp1, partition p2 placement policy pp2)",
			"alter table t placement policy pp1",
			"pp3",
			[]string{"", "pp1", "pp2"},
			"pp1",
			[]string{"", "pp1", "pp2"},
			"",
		},
		{ // 10
			" placement policy pp2",
			" placement policy pp3 partition by hash(a) (partition p0, partition p1 placement policy pp1, partition p2 placement policy pp2)",
			"alter table t placement policy pp1 remove partitioning",
			"pp3",
			[]string{"", "pp1", "pp2"},
			"pp1",
			[]string{"", "pp1", "pp2"},
			"[ddl:8200]Unsupported multi schema change for alter table placement",
		},
		{ // 11
			" placement policy pp2",
			" placement policy pp3 partition by hash(a) (partition p0, partition p1 placement policy pp1, partition p2 placement policy pp2)",
			"alter table t remove partitioning",
			"pp3",
			[]string{"", "pp1", "pp2"},
			"pp3",
			nil,
			"",
		},
	}

	for testNr, currTest := range cases {
		tk1.MustExec(`use test`)
		tk1.MustExec(`drop schema if exists ` + dbName)
		tk1.MustExec(`create schema ` + dbName + currTest.schemaPP)
		tk1.MustExec(`use ` + dbName)
		tk1.MustExec(`drop table if exists t`)
		tk1.MustExec(`create table t (a int not null auto_increment primary key, b varchar(255))` + currTest.createTableExtra)
		tk1.MustExec(`insert into t (b) values ("a"),("b"),("c"),("d"),("e"),("f"),("g"),("h"),("i"),("j")`)
		tk1.MustExec(`insert into t select null, concat("from: ", t.a, " - ", t2.a, " - ", t3.a) from t, t t2, t t3 LIMIT 313`)

		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec(`use ` + dbName)
		tk3 := testkit.NewTestKit(t, store)
		tk3.MustExec(`use ` + dbName)
		tk2.MustExec(`insert into t (b) values ("before")`)

		tbl := external.GetTableByName(t, tk2, dbName, "t")
		if currTest.beforeTablePP != "" {
			require.Equal(t, currTest.beforeTablePP, tbl.Meta().PlacementPolicyRef.Name.O, "Wrong table Placement Policy in currTest %d before alter", testNr)
		} else {
			require.Nil(t, tbl.Meta().PlacementPolicyRef)
		}

		for i, partPP := range currTest.beforePartitionsPP {
			def := tbl.Meta().Partition.Definitions[i]
			ppRef := def.PlacementPolicyRef
			if partPP != "" {
				require.Equal(t, partPP, ppRef.Name.O, "currTest %d before alter, partition %s", testNr, def.Name.O)
			} else {
				require.Nil(t, ppRef, "currTest %d before alter, expected nil for partition %s", testNr, def.Name.O)
			}
		}

		if currTest.errStr != "" {
			tk1.MustContainErrMsg(currTest.alterSQL, currTest.errStr)
			continue
		}
		tk1.MustExec(currTest.alterSQL)

		tk3.MustExec(`insert into t (b) values ("after")`)

		tbl = external.GetTableByName(t, tk3, dbName, "t")
		if currTest.afterTablePP != "" {
			require.Equal(t, currTest.afterTablePP, tbl.Meta().PlacementPolicyRef.Name.O, "Wrong table Placement Policy in currTest %d after alter", testNr)
		} else {
			require.Nil(t, tbl.Meta().PlacementPolicyRef)
		}

		for i, partPP := range currTest.afterPartitionsPP {
			def := tbl.Meta().Partition.Definitions[i]
			ppRef := tbl.Meta().Partition.Definitions[i].PlacementPolicyRef
			if partPP != "" {
				require.Equal(t, partPP, ppRef.Name.O, "currTest %d after alter, partition %s", testNr, def.Name.O)
			} else {
				require.Nil(t, ppRef, "currTest %d after alter, expected nil for partition %s", testNr, def.Name.O)
			}
		}
	}
}
