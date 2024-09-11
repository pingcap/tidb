// Copyright 2024 PingCAP, Inc.
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
	"strconv"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/stretchr/testify/require"
)

type InjectedTests struct {
	Name  string
	Count int
	Error bool
}

type FailureTest struct {
	FailpointPrefix string
	Tests           []InjectedTests
}

var truncateTests = FailureTest{
	FailpointPrefix: "truncatePart",
	Tests: []InjectedTests{
		{
			Name:  "Cancel",
			Count: 2,
			Error: true,
		},
		{
			Name:  "Fail",
			Count: 3,
		},
	},
}

func TestTruncatePartitionListFailuresWithGlobalIndex(t *testing.T) {
	create := `create table t (a int unsigned primary key nonclustered global, b int not null, c varchar(255), unique index (c) global) partition by list(b) (
                        partition p0 values in (1,2,3),
                        partition p1 values in (4,5,6),
                        partition p2 values in (7,8,9))`
	alter := `alter table t truncate partition p0,p2`
	beforeDML := []string{
		`insert into t values (1,1,1),(2,2,2),(4,4,4),(8,8,8),(9,9,9),(6,6,6)`,
		`update t set a = 7, b = 7, c = 7 where a = 1`,
		`update t set b = 3, c = 3 where c = 4`,
		`delete from t where a = 8`,
		`delete from t where b = 2`,
	}
	beforeResult := testkit.Rows("4 3 3", "6 6 6", "7 7 7", "9 9 9")
	afterDML := []string{
		`insert into t values (1,1,1),(5,5,5),(8,8,8)`,
		`update t set a = 2, b = 2, c = 2 where a = 1`,
		`update t set a = 1, b = 1, c = 1 where c = 6`,
		`update t set a = 6, b = 6 where a = 9`,
		`delete from t where a = 5`,
		`delete from t where b = 3`,
	}
	afterResult := testkit.Rows("1 1 1", "2 2 2", "6 6 9", "7 7 7", "8 8 8")
	testReorganizePartitionFailures(t, truncateTests, create, alter, beforeDML, beforeResult, afterDML, afterResult, "Cancel2", "Fail1", "Fail2", "Fail3")
	afterResult = testkit.Rows("1 1 1", "2 2 2", "8 8 8")
	testReorganizePartitionFailures(t, truncateTests, create, alter, beforeDML, beforeResult, afterDML, afterResult, "Cancel1", "Cancel2")
}

func TestTruncatePartitionListFailures(t *testing.T) {
	create := `create table t (a int unsigned primary key, b int not null, c varchar(255)) partition by list(a) (
                        partition p0 values in (1,2,3),
                        partition p1 values in (4,5,6),
                        partition p2 values in (7,8,9))`
	alter := `alter table t truncate partition p0,p2`
	beforeDML := []string{
		`insert into t values (1,1,1),(2,2,2),(4,4,4),(8,8,8),(9,9,9),(6,6,6)`,
		`update t set a = 7, b = 7, c = 7 where a = 1`,
		`update t set b = 3, c = 3 where c = 4`,
		`delete from t where a = 8`,
		`delete from t where b = 2`,
	}
	beforeResult := testkit.Rows("4 3 3", "6 6 6", "7 7 7", "9 9 9")
	afterDML := []string{
		`insert into t values (1,1,1),(5,5,5),(8,8,8)`,
		`update t set a = 2, b = 2, c = 2 where a = 1`,
		`update t set a = 1, b = 1, c = 1 where c = 6`,
		`update t set a = 6, b = 6 where a = 9`,
		`delete from t where a = 5`,
		`delete from t where b = 3`,
	}
	afterResult := testkit.Rows("1 1 1", "2 2 2", "6 6 9", "7 7 7", "8 8 8")
	testReorganizePartitionFailures(t, truncateTests, create, alter, beforeDML, beforeResult, afterDML, afterResult, "Fail1", "Fail2", "Fail3")
	afterResult = testkit.Rows("1 1 1", "2 2 2", "8 8 8")
	testReorganizePartitionFailures(t, truncateTests, create, alter, beforeDML, beforeResult, afterDML, afterResult, "Cancel1", "Cancel2")
}

func testReorganizePartitionFailures(t *testing.T, tests FailureTest, createSQL, alterSQL string, beforeDML []string, beforeResult [][]any, afterDML []string, afterResult [][]any, skipTests ...string) {
	testkit.SkipIfFailpointDisabled(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_global_index=true")
	defer func() {
		tk.MustExec("set tidb_enable_global_index=default")
	}()
	oldWaitTimeWhenErrorOccurred := ddl.WaitTimeWhenErrorOccurred
	defer func() {
		ddl.WaitTimeWhenErrorOccurred = oldWaitTimeWhenErrorOccurred
	}()
	ddl.WaitTimeWhenErrorOccurred = 0
	for _, test := range tests.Tests {
	SUBTEST:
		for i := 1; i <= test.Count; i++ {
			suffix := test.Name + strconv.Itoa(i)
			for _, skip := range skipTests {
				if suffix == skip {
					continue SUBTEST
				}
			}
			tk.MustExec(createSQL)
			for _, sql := range beforeDML {
				tk.MustExec(sql + ` /* ` + suffix + ` */`)
			}
			tk.MustQuery(`select * from t /* ` + suffix + ` */`).Sort().Check(beforeResult)
			tOrg := external.GetTableByName(t, tk, "test", "t")
			idxIDs := make([]int64, 0, len(tOrg.Meta().Indices))
			for _, idx := range tOrg.Meta().Indices {
				idxIDs = append(idxIDs, idx.ID)
			}
			oldCreate := tk.MustQuery(`show create table t`).Rows()
			name := "github.com/pingcap/tidb/pkg/ddl/" + tests.FailpointPrefix + suffix
			// Test to inject failure up to 2 times, Truncate should recover
			// Note that truncate cannot rollback!
			require.NoError(t, failpoint.Enable(name, `2*return(true)`))
			err := tk.ExecToErr(alterSQL)
			if test.Error {
				require.Error(t, err, "failpoint "+tests.FailpointPrefix+suffix)
				require.ErrorContains(t, err, "Injected error by "+tests.FailpointPrefix+suffix)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, failpoint.Disable(name))
			tk.MustQuery(`show create table t /* ` + suffix + ` */`).Check(oldCreate)
			tt := external.GetTableByName(t, tk, "test", "t")
			partition := tt.Meta().Partition
			require.Equal(t, len(tOrg.Meta().Partition.Definitions), len(partition.Definitions), suffix)
			require.Equal(t, 0, len(partition.AddingDefinitions), suffix)
			require.Equal(t, 0, len(partition.DroppingDefinitions), suffix)
			require.Equal(t, 0, len(partition.NewPartitionIDs), suffix)
			require.Equal(t, len(tOrg.Meta().Indices), len(tt.Meta().Indices), suffix)
			for i := range tOrg.Meta().Indices {
				require.Equal(t, idxIDs[i], tt.Meta().Indices[i].ID, suffix)
			}
			tk.MustExec(`admin check table t /* ` + suffix + ` */`)
			for _, sql := range afterDML {
				tk.MustExec(sql + " /* " + suffix + " */")
			}
			tk.MustQuery(`select * from t /* ` + suffix + ` */`).Sort().Check(afterResult)
			tk.MustExec(`drop table t /* ` + suffix + ` */`)
			// TODO: Check no rows on new partitions
			// TODO: Check TiFlash replicas
			// TODO: Check Label rules
			// TODO: Check bundles
			// TODO: Check autoIDs
		}
	}
}
