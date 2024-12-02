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
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/stretchr/testify/require"
)

type InjectedTest struct {
	Name        string
	Recoverable bool
	Rollback    bool
}

type FailureTest struct {
	FailpointPrefix string
	Tests           []InjectedTest
}

var truncateTests = FailureTest{
	FailpointPrefix: "truncatePart",
	Tests: []InjectedTest{
		{
			Name:        "Cancel1",
			Recoverable: false,
			Rollback:    true,
		},
		{
			Name:        "Fail1",
			Recoverable: true,
			Rollback:    true,
		},
		{
			Name:        "Fail2",
			Recoverable: true,
			Rollback:    false,
		},
		{
			Name:        "Fail3",
			Recoverable: true,
			Rollback:    false,
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
	afterRecover := testkit.Rows("1 1 1", "2 2 2", "8 8 8")
	testDDLWithInjectedErrors(t, truncateTests, create, alter, beforeDML, beforeResult, afterDML, afterResult, afterRecover, "Cancel2")
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
		`update t set b = 3, c = 3, a = 3 where c = 4`,
		`delete from t where a = 8`,
		`delete from t where b = 2`,
	}
	beforeResult := testkit.Rows("3 3 3", "6 6 6", "7 7 7", "9 9 9")
	afterDML := []string{
		`insert into t values (1,1,1),(5,5,5),(8,8,8)`,
		`update t set a = 2, b = 2, c = 2 where a = 1`,
		`update t set a = 1, b = 1, c = 1 where c = 6`,
		`update t set a = 6, b = 6, c = 6 where a = 9`,
		`delete from t where a = 5`,
		`delete from t where b = 3`,
	}
	afterResult := testkit.Rows("1 1 1", "2 2 2", "6 6 6", "7 7 7", "8 8 8")
	afterRecover := testkit.Rows("1 1 1", "2 2 2", "8 8 8")
	testDDLWithInjectedErrors(t, truncateTests, create, alter, beforeDML, beforeResult, afterDML, afterResult, afterRecover, "Fail1", "Fail2", "Fail3")
}

func testDDLWithInjectedErrors(t *testing.T, tests FailureTest, createSQL, alterSQL string, beforeDML []string, beforeResult [][]any, afterDML []string, afterRollback, afterRecover [][]any, skipTests ...string) {
TEST:
	for _, test := range tests.Tests {
		for _, skip := range skipTests {
			if test.Name == skip {
				continue TEST
			}
		}
		if test.Recoverable {
			runOneTest(t, test, true, tests.FailpointPrefix, createSQL, alterSQL, beforeDML, beforeResult, afterDML, afterRecover)
		}
		if test.Rollback {
			runOneTest(t, test, false, tests.FailpointPrefix, createSQL, alterSQL, beforeDML, beforeResult, afterDML, afterRollback)
		}
	}
}

func runOneTest(t *testing.T, test InjectedTest, recoverable bool, failpointName, createSQL, alterSQL string, beforeDML []string, beforeResult [][]any, afterDML []string, afterResult [][]any) {
	name := failpointName + test.Name
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
	tk.MustExec(createSQL)
	for _, sql := range beforeDML {
		tk.MustExec(sql + ` /* ` + name + ` */`)
	}
	tk.MustQuery(`select * from t /* ` + name + ` */`).Sort().Check(beforeResult)
	tOrg := external.GetTableByName(t, tk, "test", "t")
	idxIDs := make([]int64, 0, len(tOrg.Meta().Indices))
	for _, idx := range tOrg.Meta().Indices {
		idxIDs = append(idxIDs, idx.ID)
	}
	pids := make([]int64, 0, len(tOrg.Meta().Partition.Definitions))
	for _, def := range tOrg.Meta().Partition.Definitions {
		pids = append(pids, def.ID)
	}
	oldCreate := tk.MustQuery(`show create table t`).Rows()
	fullName := "github.com/pingcap/tidb/pkg/ddl/" + name
	term := "return(true)"
	if recoverable {
		// test that it should handle recover/retry on error
		term = "1*return(true)"
	}
	require.NoError(t, failpoint.Enable(fullName, term))
	err := tk.ExecToErr(alterSQL + " /* " + name + " */")
	require.NoError(t, failpoint.Disable(fullName))
	tt := external.GetTableByName(t, tk, "test", "t")
	pi := tt.Meta().Partition
	if recoverable {
		require.NoError(t, err)
		equal := true
		for i, pid := range pids {
			equal = equal && pid == pi.Definitions[i].ID
		}
		require.False(t, equal, name)
		return
	}
	require.Error(t, err, "failpoint "+name)
	require.ErrorContains(t, err, "Injected error by "+name)
	tk.MustQuery(`show create table t /* ` + name + ` */`).Check(oldCreate)
	require.Equal(t, len(tOrg.Meta().Partition.Definitions), len(pi.Definitions), name)
	require.Equal(t, 0, len(pi.AddingDefinitions), name)
	require.Equal(t, 0, len(pi.DroppingDefinitions), name)
	require.Equal(t, 0, len(pi.NewPartitionIDs), name)
	require.Equal(t, len(tOrg.Meta().Indices), len(tt.Meta().Indices), name)
	for i := range tOrg.Meta().Indices {
		require.Equal(t, idxIDs[i], tt.Meta().Indices[i].ID, name)
	}
	for i, pid := range pids {
		require.Equal(t, pid, tt.Meta().Partition.Definitions[i].ID, name)
	}
	tk.MustExec(`admin check table t /* ` + name + ` */`)
	tk.MustExec(`update t set b = 7 where a = 9 /* ` + name + ` */`)
	for _, sql := range afterDML {
		tk.MustExec(sql + " /* " + name + " */")
	}
	tk.MustQuery(`select * from t /* ` + name + ` */`).Sort().Check(afterResult)
	tk.MustExec(`drop table t /* ` + name + ` */`)
	// TODO: Check no rows on new partitions
	// TODO: Check TiFlash replicas
	// TODO: Check Label rules
	// TODO: Check bundles
	// TODO: Check autoIDs
	// TODO: Check delete_range tables, so no delete request for old partitions in failed alters!
}
