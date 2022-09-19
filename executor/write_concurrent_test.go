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

package executor_test

import (
	"context"
	"math/rand"
	"testing"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestBatchInsertWithOnDuplicate(t *testing.T) {
	t.Parallel()

	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewAsyncTestKit(t, store)
	// prepare schema.
	ctx := tk.OpenSession(context.Background(), "test")
	defer tk.CloseSession(ctx)
	tk.MustExec(ctx, "drop table if exists duplicate_test")
	tk.MustExec(ctx, "create table duplicate_test(id int auto_increment, k1 int, primary key(id), unique key uk(k1))")
	tk.MustExec(ctx, "insert into duplicate_test(k1) values(?),(?),(?),(?),(?)", permInt(5)...)

	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableBatchDML = true
	})

	tk.ConcurrentRun(
		3,
		2,
		// prepare data for each loop.
		func(ctx context.Context, tk *testkit.AsyncTestKit, concurrent int, currentLoop int) [][][]interface{} {
			var ii [][][]interface{}
			for i := 0; i < concurrent; i++ {
				ii = append(ii, [][]interface{}{permInt(7)})
			}
			return ii
		},
		// concurrent execute logic.
		func(ctx context.Context, tk *testkit.AsyncTestKit, input [][]interface{}) {
			tk.MustExec(ctx, "set @@session.tidb_batch_insert=1")
			tk.MustExec(ctx, "set @@session.tidb_dml_batch_size=1")
			_, _ = tk.Exec(ctx, "insert ignore into duplicate_test(k1) values (?),(?),(?),(?),(?),(?),(?)", input[0]...)
		},
		// check after all done.
		func(ctx context.Context, tk *testkit.AsyncTestKit) {
			tk.MustExec(ctx, "admin check table duplicate_test")
			tk.MustQuery(ctx, "select d1.id, d1.k1 from duplicate_test d1 ignore index(uk), duplicate_test d2 use index (uk) where d1.id = d2.id and d1.k1 <> d2.k1").Check(testkit.Rows())
		})
}

func permInt(n int) []interface{} {
	randPermSlice := rand.Perm(n)
	v := make([]interface{}, 0, len(randPermSlice))
	for _, i := range randPermSlice {
		v = append(v, i)
	}
	return v
}

func TestConstraintCheckForOptimisticUntouched(t *testing.T) {
	t.Parallel()
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists test_optimistic_untouched_flag;")
	tk.MustExec(`create table test_optimistic_untouched_flag(c0 int, c1 varchar(20), c2 varchar(20), unique key uk(c0));`)
	tk.MustExec(`insert into test_optimistic_untouched_flag(c0, c1, c2) values (1, null, 'green');`)

	// Insert a row with duplicated entry on the unique key, the commit should fail.
	tk.MustExec("begin optimistic;")
	tk.MustExec(`insert into test_optimistic_untouched_flag(c0, c1, c2) values (1, 'red', 'white');`)
	tk.MustExec(`delete from test_optimistic_untouched_flag where c1 is null;`)
	tk.MustExec("update test_optimistic_untouched_flag set c2 = 'green' where c2 between 'purple' and 'white';")
	_, err := tk.Exec("commit")
	require.Error(t, err)

	tk.MustExec("begin optimistic;")
	tk.MustExec(`insert into test_optimistic_untouched_flag(c0, c1, c2) values (1, 'red', 'white');`)
	tk.MustExec("update test_optimistic_untouched_flag set c2 = 'green' where c2 between 'purple' and 'white';")
	_, err = tk.Exec("commit")
	require.Error(t, err)
}
