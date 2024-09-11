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

package executor_test

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
)

type exportExecutor interface {
	GetExecutor4Test() any
}

func TestDetachAllContexts(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.Session().GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, true)
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert into t values (1), (2), (3)")

	rs, err := tk.Exec("select * from t")
	require.NoError(t, err)
	oldExecutor := rs.(exportExecutor).GetExecutor4Test().(exec.Executor)

	drs := rs.(sqlexec.DetachableRecordSet)
	srs, ok, err := drs.TryDetach()
	require.True(t, ok)
	require.NoError(t, err)

	require.NotEqual(t, rs, srs)
	newExecutor := srs.(exportExecutor).GetExecutor4Test().(exec.Executor)

	require.NotEqual(t, oldExecutor, newExecutor)
	// Children should be different
	for i, child := range oldExecutor.AllChildren() {
		require.NotEqual(t, child, newExecutor.AllChildren()[i])
	}

	// Then execute another statement
	tk.MustQuery("select * from t limit 1").Check(testkit.Rows("1"))
	// The previous detached record set can still be used
	// check data
	chk := srs.NewChunk(nil)
	err = srs.Next(context.Background(), chk)
	require.NoError(t, err)
	require.Equal(t, 3, chk.NumRows())
	require.Equal(t, int64(1), chk.GetRow(0).GetInt64(0))
	require.Equal(t, int64(2), chk.GetRow(1).GetInt64(0))
	require.Equal(t, int64(3), chk.GetRow(2).GetInt64(0))
}

func TestAfterDetachSessionCanExecute(t *testing.T) {
	// This test shows that the session can be safely used to execute another statement after detaching.
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.Session().GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, true)
	tk.MustExec("create table t (a int)")
	for i := 0; i < 10000; i++ {
		tk.MustExec("insert into t values (?)", i)
	}

	rs, err := tk.Exec("select * from t")
	require.NoError(t, err)
	drs, ok, err := rs.(sqlexec.DetachableRecordSet).TryDetach()
	require.NoError(t, err)
	require.True(t, ok)

	// Now, the `drs` can be used concurrently with the session.
	var wg sync.WaitGroup
	var stop atomic.Bool
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < 10000; i++ {
			if stop.Load() {
				return
			}
			tk.MustQuery("select * from t where a = ?", i).Check(testkit.Rows(strconv.Itoa(i)))
		}
	}()

	chk := drs.NewChunk(nil)
	expectedSelect := 0
	for {
		err = drs.Next(context.Background(), chk)
		require.NoError(t, err)

		if chk.NumRows() == 0 {
			break
		}
		for i := 0; i < chk.NumRows(); i++ {
			require.Equal(t, int64(expectedSelect), chk.GetRow(i).GetInt64(0))
			expectedSelect++
		}
	}
	stop.Store(true)
	wg.Wait()
}

func TestDetachWithParam(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.Session().GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, true)
	tk.MustExec("create table t (a int primary key)")
	for i := 0; i < 10000; i++ {
		tk.MustExec("insert into t values (?)", i)
	}

	rs, err := tk.Exec("select * from t where a > ? and a < ?", 100, 200)
	require.NoError(t, err)
	drs, ok, err := rs.(sqlexec.DetachableRecordSet).TryDetach()
	require.NoError(t, err)
	require.True(t, ok)

	// Now, execute another statement with different size of param. It'll not affect the execution of detached executor.
	var wg sync.WaitGroup
	var stop atomic.Bool
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < 10000; i++ {
			if stop.Load() {
				return
			}
			tk.MustQuery("select * from t where a = ?", i).Check(testkit.Rows(strconv.Itoa(i)))
		}
	}()

	chk := drs.NewChunk(nil)
	expectedSelect := 101
	for {
		err = drs.Next(context.Background(), chk)
		require.NoError(t, err)

		if chk.NumRows() == 0 {
			break
		}
		for i := 0; i < chk.NumRows(); i++ {
			require.Equal(t, int64(expectedSelect), chk.GetRow(i).GetInt64(0))
			expectedSelect++
		}
	}
	stop.Store(true)
	wg.Wait()
}

func TestDetachIndexReaderAndIndexLookUp(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.Session().GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, true)
	tk.MustExec("create table t (a int, b int, c int, key idx_a_b (a,b), key idx_b (b))")
	for i := 0; i < 10000; i++ {
		tk.MustExec("insert into t values (?, ?, ?)", i, i, i)
	}

	// Test detach index reader
	tk.MustHavePlan("select a, b from t where a > 100 and a < 200", "IndexReader")
	rs, err := tk.Exec("select a, b from t where a > ? and a < ?", 100, 200)
	require.NoError(t, err)
	drs, ok, err := rs.(sqlexec.DetachableRecordSet).TryDetach()
	require.NoError(t, err)
	require.True(t, ok)

	chk := drs.NewChunk(nil)
	expectedSelect := 101
	for {
		err = drs.Next(context.Background(), chk)
		require.NoError(t, err)

		if chk.NumRows() == 0 {
			break
		}
		for i := 0; i < chk.NumRows(); i++ {
			require.Equal(t, int64(expectedSelect), chk.GetRow(i).GetInt64(0))
			require.Equal(t, int64(expectedSelect), chk.GetRow(i).GetInt64(1))
			expectedSelect++
		}
	}

	// Test detach indexLookUp
	tk.MustHavePlan("select c from t use index(idx_b) where b > 100 and b < 200", "IndexLookUp")
	rs, err = tk.Exec("select c from t where b > ? and b < ?", 100, 200)
	require.NoError(t, err)
	drs, ok, err = rs.(sqlexec.DetachableRecordSet).TryDetach()
	require.NoError(t, err)
	require.True(t, ok)

	chk = drs.NewChunk(nil)
	expectedSelect = 101
	for {
		err = drs.Next(context.Background(), chk)
		require.NoError(t, err)

		if chk.NumRows() == 0 {
			break
		}
		for i := 0; i < chk.NumRows(); i++ {
			require.Equal(t, int64(expectedSelect), chk.GetRow(i).GetInt64(0))
			expectedSelect++
		}
	}
}

func TestDetachSelection(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.Session().GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, true)
	tk.MustExec("create table t (a int, b int, c int, key idx_a_b (a,b), key idx_b (b))")
	for i := 0; i < 10000; i++ {
		tk.MustExec("insert into t values (?, ?, ?)", i, i, i)
	}

	tk.MustHavePlan("select a, b from t where c > 100 and c < 200", "Selection")
	rs, err := tk.Exec("select a, b from t where c > ? and c < ?", 100, 200)
	require.NoError(t, err)
	drs, ok, err := rs.(sqlexec.DetachableRecordSet).TryDetach()
	require.NoError(t, err)
	require.True(t, ok)

	chk := drs.NewChunk(nil)
	expectedSelect := 101
	for {
		err = drs.Next(context.Background(), chk)
		require.NoError(t, err)

		if chk.NumRows() == 0 {
			break
		}
		for i := 0; i < chk.NumRows(); i++ {
			require.Equal(t, int64(expectedSelect), chk.GetRow(i).GetInt64(0))
			require.Equal(t, int64(expectedSelect), chk.GetRow(i).GetInt64(1))
			expectedSelect++
		}
	}
	require.NoError(t, drs.Close())
	require.Equal(t, 200, expectedSelect)

	// Selection with optional property is not allowed
	tk.MustExec("set @a = 1")
	tk.MustExec("set @b = 10")
	tk.MustHavePlan("select a, b from t where a + @a + getvar('b') > 100 and a < 200", "Selection")
	rs, err = tk.Exec("select a, b from t where a + @a + getvar('b') > ? and a < ?", 100, 200)
	require.NoError(t, err)
	drs, ok, err = rs.(sqlexec.DetachableRecordSet).TryDetach()
	require.NoError(t, err)
	require.True(t, ok)

	// set user variable to another value to test the expression should not change after detaching
	tk.MustExec("set @a=100")
	tk.MustExec("set @b=1000")
	tk.MustExec("select 1")
	chk = drs.NewChunk(nil)
	expectedSelect = 90
	for {
		err = drs.Next(context.Background(), chk)
		require.NoError(t, err)

		if chk.NumRows() == 0 {
			break
		}
		for i := 0; i < chk.NumRows(); i++ {
			require.Equal(t, int64(expectedSelect), chk.GetRow(i).GetInt64(0))
			require.Equal(t, int64(expectedSelect), chk.GetRow(i).GetInt64(1))
			expectedSelect++
		}
	}
	require.NoError(t, drs.Close())
	require.Equal(t, 200, expectedSelect)

	// Selection with optional property is not allowed
	tk.MustHavePlan("select a from t where a + found_rows() > 100 and a < 200", "Selection")
	rs, err = tk.Exec("select a from t where a + found_rows() > ? and a < ?", 100, 200)
	require.NoError(t, err)
	drs, ok, _ = rs.(sqlexec.DetachableRecordSet).TryDetach()
	require.False(t, ok)
	require.Nil(t, drs)
	require.NoError(t, rs.Close())
}

func TestDetachProjection(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.Session().GetSessionVars().SetStatusFlag(mysql.ServerStatusCursorExists, true)
	tk.MustExec("create table t (a int, b int, c int, key idx_a_b (a,b), key idx_b (b))")
	for i := 0; i < 10000; i++ {
		tk.MustExec("insert into t values (?, ?, ?)", i, i, i)
	}

	tk.MustHavePlan("select a + b from t where a > 100 and a < 200", "Projection")
	rs, err := tk.Exec("select a + b from t where a > ? and a < ?", 100, 200)
	require.NoError(t, err)
	drs, ok, err := rs.(sqlexec.DetachableRecordSet).TryDetach()
	require.NoError(t, err)
	require.True(t, ok)

	chk := drs.NewChunk(nil)
	expectedSelect := 101
	for {
		err = drs.Next(context.Background(), chk)
		require.NoError(t, err)

		if chk.NumRows() == 0 {
			break
		}
		for i := 0; i < chk.NumRows(); i++ {
			require.Equal(t, int64(2*expectedSelect), chk.GetRow(i).GetInt64(0))
			expectedSelect++
		}
	}
	require.NoError(t, drs.Close())
	require.Equal(t, 200, expectedSelect)

	// Projection with optional property is not allowed
	tk.MustHavePlan("select setvar('x', a) from t where a > 100 and a < 200", "Projection")
	rs, err = tk.Exec("select setvar('x', a) from t where a > ? and a < ?", 100, 200)
	require.NoError(t, err)
	drs, ok, _ = rs.(sqlexec.DetachableRecordSet).TryDetach()
	require.False(t, ok)
	require.Nil(t, drs)
	require.NoError(t, rs.Close())

	// Projection with user variable is allowed
	tk.MustExec("set @a = 1")
	tk.MustExec("set @b = 10")
	tk.MustHavePlan("select a + b + @a + getvar('b') from t where a > 100 and a < 200", "Projection")
	rs, err = tk.Exec("select a + b + @a + getvar('b') from t where a > ? and a < ?", 100, 200)
	require.NoError(t, err)
	drs, ok, err = rs.(sqlexec.DetachableRecordSet).TryDetach()
	require.NoError(t, err)
	require.True(t, ok)
	// set user variable to another value to test the expression should not change after detaching
	tk.MustExec("set @a=100")
	tk.MustExec("set @b=1000")
	chk = drs.NewChunk(nil)
	expectedSelect = 101
	for {
		err = drs.Next(context.Background(), chk)
		require.NoError(t, err)

		if chk.NumRows() == 0 {
			break
		}
		for i := 0; i < chk.NumRows(); i++ {
			require.Equal(t, float64(2*expectedSelect+11), chk.GetRow(i).GetFloat64(0))
			expectedSelect++
		}
	}
	require.NoError(t, drs.Close())
	require.Equal(t, 200, expectedSelect)

	// Projection with Selection is also allowed
	tk.MustHavePlan("select a + b from t where c > 100 and c < 200", "Projection")
	tk.MustHavePlan("select a + b from t where c > 100 and c < 200", "Selection")
	rs, err = tk.Exec("select a + b from t where c > ? and c < ?", 100, 200)
	require.NoError(t, err)
	drs, ok, err = rs.(sqlexec.DetachableRecordSet).TryDetach()
	require.NoError(t, err)
	require.True(t, ok)

	chk = drs.NewChunk(nil)
	expectedSelect = 101
	for {
		err = drs.Next(context.Background(), chk)
		require.NoError(t, err)

		if chk.NumRows() == 0 {
			break
		}
		for i := 0; i < chk.NumRows(); i++ {
			require.Equal(t, int64(2*expectedSelect), chk.GetRow(i).GetInt64(0))
			expectedSelect++
		}
	}
	require.NoError(t, drs.Close())
	require.Equal(t, 200, expectedSelect)
}
