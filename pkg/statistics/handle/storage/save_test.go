// Copyright 2026 PingCAP, Inc.
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

package storage_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
)

func TestInsertColStats2KVDoesNotDeadlockWithAnalyzeSave(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert into t values (1), (2)")
	tk.MustExec("analyze table t")

	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableID := tbl.Meta().ID
	tk.MustQuery("select count from mysql.stats_meta where table_id = ?", tableID).Check(testkit.Rows("2"))

	const colID int64 = 100
	col := &model.ColumnInfo{
		ID:        colID,
		Name:      ast.NewCIStr("filler1"),
		FieldType: *types.NewFieldType(mysql.TypeVarchar),
		State:     model.StatePublic,
	}
	col.FieldType.SetFlen(200)

	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseFn := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseFn)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/statistics/handle/storage/pauseBeforeInsertColStatsMetaUpdate", func() {
		close(entered)
		<-release
	})

	colStatsErr := make(chan error, 1)
	go func() {
		colStatsErr <- util.CallWithSCtx(dom.StatsHandle().SPool(), func(sctx sessionctx.Context) error {
			_, err := storage.InsertColStats2KV(util.StatsCtx, sctx, tableID, []*model.ColumnInfo{col})
			return err
		}, util.FlagWrapTxn)
	}()

	select {
	case <-entered:
	case err := <-colStatsErr:
		require.NoError(t, err)
		t.Fatal("InsertColStats2KV finished before locking the histogram row")
	case <-time.After(20 * time.Second):
		t.Fatal("InsertColStats2KV did not reach the stats_meta update")
	}

	tkA := testkit.NewTestKit(t, store)
	tkA.MustExec("set innodb_lock_wait_timeout = 15")
	analyzeDone := make(chan struct{})
	var analyzeErr error
	go func() {
		defer close(analyzeDone)
		analyzeErr = runAnalyzeStatsSave(tkA, tableID, colID)
	}()

	require.Eventually(t, func() bool {
		waiting, err := trxLockWaiting(tk)
		require.NoError(t, err)
		return waiting
	}, 15*time.Second, 20*time.Millisecond)
	releaseFn()

	select {
	case <-analyzeDone:
	case <-time.After(20 * time.Second):
		t.Fatal("analyze stats save did not finish")
	}
	require.NoError(t, analyzeErr)
	select {
	case err := <-colStatsErr:
		require.NoError(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("InsertColStats2KV did not finish")
	}

	// The analyze save runs after the add-column stats transaction commits, so its
	// histogram replace is the row that remains.
	tk.MustQuery(
		"select distinct_count, null_count from mysql.stats_histograms where table_id = ? and is_index = 0 and hist_id = ?",
		tableID, colID,
	).Check(testkit.Rows("1 0"))
}

func trxLockWaiting(tk *testkit.TestKit) (bool, error) {
	rs, err := tk.Exec("select state from information_schema.tidb_trx where state = 'LockWaiting'")
	if err != nil {
		return false, err
	}
	rows, err := sqlexec.DrainRecordSetAndClose(context.Background(), rs, 32)
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}

func runAnalyzeStatsSave(tk *testkit.TestKit, tableID, colID int64) error {
	if _, err := tk.Exec("begin pessimistic"); err != nil {
		return err
	}
	rollback := func(err error) error {
		_, _ = tk.Exec("rollback")
		return err
	}
	fakeID := -tableID
	query := fmt.Sprintf(
		"select count from mysql.stats_meta where table_id in (%s, %s) for update",
		strconv.FormatInt(fakeID, 10),
		strconv.FormatInt(tableID, 10),
	)
	rs, err := tk.Exec(query)
	if err != nil {
		return rollback(err)
	}
	if _, err = sqlexec.DrainRecordSetAndClose(context.Background(), rs, 1024); err != nil {
		return rollback(err)
	}
	replace := fmt.Sprintf(
		"replace into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, version, null_count) values (%d, 0, %d, 1, 1, 0)",
		tableID, colID,
	)
	if _, err = tk.Exec(replace); err != nil {
		return rollback(err)
	}
	if _, err = tk.Exec("commit"); err != nil {
		return err
	}
	return nil
}
