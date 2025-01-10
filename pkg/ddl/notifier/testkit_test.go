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

package notifier_test

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestPublishToTableStore(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	t.Cleanup(func() {
		tk.MustExec("TRUNCATE mysql." + ddl.NotifierTableName)
	})

	ctx := context.Background()
	s := notifier.OpenTableStore("mysql", ddl.NotifierTableName)
	se := sess.NewSession(tk.Session())
	event1 := notifier.NewCreateTableEvent(&model.TableInfo{ID: 1000, Name: ast.NewCIStr("t1")})
	err := notifier.PubSchemeChangeToStore(ctx, se, 1, -1, event1, s)
	require.NoError(t, err)
	event2 := notifier.NewDropTableEvent(&model.TableInfo{ID: 1001, Name: ast.NewCIStr("t2")})
	err = notifier.PubSchemeChangeToStore(ctx, se, 2, -1, event2, s)
	require.NoError(t, err)
	changes := make([]*notifier.SchemaChange, 8)
	result, closeFn := s.List(ctx, se)
	n, err := result.Read(changes)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	closeFn()
}

func TestBasicPubSub(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")
	tk.MustExec("DROP TABLE IF EXISTS " + ddl.NotifierTableName)
	tk.MustExec(ddl.NotifierTableSQL)

	s := notifier.OpenTableStore("test", ddl.NotifierTableName)
	sessionPool := util.NewSessionPool(
		2,
		func() (pools.Resource, error) {
			return testkit.NewTestKit(t, store).Session(), nil
		},
		nil,
		nil,
	)

	n := notifier.NewDDLNotifier(sessionPool, s, 50*time.Millisecond)

	var seenChangesMu sync.Mutex
	seenChanges := make([]*notifier.SchemaChangeEvent, 0, 8)
	injectedErrors := []error{
		nil,                            // received event1
		notifier.ErrNotReadyRetryLater, // event2 will be retried
		nil,                            // received event2 (should not receive event3)
		notifier.ErrNotReadyRetryLater,
		io.EOF,
	}
	testHandler := func(_ context.Context, _ sessionctx.Context, c *notifier.SchemaChangeEvent) error {
		var err error
		if len(injectedErrors) > 0 {
			err = injectedErrors[0]
			injectedErrors = injectedErrors[1:]
		}
		if err != nil {
			return err
		}

		seenChangesMu.Lock()
		defer seenChangesMu.Unlock()
		seenChanges = append(seenChanges, c)
		return nil
	}
	n.RegisterHandler(notifier.TestHandlerID, testHandler)

	done := make(chan struct{})
	go func() {
		n.OnBecomeOwner()
		close(done)
	}()

	tk2 := testkit.NewTestKit(t, store)
	se := sess.NewSession(tk2.Session())
	ctx := context.Background()
	event1 := notifier.NewCreateTableEvent(&model.TableInfo{ID: 1000, Name: ast.NewCIStr("t1")})
	err := notifier.PubSchemeChangeToStore(ctx, se, 1, -1, event1, s)
	require.NoError(t, err)
	event2 := notifier.NewDropTableEvent(&model.TableInfo{ID: 1001, Name: ast.NewCIStr("t2#special-char?in'name")})
	err = notifier.PubSchemeChangeToStore(ctx, se, 2, -1, event2, s)
	require.NoError(t, err)
	event3 := notifier.NewDropTableEvent(&model.TableInfo{ID: 1002, Name: ast.NewCIStr("t3")})
	err = notifier.PubSchemeChangeToStore(ctx, se, 3, -1, event3, s)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		seenChangesMu.Lock()
		defer seenChangesMu.Unlock()
		return len(seenChanges) == 3
	}, time.Second, 25*time.Millisecond)

	require.Equal(t, event1, seenChanges[0])
	require.Equal(t, event2, seenChanges[1])
	require.Equal(t, event3, seenChanges[2])
	n.OnRetireOwner()
	<-done
}

func TestDeliverOrderAndCleanup(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")
	tk.MustExec("DROP TABLE IF EXISTS " + ddl.NotifierTableName)
	tk.MustExec(ddl.NotifierTableSQL)

	s := notifier.OpenTableStore("test", ddl.NotifierTableName)
	sessionPool := util.NewSessionPool(
		1,
		func() (pools.Resource, error) {
			return tk.Session(), nil
		},
		nil,
		nil,
	)
	n := notifier.NewDDLNotifier(sessionPool, s, 50*time.Millisecond)

	newRndFailHandler := func() (notifier.SchemaChangeHandler, *[]int64) {
		maxFail := 5
		tableIDs := make([]int64, 0, 8)
		h := func(
			_ context.Context,
			_ sessionctx.Context,
			change *notifier.SchemaChangeEvent,
		) error {
			if maxFail > 0 {
				if rand.Int63n(2) == 0 {
					maxFail--
					return notifier.ErrNotReadyRetryLater
				}
			}

			tableIDs = append(tableIDs, change.GetCreateTableInfo().ID)
			return nil
		}
		return h, &tableIDs
	}

	h1, id1 := newRndFailHandler()
	h2, id2 := newRndFailHandler()
	h3, id3 := newRndFailHandler()
	n.RegisterHandler(3, h1)
	n.RegisterHandler(4, h2)
	n.RegisterHandler(9, h3)

	done := make(chan struct{})
	go func() {
		n.OnBecomeOwner()
		close(done)
	}()

	tk2 := testkit.NewTestKit(t, store)
	se := sess.NewSession(tk2.Session())
	ctx := context.Background()
	event1 := notifier.NewCreateTableEvent(&model.TableInfo{ID: 1000, Name: ast.NewCIStr("t1")})
	err := notifier.PubSchemeChangeToStore(ctx, se, 1, -1, event1, s)
	require.NoError(t, err)
	event2 := notifier.NewCreateTableEvent(&model.TableInfo{ID: 1001, Name: ast.NewCIStr("t2")})
	err = notifier.PubSchemeChangeToStore(ctx, se, 2, -1, event2, s)
	require.NoError(t, err)
	event3 := notifier.NewCreateTableEvent(&model.TableInfo{ID: 1002, Name: ast.NewCIStr("t3")})
	err = notifier.PubSchemeChangeToStore(ctx, se, 3, -1, event3, s)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		changes := make([]*notifier.SchemaChange, 8)
		result, closeFn := s.List(ctx, se)
		count, err2 := result.Read(changes)
		require.NoError(t, err2)
		closeFn()
		return count == 0
	}, time.Second, 50*time.Millisecond)

	require.Equal(t, []int64{1000, 1001, 1002}, *id1)
	require.Equal(t, []int64{1000, 1001, 1002}, *id2)
	require.Equal(t, []int64{1000, 1001, 1002}, *id3)

	n.OnRetireOwner()
	<-done
}

func TestPubSub(t *testing.T) {
	tps := make([]model.ActionType, 0, 32)
	tpsLock := sync.Mutex{}
	handler := func(_ context.Context, _ sessionctx.Context, c *notifier.SchemaChangeEvent) error {
		tpsLock.Lock()
		defer tpsLock.Unlock()
		tps = append(tps, c.GetType())
		return nil
	}
	testfailpoint.EnableCall(
		t,
		"github.com/pingcap/tidb/pkg/domain/afterDDLNotifierCreated",
		func(registry *notifier.DDLNotifier) {
			registry.RegisterHandler(notifier.TestHandlerID, handler)
		},
	)

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("USE test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int)")                                                                                                // ActionCreateTable
	tk.MustExec("alter table t partition by range(a) (partition p1 values less than (20))")                                              // ActionAlterTablePartitioning
	tk.MustExec("alter table t reorganize partition p1 into (partition p11 values less than (10), partition p12 values less than (20))") // ActionReorganizePartition
	tk.MustExec("alter table t truncate partition p11")                                                                                  // ActionTruncateTablePartition
	tk.MustExec("alter table t drop partition p11")                                                                                      // ActionDropTablePartition
	tk.MustExec("alter table t add partition(partition p13 values less than (30))")                                                      // ActionAddTablePartition
	tk.MustExec("create table t1 (a int)")                                                                                               // ActionCreateTable
	tk.MustExec("ALTER TABLE t EXCHANGE PARTITION p12 WITH TABLE t1")                                                                    // ActionExchangeTablePartition
	tk.MustExec("alter table t remove partitioning")                                                                                     // ActionRemovePartitioning
	tk.MustExec("truncate table t")                                                                                                      // ActionTruncateTable
	tk.MustExec("drop table t1")                                                                                                         // ActionDropTable
	tk.MustExec("alter table t modify column a varchar(15)")                                                                             // ActionModifyColumn
	tk.MustExec("alter table t add column b int")                                                                                        // ActionAddColumn
	tk.MustExec("alter table t add index(b)")
	tk.MustExec("create table t1(a int, b int key, FOREIGN KEY (b) REFERENCES t(b) ON DELETE CASCADE);") // ActionCreateTable with foreign key
	tk.MustExec("alter table t1 add column c int, add index idx_a(a)")                                   // ActionAddColumn
	tk.MustExec("drop database test")                                                                    // ActionDropSchema

	require.Eventually(t, func() bool {
		tpsLock.Lock()
		defer tpsLock.Unlock()
		return len(tps) == 18
	}, 5*time.Second, 500*time.Millisecond)

	require.Equal(t, []model.ActionType{
		model.ActionCreateTable,
		model.ActionAlterTablePartitioning,
		model.ActionReorganizePartition,
		model.ActionTruncateTablePartition,
		model.ActionDropTablePartition,
		model.ActionAddTablePartition,
		model.ActionCreateTable,
		model.ActionExchangeTablePartition,
		model.ActionRemovePartitioning,
		model.ActionTruncateTable,
		model.ActionDropTable,
		model.ActionModifyColumn,
		model.ActionAddColumn,
		model.ActionAddIndex,
		model.ActionCreateTable,
		model.ActionAddColumn,
		model.ActionAddIndex,
		model.ActionDropSchema,
	}, tps)
}

func TestPublishEventError(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")
	cases := []string{
		// todo: will add more case after issue 56634 fixed
		"create table t (a int)", // ActionCreateTable
	}

	err := "[ddl:-1]DDL job rollback, error msg: mock publish event error"
	tk.MustExec("set global tidb_ddl_error_count_limit = 3")
	tk.MustExec("drop table if exists t")
	for _, sql := range cases {
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/asyncNotifyEventError", "return()")
		tk.MustGetErrMsg(sql, err)
		testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/ddl/asyncNotifyEventError")

		tk.MustExec(sql)
	}
}

func Test2OwnerForAShortTime(t *testing.T) {
	conf := new(log.Config)
	logFilename := path.Join(t.TempDir(), "/test2OwnerForAShortTime.log")
	conf.File.Filename = logFilename
	lg, p, e := log.InitLogger(conf)
	require.NoError(t, e)
	rs := log.ReplaceGlobals(lg, p)
	defer rs()

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")
	tk.MustExec("DROP TABLE IF EXISTS " + ddl.NotifierTableName)
	tk.MustExec(ddl.NotifierTableSQL)
	tk.MustExec("CREATE TABLE result (id INT PRIMARY KEY)")

	s := notifier.OpenTableStore("test", ddl.NotifierTableName)
	sessionPool := util.NewSessionPool(
		1,
		func() (pools.Resource, error) {
			return tk.Session(), nil
		},
		nil,
		nil,
	)

	n := notifier.NewDDLNotifier(sessionPool, s, 50*time.Millisecond)
	waitCh := make(chan struct{})
	waitCh2 := make(chan struct{})

	testHandler := func(ctx context.Context, se sessionctx.Context, c *notifier.SchemaChangeEvent) error {
		close(waitCh)
		// mimic other owner will handle this event, wait for another session to update
		// the processed_by_flag.
		<-waitCh2
		_, err := se.GetSQLExecutor().Execute(ctx, "INSERT INTO test.result VALUES(1)")
		require.NoError(t, err)
		return nil
	}
	n.RegisterHandler(notifier.TestHandlerID, testHandler)

	done := make(chan struct{})
	go func() {
		n.OnBecomeOwner()
		close(done)
	}()

	tk2 := testkit.NewTestKit(t, store)
	se := sess.NewSession(tk2.Session())
	ctx := context.Background()
	event1 := notifier.NewCreateTableEvent(&model.TableInfo{ID: 1000, Name: ast.NewCIStr("t1")})
	err := notifier.PubSchemeChangeToStore(ctx, se, 1, -1, event1, s)
	require.NoError(t, err)

	<-waitCh
	// mimic another owner to handle the event, which is delete the record
	tk2.MustExec("DELETE FROM test." + ddl.NotifierTableName)
	close(waitCh2)

	require.Eventually(t, func() bool {
		content, err2 := os.ReadFile(logFilename)
		require.NoError(t, err2)
		if !bytes.Contains(content, []byte("Error processing change")) {
			return false
		}
		return bytes.Contains(content, []byte("Write conflict"))
	}, time.Second, 25*time.Millisecond)
	// the handler should not commit
	tk2.MustQuery("SELECT * FROM test.result").Check(testkit.Rows())

	n.OnRetireOwner()
	<-done
}

func TestPaginatedList(t *testing.T) {
	backup := notifier.ProcessEventsBatchSize
	notifier.ProcessEventsBatchSize = 3
	t.Cleanup(func() {
		notifier.ProcessEventsBatchSize = backup
	})

	names := make([]string, 0, 32)
	namesLock := sync.Mutex{}
	handler := func(_ context.Context, _ sessionctx.Context, c *notifier.SchemaChangeEvent) error {
		namesLock.Lock()
		defer namesLock.Unlock()
		switch c.GetType() {
		case model.ActionCreateTable:
			names = append(names, c.GetCreateTableInfo().Name.O)
		case model.ActionAddColumn:
			_, colInfo := c.GetAddColumnInfo()
			names = append(names, colInfo[0].Name.O)
		default:
			t.Fatalf("unexpected event type: %s", c.GetType().String())
		}
		return nil
	}

	blocking := atomic.NewBool(true)
	count := atomic.NewInt32(0)
	blockingHandler := func(context.Context, sessionctx.Context, *notifier.SchemaChangeEvent) error {
		if blocking.Load() {
			return notifier.ErrNotReadyRetryLater
		}
		count.Inc()
		return nil
	}

	testfailpoint.EnableCall(
		t,
		"github.com/pingcap/tidb/pkg/domain/afterDDLNotifierCreated",
		func(registry *notifier.DDLNotifier) {
			registry.RegisterHandler(notifier.TestHandlerID, handler)
			registry.RegisterHandler(10, blockingHandler)
		},
	)

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("USE test")
	tk.MustExec("create table t1 (a int)")
	tk.MustExec("create table t2 (a int)")
	tk.MustExec("create table t3 (a int)")
	tk.MustExec("create table t4 (a int)")
	tk.MustExec("alter table t1 add column c5 int, add column c6 int, add column c7 int, add column c8 int")

	require.Eventually(t, func() bool {
		namesLock.Lock()
		defer namesLock.Unlock()
		return len(names) == 8
	}, 5*time.Second, 500*time.Millisecond)

	require.Equal(t, []string{"t1", "t2", "t3", "t4", "c5", "c6", "c7", "c8"}, names)

	blocking.Store(false)
	require.Eventually(t, func() bool {
		return count.Load() == 8
	}, 5*time.Second, 500*time.Millisecond)
}
