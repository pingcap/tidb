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
	"context"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
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
	event1 := notifier.NewCreateTableEvent(&model.TableInfo{ID: 1000, Name: pmodel.NewCIStr("t1")})
	err := notifier.PubSchemeChangeToStore(ctx, se, 1, -1, event1, s)
	require.NoError(t, err)
	event2 := notifier.NewDropTableEvent(&model.TableInfo{ID: 1001, Name: pmodel.NewCIStr("t2")})
	err = notifier.PubSchemeChangeToStore(ctx, se, 2, -1, event2, s)
	require.NoError(t, err)
	got, err := s.List(ctx, se)
	require.NoError(t, err)
	require.Len(t, got, 2)
}

func TestBasicPubSub(t *testing.T) {
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
	event1 := notifier.NewCreateTableEvent(&model.TableInfo{ID: 1000, Name: pmodel.NewCIStr("t1")})
	err := notifier.PubSchemeChangeToStore(ctx, se, 1, -1, event1, s)
	require.NoError(t, err)
	event2 := notifier.NewDropTableEvent(&model.TableInfo{ID: 1001, Name: pmodel.NewCIStr("t2#special-char?in'name")})
	err = notifier.PubSchemeChangeToStore(ctx, se, 2, -1, event2, s)
	require.NoError(t, err)
	event3 := notifier.NewDropTableEvent(&model.TableInfo{ID: 1002, Name: pmodel.NewCIStr("t3")})
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
	event1 := notifier.NewCreateTableEvent(&model.TableInfo{ID: 1000, Name: pmodel.NewCIStr("t1")})
	err := notifier.PubSchemeChangeToStore(ctx, se, 1, -1, event1, s)
	require.NoError(t, err)
	event2 := notifier.NewCreateTableEvent(&model.TableInfo{ID: 1001, Name: pmodel.NewCIStr("t2")})
	err = notifier.PubSchemeChangeToStore(ctx, se, 2, -1, event2, s)
	require.NoError(t, err)
	event3 := notifier.NewCreateTableEvent(&model.TableInfo{ID: 1002, Name: pmodel.NewCIStr("t3")})
	err = notifier.PubSchemeChangeToStore(ctx, se, 3, -1, event3, s)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		changes, err2 := s.List(ctx, se)
		require.NoError(t, err2)
		return len(changes) == 0
	}, time.Second, 50*time.Millisecond)

	require.Equal(t, []int64{1000, 1001, 1002}, *id1)
	require.Equal(t, []int64{1000, 1001, 1002}, *id2)
	require.Equal(t, []int64{1000, 1001, 1002}, *id3)

	n.OnRetireOwner()
	<-done
}

func TestPubSub(t *testing.T) {
	events := make([]*notifier.SchemaChangeEvent, 0, 32)
	eventsLock := sync.Mutex{}
	handler := func(_ context.Context, _ sessionctx.Context, c *notifier.SchemaChangeEvent) error {
		eventsLock.Lock()
		defer eventsLock.Unlock()
		events = append(events, c)
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

	require.Eventually(t, func() bool {
		eventsLock.Lock()
		defer eventsLock.Unlock()
		return len(events) == 17
	}, 5*time.Second, 500*time.Millisecond)

	tps := make([]model.ActionType, len(events))
	for i, event := range events {
		tps[i] = event.GetType()
	}
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
