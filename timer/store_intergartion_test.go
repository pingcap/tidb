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

package timer_test

import (
	"context"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/timer/api"
	"github.com/pingcap/tidb/timer/tablestore"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/tests/v3/integration"
)

func TestMemTimerStore(t *testing.T) {
	store := api.NewMemoryTimerStore()
	defer store.Close()
	runTimerStoreTest(t, store)

	store = api.NewMemoryTimerStore()
	defer store.Close()
	runTimerStoreWatchTest(t, store)
}

func TestTableTimerStore(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	dbName := "test"
	tblName := "timerstore"
	tk.MustExec("use test")
	tk.MustExec(tablestore.CreateTimerTableSQL(dbName, tblName))

	// test CURD
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()

	timerStore := tablestore.NewTableTimerStore(1, pool, dbName, tblName, nil)
	defer timerStore.Close()
	runTimerStoreTest(t, timerStore)

	// test notifications
	integration.BeforeTestExternal(t)
	testEtcdCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer testEtcdCluster.Terminate(t)

	cli := testEtcdCluster.RandClient()
	tk.MustExec("drop table " + tblName)
	tk.MustExec(tablestore.CreateTimerTableSQL(dbName, tblName))
	timerStore = tablestore.NewTableTimerStore(1, pool, dbName, tblName, cli)
	defer timerStore.Close()
	runTimerStoreWatchTest(t, timerStore)
}

func runTimerStoreTest(t *testing.T, store *api.TimerStore) {
	ctx := context.Background()
	timer := runTimerStoreInsertAndGet(ctx, t, store)
	runTimerStoreUpdate(ctx, t, store, timer)
	runTimerStoreDelete(ctx, t, store, timer)
	runTimerStoreInsertAndList(ctx, t, store)
}

func runTimerStoreInsertAndGet(ctx context.Context, t *testing.T, store *api.TimerStore) *api.TimerRecord {
	records, err := store.List(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, records)

	recordTpl := api.TimerRecord{
		TimerSpec: api.TimerSpec{
			Namespace:       "n1",
			Key:             "/path/to/key",
			SchedPolicyType: api.SchedEventInterval,
			SchedPolicyExpr: "1h",
			Data:            []byte("data1"),
		},
	}

	// normal insert
	record := recordTpl.Clone()
	id, err := store.Create(ctx, record)
	require.NoError(t, err)
	require.Equal(t, recordTpl, *record)
	require.NotEmpty(t, id)
	recordTpl.ID = id
	recordTpl.EventStatus = api.SchedEventIdle

	// get by id
	got, err := store.GetByID(ctx, id)
	require.NoError(t, err)
	require.NotSame(t, record, got)
	record = got
	require.Equal(t, recordTpl.ID, record.ID)
	require.NotZero(t, record.Version)
	recordTpl.Version = record.Version
	require.False(t, record.CreateTime.IsZero())
	recordTpl.CreateTime = record.CreateTime
	require.Equal(t, recordTpl, *record)

	// id not exist
	_, err = store.GetByID(ctx, "noexist")
	require.True(t, errors.ErrorEqual(err, api.ErrTimerNotExist))

	// get by key
	record, err = store.GetByKey(ctx, "n1", "/path/to/key")
	require.NoError(t, err)
	require.Equal(t, recordTpl, *record)

	// key not exist
	_, err = store.GetByKey(ctx, "n1", "noexist")
	require.True(t, errors.ErrorEqual(err, api.ErrTimerNotExist))
	_, err = store.GetByKey(ctx, "n2", "/path/to/ke")
	require.True(t, errors.ErrorEqual(err, api.ErrTimerNotExist))

	// invalid insert
	invalid := &api.TimerRecord{}
	_, err = store.Create(ctx, invalid)
	require.EqualError(t, err, "field 'Namespace' should not be empty")

	invalid.Namespace = "n1"
	_, err = store.Create(ctx, invalid)
	require.EqualError(t, err, "field 'Key' should not be empty")

	invalid.Key = "k1"
	_, err = store.Create(ctx, invalid)
	require.EqualError(t, err, "field 'SchedPolicyType' should not be empty")

	invalid.SchedPolicyType = api.SchedEventInterval
	invalid.SchedPolicyExpr = "1x"
	_, err = store.Create(ctx, invalid)
	require.EqualError(t, err, "schedule event configuration is not valid: invalid schedule event expr '1x': unknown unit x")

	return &recordTpl
}

func runTimerStoreUpdate(ctx context.Context, t *testing.T, store *api.TimerStore, tpl *api.TimerRecord) {
	// normal update
	orgRecord, err := store.GetByID(ctx, tpl.ID)
	require.NoError(t, err)
	require.Equal(t, "1h", tpl.SchedPolicyExpr)
	err = store.Update(ctx, tpl.ID, &api.TimerUpdate{
		SchedPolicyExpr: api.NewOptionalVal("2h"),
	})
	require.NoError(t, err)
	record, err := store.GetByID(ctx, tpl.ID)
	require.NoError(t, err)
	require.NotSame(t, orgRecord, record)
	require.Greater(t, record.Version, tpl.Version)
	tpl.Version = record.Version
	tpl.SchedPolicyExpr = "2h"
	require.Equal(t, *tpl, *record)

	// err update
	err = store.Update(ctx, tpl.ID, &api.TimerUpdate{
		SchedPolicyExpr: api.NewOptionalVal("2x"),
	})
	require.EqualError(t, err, "schedule event configuration is not valid: invalid schedule event expr '2x': unknown unit x")
	record, err = store.GetByID(ctx, tpl.ID)
	require.NoError(t, err)
	require.Equal(t, *tpl, *record)
}

func runTimerStoreDelete(ctx context.Context, t *testing.T, store *api.TimerStore, tpl *api.TimerRecord) {
	exist, err := store.Delete(ctx, tpl.ID)
	require.NoError(t, err)
	require.True(t, exist)

	_, err = store.GetByID(ctx, tpl.ID)
	require.True(t, errors.ErrorEqual(err, api.ErrTimerNotExist))

	exist, err = store.Delete(ctx, tpl.ID)
	require.NoError(t, err)
	require.False(t, exist)
}

func runTimerStoreInsertAndList(ctx context.Context, t *testing.T, store *api.TimerStore) {
	records, err := store.List(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, records)

	recordTpl1 := api.TimerRecord{
		TimerSpec: api.TimerSpec{
			Namespace:       "n1",
			Key:             "/path/to/key1",
			SchedPolicyType: api.SchedEventInterval,
			SchedPolicyExpr: "1h",
		},
		EventStatus: api.SchedEventIdle,
	}

	recordTpl2 := api.TimerRecord{
		TimerSpec: api.TimerSpec{
			Namespace:       "n1",
			Key:             "/path/to/key2",
			SchedPolicyType: api.SchedEventInterval,
			SchedPolicyExpr: "2h",
		},
		EventStatus: api.SchedEventIdle,
	}

	recordTpl3 := api.TimerRecord{
		TimerSpec: api.TimerSpec{
			Namespace:       "n2",
			Key:             "/path/to/another",
			SchedPolicyType: api.SchedEventInterval,
			SchedPolicyExpr: "3h",
		},
		EventStatus: api.SchedEventIdle,
	}

	id, err := store.Create(ctx, &recordTpl1)
	require.NoError(t, err)
	got, err := store.GetByID(ctx, id)
	require.NoError(t, err)
	recordTpl1.ID = got.ID
	recordTpl1.Version = got.Version
	recordTpl1.CreateTime = got.CreateTime

	id, err = store.Create(ctx, &recordTpl2)
	require.NoError(t, err)
	got, err = store.GetByID(ctx, id)
	require.NoError(t, err)
	recordTpl2.ID = got.ID
	recordTpl2.Version = got.Version
	recordTpl2.CreateTime = got.CreateTime

	id, err = store.Create(ctx, &recordTpl3)
	require.NoError(t, err)
	got, err = store.GetByID(ctx, id)
	require.NoError(t, err)
	recordTpl3.ID = got.ID
	recordTpl3.Version = got.Version
	recordTpl3.CreateTime = got.CreateTime

	checkList := func(expected []*api.TimerRecord, list []*api.TimerRecord) {
		expectedMap := make(map[string]*api.TimerRecord, len(expected))
		for _, r := range expected {
			expectedMap[r.ID] = r
		}

		for _, r := range list {
			require.Contains(t, expectedMap, r.ID)
			got, ok := expectedMap[r.ID]
			require.True(t, ok)
			require.Equal(t, *got, *r)
			delete(expectedMap, r.ID)
		}

		require.Empty(t, expectedMap)
	}

	timers, err := store.List(ctx, nil)
	require.NoError(t, err)
	checkList([]*api.TimerRecord{&recordTpl1, &recordTpl2, &recordTpl3}, timers)

	timers, err = store.List(ctx, &api.TimerCond{
		Key:       api.NewOptionalVal("/path/to/k"),
		KeyPrefix: true,
	})
	require.NoError(t, err)
	checkList([]*api.TimerRecord{&recordTpl1, &recordTpl2}, timers)
}

func runTimerStoreWatchTest(t *testing.T, store *api.TimerStore) {
	require.True(t, store.WatchSupported())
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
	}()

	timer := api.TimerRecord{
		TimerSpec: api.TimerSpec{
			Namespace:       "n1",
			Key:             "/path/to/key",
			SchedPolicyType: api.SchedEventInterval,
			SchedPolicyExpr: "1h",
			Data:            []byte("data1"),
		},
	}

	ch := store.Watch(ctx)
	assertWatchEvent := func(tp api.WatchTimerEventType, id string) {
		timeout := time.NewTimer(time.Second)
		defer timeout.Stop()
		select {
		case resp, ok := <-ch:
			if id == "" {
				require.False(t, ok)
				return
			}
			require.True(t, ok)
			require.NotNil(t, resp)
			require.Equal(t, 1, len(resp.Events))
			require.Equal(t, tp, resp.Events[0].Tp)
			require.Equal(t, id, resp.Events[0].TimerID)
		case <-timeout.C:
			require.FailNow(t, "no response")
		}
	}

	id, err := store.Create(ctx, &timer)
	require.NoError(t, err)
	assertWatchEvent(api.WatchTimerEventCreate, id)

	err = store.Update(ctx, id, &api.TimerUpdate{
		SchedPolicyExpr: api.NewOptionalVal("2h"),
	})
	require.NoError(t, err)
	assertWatchEvent(api.WatchTimerEventUpdate, id)

	exit, err := store.Delete(ctx, id)
	require.NoError(t, err)
	require.True(t, exit)
	assertWatchEvent(api.WatchTimerEventDelete, id)

	cancel()
	assertWatchEvent(0, "")
}

func TestMemNotifier(t *testing.T) {
	notifier := api.NewMemTimerWatchEventNotifier()
	defer notifier.Close()
	runNotifierTest(t, notifier)
}

type multiNotifier struct {
	notifier1 api.TimerWatchEventNotifier
	notifier2 api.TimerWatchEventNotifier
}

func (n *multiNotifier) Notify(tp api.WatchTimerEventType, timerID string) {
	n.notifier1.Notify(tp, timerID)
}

func (n *multiNotifier) Watch(ctx context.Context) api.WatchTimerChan {
	return n.notifier2.Watch(ctx)
}

func (n *multiNotifier) Close() {
	n.notifier1.Close()
	n.notifier2.Close()
}

func TestEtcdNotifier(t *testing.T) {
	integration.BeforeTestExternal(t)
	testEtcdCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer testEtcdCluster.Terminate(t)

	cli := testEtcdCluster.RandClient()
	notifier := tablestore.NewEtcdNotifier(1, cli)
	defer notifier.Close()
	runNotifierTest(t, notifier)

	// test one notifier notify, the other one watch
	notifier = &multiNotifier{
		notifier1: tablestore.NewEtcdNotifier(1, cli),
		notifier2: tablestore.NewEtcdNotifier(1, cli),
	}
	defer notifier.Close()
	runNotifierTest(t, notifier)
}

func runNotifierTest(t *testing.T, notifier api.TimerWatchEventNotifier) {
	defer notifier.Close()

	checkWatcherEvents := func(ch api.WatchTimerChan, events []api.WatchTimerEvent) {
		gotEvents := make([]api.WatchTimerEvent, 0, len(events))
	loop:
		for {
			select {
			case <-time.After(time.Second):
				require.Equal(t, events, gotEvents, "wait events timeout")
				return
			case resp, ok := <-ch:
				if !ok {
					break loop
				}

				require.NotEmpty(t, resp.Events)
				for _, event := range resp.Events {
					gotEvents = append(gotEvents, *event)
				}
				if len(gotEvents) >= len(events) {
					break loop
				}
			}
		}
		require.Equal(t, events, gotEvents)
	}

	checkWatcherClosed := func(ch api.WatchTimerChan, checkNoData bool) {
		for {
			select {
			case _, ok := <-ch:
				if !ok {
					return
				}
				require.False(t, checkNoData)
			case <-time.After(time.Second):
				require.FailNow(t, "wait closed timeout")
			}
		}
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	watcher1 := notifier.Watch(ctx1)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	watcher2 := notifier.Watch(ctx2)

	notifier.Notify(api.WatchTimerEventCreate, "1")
	notifier.Notify(api.WatchTimerEventCreate, "2")
	notifier.Notify(api.WatchTimerEventUpdate, "1")
	notifier.Notify(api.WatchTimerEventDelete, "2")

	expectedEvents := []api.WatchTimerEvent{
		{
			Tp:      api.WatchTimerEventCreate,
			TimerID: "1",
		},
		{
			Tp:      api.WatchTimerEventCreate,
			TimerID: "2",
		},
		{
			Tp:      api.WatchTimerEventUpdate,
			TimerID: "1",
		},
		{
			Tp:      api.WatchTimerEventDelete,
			TimerID: "2",
		},
	}
	checkWatcherEvents(watcher1, expectedEvents)
	checkWatcherEvents(watcher2, expectedEvents)
	notifier.Notify(api.WatchTimerEventCreate, "3")
	notifier.Notify(api.WatchTimerEventUpdate, "3")
	cancel1()
	notifier.Notify(api.WatchTimerEventDelete, "3")
	notifier.Notify(api.WatchTimerEventCreate, "4")
	expectedEvents = []api.WatchTimerEvent{
		{
			Tp:      api.WatchTimerEventCreate,
			TimerID: "3",
		},
		{
			Tp:      api.WatchTimerEventUpdate,
			TimerID: "3",
		},
		{
			Tp:      api.WatchTimerEventDelete,
			TimerID: "3",
		},
		{
			Tp:      api.WatchTimerEventCreate,
			TimerID: "4",
		},
	}
	checkWatcherClosed(watcher1, false)
	checkWatcherEvents(watcher2, expectedEvents)
	notifier.Notify(api.WatchTimerEventCreate, "5")
	notifier.Close()
	watcher3 := notifier.Watch(context.Background())
	notifier.Notify(api.WatchTimerEventDelete, "4")
	watcher4 := notifier.Watch(context.Background())
	checkWatcherClosed(watcher2, false)
	checkWatcherClosed(watcher3, true)
	checkWatcherClosed(watcher4, true)
}
