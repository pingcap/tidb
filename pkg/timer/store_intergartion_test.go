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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/timer/api"
	"github.com/pingcap/tidb/pkg/timer/runtime"
	"github.com/pingcap/tidb/pkg/timer/tablestore"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/tests/v3/integration"
)

func TestMemTimerStore(t *testing.T) {
	timeutil.SetSystemTZ("Asia/Shanghai")
	store := api.NewMemoryTimerStore()
	defer store.Close()
	runTimerStoreTest(t, store)

	store = api.NewMemoryTimerStore()
	defer store.Close()
	runTimerStoreWatchTest(t, store)
}

func TestTableTimerStore(t *testing.T) {
	timeutil.SetSystemTZ("Asia/Shanghai")
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

	// test cluster time zone
	runClusterTimeZoneTest(t, timerStore, func(tz string) {
		r, err := pool.Get()
		defer pool.Put(r)
		require.NoError(t, err)
		err = r.(sessionctx.Context).GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar(context.Background(), "time_zone", tz)
		require.NoError(t, err)
	})

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

func runClusterTimeZoneTest(t *testing.T, store *api.TimerStore, setClusterTZ func(string)) {
	timerID, err := store.Create(context.Background(), &api.TimerRecord{
		TimerSpec: api.TimerSpec{
			Namespace:       "n1",
			Key:             "/path/to/testtz",
			TimeZone:        "",
			SchedPolicyType: api.SchedEventCron,
			SchedPolicyExpr: "* 1 * * *",
		},
	})
	require.NoError(t, err)
	timer, err := store.GetByID(context.Background(), timerID)
	require.NoError(t, err)
	require.Equal(t, "", timer.TimeZone)
	require.Equal(t, timeutil.SystemLocation(), timer.Location)

	setClusterTZ("UTC")
	timer, err = store.GetByID(context.Background(), timerID)
	require.NoError(t, err)
	require.Equal(t, "", timer.TimeZone)
	require.Equal(t, time.UTC, timer.Location)
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
			TimeZone:        "",
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
	recordTpl.Location = timeutil.SystemLocation()
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

	invalid.SchedPolicyExpr = "1h"
	invalid.TimeZone = "tidb"
	_, err = store.Create(ctx, invalid)
	require.ErrorContains(t, err, "Unknown or incorrect time zone: 'tidb'")

	return &recordTpl
}

func runTimerStoreUpdate(ctx context.Context, t *testing.T, store *api.TimerStore, tpl *api.TimerRecord) {
	// normal update
	orgRecord, err := store.GetByID(ctx, tpl.ID)
	require.NoError(t, err)
	require.Equal(t, "1h", tpl.SchedPolicyExpr)
	eventID := uuid.NewString()
	eventStart := time.Unix(1234567, 0)
	watermark := time.Unix(7890123, 0)
	err = store.Update(ctx, tpl.ID, &api.TimerUpdate{
		Tags:            api.NewOptionalVal([]string{"l1", "l2"}),
		TimeZone:        api.NewOptionalVal("UTC"),
		SchedPolicyExpr: api.NewOptionalVal("2h"),
		ManualRequest: api.NewOptionalVal(api.ManualRequest{
			ManualRequestID:   "req1",
			ManualRequestTime: time.Unix(123, 0),
			ManualTimeout:     time.Minute,
			ManualProcessed:   true,
			ManualEventID:     "event1",
		}),
		EventStatus: api.NewOptionalVal(api.SchedEventTrigger),
		EventID:     api.NewOptionalVal(eventID),
		EventData:   api.NewOptionalVal([]byte("eventdata1")),
		EventStart:  api.NewOptionalVal(eventStart),
		EventExtra: api.NewOptionalVal(api.EventExtra{
			EventManualRequestID: "req2",
			EventWatermark:       time.Unix(456, 0),
		}),
		Watermark:    api.NewOptionalVal(watermark),
		SummaryData:  api.NewOptionalVal([]byte("summary1")),
		CheckVersion: api.NewOptionalVal(orgRecord.Version),
		CheckEventID: api.NewOptionalVal(""),
	})
	require.NoError(t, err)
	record, err := store.GetByID(ctx, tpl.ID)
	require.NoError(t, err)
	require.NotSame(t, orgRecord, record)
	require.Greater(t, record.Version, tpl.Version)
	tpl.Version = record.Version
	tpl.TimeZone = "UTC"
	tpl.Location = time.UTC
	tpl.SchedPolicyExpr = "2h"
	tpl.Tags = []string{"l1", "l2"}
	tpl.EventStatus = api.SchedEventTrigger
	tpl.EventID = eventID
	tpl.EventData = []byte("eventdata1")
	require.Equal(t, eventStart.Unix(), record.EventStart.Unix())
	tpl.EventStart = record.EventStart
	require.Equal(t, watermark.Unix(), record.Watermark.Unix())
	tpl.Watermark = record.Watermark
	tpl.SummaryData = []byte("summary1")
	tpl.ManualRequest = api.ManualRequest{
		ManualRequestID:   "req1",
		ManualRequestTime: time.Unix(123, 0),
		ManualTimeout:     time.Minute,
		ManualProcessed:   true,
		ManualEventID:     "event1",
	}
	tpl.EventExtra = api.EventExtra{
		EventManualRequestID: "req2",
		EventWatermark:       time.Unix(456, 0),
	}
	tpl.CreateTime = tpl.CreateTime.In(time.UTC)
	require.Equal(t, *tpl, *record)

	// tags full update again
	err = store.Update(ctx, tpl.ID, &api.TimerUpdate{
		Tags: api.NewOptionalVal([]string{"l3"}),
	})
	require.NoError(t, err)
	record, err = store.GetByID(ctx, tpl.ID)
	require.NoError(t, err)
	tpl.Version = record.Version
	tpl.Tags = []string{"l3"}
	require.Equal(t, *tpl, *record)

	// update manual request
	err = store.Update(ctx, tpl.ID, &api.TimerUpdate{
		ManualRequest: api.NewOptionalVal(api.ManualRequest{
			ManualRequestID: "req3",
		}),
	})
	require.NoError(t, err)
	record, err = store.GetByID(ctx, tpl.ID)
	require.NoError(t, err)
	tpl.Version = record.Version
	tpl.ManualRequest = api.ManualRequest{
		ManualRequestID: "req3",
	}
	require.Equal(t, *tpl, *record)

	// update event extra
	err = store.Update(ctx, tpl.ID, &api.TimerUpdate{
		EventExtra: api.NewOptionalVal(api.EventExtra{
			EventManualRequestID: "req4",
		}),
	})
	require.NoError(t, err)
	record, err = store.GetByID(ctx, tpl.ID)
	require.NoError(t, err)
	tpl.Version = record.Version
	tpl.EventExtra = api.EventExtra{
		EventManualRequestID: "req4",
	}
	require.Equal(t, *tpl, *record)

	// set some to empty
	var zeroTime time.Time
	err = store.Update(ctx, tpl.ID, &api.TimerUpdate{
		TimeZone:      api.NewOptionalVal(""),
		Tags:          api.NewOptionalVal([]string(nil)),
		ManualRequest: api.NewOptionalVal(api.ManualRequest{}),
		EventStatus:   api.NewOptionalVal(api.SchedEventIdle),
		EventID:       api.NewOptionalVal(""),
		EventData:     api.NewOptionalVal([]byte(nil)),
		EventStart:    api.NewOptionalVal(zeroTime),
		EventExtra:    api.NewOptionalVal(api.EventExtra{}),
		Watermark:     api.NewOptionalVal(zeroTime),
		SummaryData:   api.NewOptionalVal([]byte(nil)),
	})
	require.NoError(t, err)
	record, err = store.GetByID(ctx, tpl.ID)
	require.NoError(t, err)
	tpl.TimeZone = ""
	tpl.Location = timeutil.SystemLocation()
	tpl.Version = record.Version
	tpl.Tags = nil
	tpl.ManualRequest = api.ManualRequest{}
	tpl.EventStatus = api.SchedEventIdle
	tpl.EventID = ""
	tpl.EventData = nil
	tpl.EventStart = zeroTime
	tpl.EventExtra = api.EventExtra{}
	tpl.Watermark = zeroTime
	tpl.SummaryData = nil
	tpl.CreateTime = tpl.CreateTime.In(tpl.Location)
	require.Equal(t, *tpl, *record)

	// err check version
	err = store.Update(ctx, tpl.ID, &api.TimerUpdate{
		SchedPolicyExpr: api.NewOptionalVal("2h"),
		CheckVersion:    api.NewOptionalVal(record.Version + 1),
	})
	require.EqualError(t, err, "timer version not match")
	record, err = store.GetByID(ctx, tpl.ID)
	require.NoError(t, err)
	require.Equal(t, *tpl, *record)

	// err check event ID
	err = store.Update(ctx, tpl.ID, &api.TimerUpdate{
		SchedPolicyExpr: api.NewOptionalVal("2h"),
		CheckEventID:    api.NewOptionalVal("aabb"),
	})
	require.EqualError(t, err, "timer event id not match")
	record, err = store.GetByID(ctx, tpl.ID)
	require.NoError(t, err)
	require.Equal(t, *tpl, *record)

	// err update
	err = store.Update(ctx, tpl.ID, &api.TimerUpdate{
		SchedPolicyExpr: api.NewOptionalVal("2x"),
	})
	require.EqualError(t, err, "schedule event configuration is not valid: invalid schedule event expr '2x': unknown unit x")
	record, err = store.GetByID(ctx, tpl.ID)
	require.NoError(t, err)
	require.Equal(t, *tpl, *record)

	err = store.Update(ctx, tpl.ID, &api.TimerUpdate{
		TimeZone: api.NewOptionalVal("invalid"),
	})
	require.ErrorContains(t, err, "Unknown or incorrect time zone: 'invalid'")
	record, err = store.GetByID(ctx, tpl.ID)
	require.NoError(t, err)
	require.Equal(t, *tpl, *record)

	err = store.Update(ctx, tpl.ID, &api.TimerUpdate{
		TimeZone: api.NewOptionalVal("tidb"),
	})
	require.ErrorContains(t, err, "Unknown or incorrect time zone: 'tidb'")
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
			Tags:            []string{"tag1", "tag2"},
		},
		EventStatus: api.SchedEventIdle,
	}

	recordTpl3 := api.TimerRecord{
		TimerSpec: api.TimerSpec{
			Namespace:       "n2",
			Key:             "/path/to/another",
			SchedPolicyType: api.SchedEventInterval,
			SchedPolicyExpr: "3h",
			Tags:            []string{"tag2", "tag3"},
		},
		EventStatus: api.SchedEventIdle,
	}

	id, err := store.Create(ctx, &recordTpl1)
	require.NoError(t, err)
	got, err := store.GetByID(ctx, id)
	require.NoError(t, err)
	recordTpl1.ID = got.ID
	recordTpl1.Location = timeutil.SystemLocation()
	recordTpl1.Version = got.Version
	recordTpl1.CreateTime = got.CreateTime

	id, err = store.Create(ctx, &recordTpl2)
	require.NoError(t, err)
	got, err = store.GetByID(ctx, id)
	require.NoError(t, err)
	recordTpl2.ID = got.ID
	recordTpl2.Version = got.Version
	recordTpl2.Location = timeutil.SystemLocation()
	recordTpl2.CreateTime = got.CreateTime

	id, err = store.Create(ctx, &recordTpl3)
	require.NoError(t, err)
	got, err = store.GetByID(ctx, id)
	require.NoError(t, err)
	recordTpl3.ID = got.ID
	recordTpl3.Version = got.Version
	recordTpl3.Location = timeutil.SystemLocation()
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

	timers, err = store.List(ctx, &api.TimerCond{
		Key: api.NewOptionalVal("/path/to/k"),
	})
	require.NoError(t, err)
	checkList([]*api.TimerRecord{}, timers)

	timers, err = store.List(ctx, &api.TimerCond{
		Namespace: api.NewOptionalVal("n2"),
		Key:       api.NewOptionalVal("/path/to/key2"),
	})
	require.NoError(t, err)
	checkList([]*api.TimerRecord{}, timers)

	timers, err = store.List(ctx, &api.TimerCond{
		Namespace: api.NewOptionalVal("n1"),
		Key:       api.NewOptionalVal("/path/to/key2"),
	})
	require.NoError(t, err)
	checkList([]*api.TimerRecord{&recordTpl2}, timers)

	timers, err = store.List(ctx, &api.TimerCond{
		Tags: api.NewOptionalVal([]string{"tag2"}),
	})
	require.NoError(t, err)
	checkList([]*api.TimerRecord{&recordTpl2, &recordTpl3}, timers)

	timers, err = store.List(ctx, &api.TimerCond{
		Tags: api.NewOptionalVal([]string{"tag1", "tag3"}),
	})
	require.NoError(t, err)
	checkList([]*api.TimerRecord{}, timers)

	timers, err = store.List(ctx, &api.TimerCond{
		Tags: api.NewOptionalVal([]string{"tag2", "tag3"}),
	})
	require.NoError(t, err)
	checkList([]*api.TimerRecord{&recordTpl3}, timers)

	timers, err = store.List(ctx, api.And(
		&api.TimerCond{Namespace: api.NewOptionalVal("n1")},
		&api.TimerCond{Tags: api.NewOptionalVal([]string{"tag2"})},
	))
	require.NoError(t, err)
	checkList([]*api.TimerRecord{&recordTpl2}, timers)

	timers, err = store.List(ctx, api.Not(api.And(
		&api.TimerCond{Namespace: api.NewOptionalVal("n1")},
		&api.TimerCond{Tags: api.NewOptionalVal([]string{"tag2"})},
	)))
	require.NoError(t, err)
	checkList([]*api.TimerRecord{&recordTpl1, &recordTpl3}, timers)

	timers, err = store.List(ctx, api.Or(
		&api.TimerCond{Key: api.NewOptionalVal("/path/to/key2")},
		&api.TimerCond{Tags: api.NewOptionalVal([]string{"tag3"})},
	))
	require.NoError(t, err)
	checkList([]*api.TimerRecord{&recordTpl2, &recordTpl3}, timers)

	timers, err = store.List(ctx, api.Not(api.Or(
		&api.TimerCond{Key: api.NewOptionalVal("/path/to/key2")},
		&api.TimerCond{Tags: api.NewOptionalVal([]string{"tag3"})},
	)))
	require.NoError(t, err)
	checkList([]*api.TimerRecord{&recordTpl1}, timers)
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
		timeout := time.NewTimer(time.Minute)
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
			case <-time.After(time.Minute):
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
			case <-time.After(time.Minute):
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

	time.Sleep(time.Second)
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
	time.Sleep(time.Second)
	notifier.Notify(api.WatchTimerEventDelete, "4")
	watcher4 := notifier.Watch(context.Background())
	time.Sleep(time.Second)
	checkWatcherClosed(watcher2, false)
	checkWatcherClosed(watcher3, true)
	checkWatcherClosed(watcher4, true)
}

type mockHook struct {
	preFunc   func(ctx context.Context, event api.TimerShedEvent) (api.PreSchedEventResult, error)
	schedFunc func(ctx context.Context, event api.TimerShedEvent) error
}

func (h *mockHook) Start() {}

func (h *mockHook) Stop() {}

func (h *mockHook) OnPreSchedEvent(ctx context.Context, event api.TimerShedEvent) (r api.PreSchedEventResult, err error) {
	if h.preFunc != nil {
		return h.preFunc(ctx, event)
	}
	return
}

func (h *mockHook) OnSchedEvent(ctx context.Context, event api.TimerShedEvent) error {
	if h.schedFunc != nil {
		return h.schedFunc(ctx, event)
	}
	return nil
}

func TestTableStoreManualTrigger(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	dbName := "test"
	tblName := "timerstore"
	tk.MustExec("use test")
	tk.MustExec(tablestore.CreateTimerTableSQL(dbName, tblName))

	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()

	timerStore := tablestore.NewTableTimerStore(1, pool, dbName, tblName, nil)
	defer timerStore.Close()

	var hookReqID atomic.Pointer[string]
	hook := &mockHook{
		preFunc: func(ctx context.Context, event api.TimerShedEvent) (r api.PreSchedEventResult, err error) {
			timer := event.Timer()
			require.False(t, timer.ManualProcessed)
			require.Empty(t, timer.ManualEventID)
			return
		},
		schedFunc: func(ctx context.Context, event api.TimerShedEvent) error {
			timer := event.Timer()
			require.Equal(t, timer.ManualRequestID, timer.EventManualRequestID)
			require.Equal(t, timer.Watermark.Unix(), timer.EventWatermark.Unix())
			require.True(t, timer.ManualProcessed)
			require.Equal(t, timer.EventID, timer.ManualEventID)
			hookReqID.Store(&timer.EventManualRequestID)
			return nil
		},
	}

	rt := runtime.NewTimerRuntimeBuilder("test", timerStore).
		RegisterHookFactory("hook1", func(hookClass string, cli api.TimerClient) api.Hook { return hook }).
		Build()

	rt.Start()
	defer rt.Stop()

	cli := api.NewDefaultTimerClient(timerStore)
	timer, err := cli.CreateTimer(context.TODO(), api.TimerSpec{
		Key:             "key1",
		HookClass:       "hook1",
		SchedPolicyType: api.SchedEventInterval,
		SchedPolicyExpr: "1h",
		Watermark:       time.Now(),
		Enable:          true,
	})
	require.NoError(t, err)

	reqID, err := cli.ManualTriggerEvent(context.TODO(), timer.ID)
	require.NoError(t, err)
	start := time.Now()
	eventID := ""
	for eventID == "" {
		if time.Since(start) > time.Minute {
			require.FailNow(t, "timeout")
		}
		time.Sleep(100 * time.Millisecond)
		timer, err = cli.GetTimerByID(context.TODO(), timer.ID)
		require.NoError(t, err)
		require.Equal(t, reqID, timer.ManualRequestID)
		eventID = timer.EventID
	}

	require.Equal(t, reqID, timer.ManualRequestID)
	require.Equal(t, eventID, timer.ManualEventID)
	require.True(t, timer.ManualProcessed)
	require.Equal(t, reqID, timer.EventManualRequestID)
	start = time.Now()
	for hookReqID.Load() == nil {
		if time.Since(start) > time.Minute {
			require.FailNow(t, "timeout")
		}
		time.Sleep(100 * time.Microsecond)
	}
	require.Equal(t, reqID, *hookReqID.Load())

	require.NoError(t, cli.CloseTimerEvent(context.TODO(), timer.ID, timer.EventID))
	timer, err = cli.GetTimerByID(context.TODO(), timer.ID)
	require.NoError(t, err)
	require.Equal(t, reqID, timer.ManualRequestID)
	require.Equal(t, eventID, timer.ManualEventID)
	require.True(t, timer.ManualProcessed)
	require.Equal(t, api.EventExtra{}, timer.EventExtra)
}

func TestTimerStoreWithTimeZone(t *testing.T) {
	// mem store
	testTimerStoreWithTimeZone(t, api.NewMemoryTimerStore(), timeutil.SystemLocation().String())

	// table store
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	dbName := "test"
	tblName := "timerstore"
	tk.MustExec("use test")
	tk.MustExec(tablestore.CreateTimerTableSQL(dbName, tblName))
	tk.MustExec("set @@time_zone = 'America/Los_Angeles'")

	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return tk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()

	timerStore := tablestore.NewTableTimerStore(1, pool, dbName, tblName, nil)
	defer timerStore.Close()

	testTimerStoreWithTimeZone(t, timerStore, timeutil.SystemLocation().String())
	tk.MustExec("set @@global.time_zone='Asia/Tokyo'")
	tk.MustExec(fmt.Sprintf("truncate table %s.%s", dbName, tblName))
	testTimerStoreWithTimeZone(t, timerStore, "Asia/Tokyo")

	// check time zone should be set back to the previous one.
	require.Equal(t, "America/Los_Angeles", tk.Session().GetSessionVars().Location().String())
}

func testTimerStoreWithTimeZone(t *testing.T, timerStore *api.TimerStore, defaultTZ string) {
	// 2024-11-03 09:30:00 UTC is 2024-11-03 01:30:00 -08:00 in `America/Los_Angeles`
	// We should notice that it should not be regarded as 2024-11-03 01:30:00 -07:00
	// because of DST these two times have the same format in time zone `America/Los_Angeles`.
	time1, err := time.ParseInLocation(time.DateTime, "2024-11-03 09:30:00", time.UTC)
	require.NoError(t, err)

	time2, err := time.ParseInLocation(time.DateTime, "2024-11-03 08:30:00", time.UTC)
	require.NoError(t, err)

	id1, err := timerStore.Create(context.TODO(), &api.TimerRecord{
		TimerSpec: api.TimerSpec{
			Namespace:       "default",
			Key:             "test1",
			SchedPolicyType: api.SchedEventInterval,
			SchedPolicyExpr: "1h",
			Watermark:       time1,
		},
		EventStatus: api.SchedEventTrigger,
		EventStart:  time2,
	})
	require.NoError(t, err)

	id2, err := timerStore.Create(context.TODO(), &api.TimerRecord{
		TimerSpec: api.TimerSpec{
			Namespace:       "default",
			Key:             "test2",
			SchedPolicyType: api.SchedEventInterval,
			SchedPolicyExpr: "1h",
			Watermark:       time2,
		},
		EventStatus: api.SchedEventTrigger,
		EventStart:  time1,
	})
	require.NoError(t, err)

	// create case
	timer1, err := timerStore.GetByID(context.TODO(), id1)
	require.NoError(t, err)
	require.Equal(t, time1.In(time.UTC).String(), timer1.Watermark.In(time.UTC).String())
	require.Equal(t, time2.In(time.UTC).String(), timer1.EventStart.In(time.UTC).String())
	checkTimerRecordLocation(t, timer1, defaultTZ)

	timer2, err := timerStore.GetByID(context.TODO(), id2)
	require.NoError(t, err)
	require.Equal(t, time2.In(time.UTC).String(), timer2.Watermark.In(time.UTC).String())
	require.Equal(t, time1.In(time.UTC).String(), timer2.EventStart.In(time.UTC).String())
	checkTimerRecordLocation(t, timer2, defaultTZ)

	// update time
	require.NoError(t, timerStore.Update(context.TODO(), id1, &api.TimerUpdate{
		Watermark:  api.NewOptionalVal(time2),
		EventStart: api.NewOptionalVal(time1),
	}))

	require.NoError(t, timerStore.Update(context.TODO(), id2, &api.TimerUpdate{
		Watermark:  api.NewOptionalVal(time1),
		EventStart: api.NewOptionalVal(time2),
	}))

	timer1, err = timerStore.GetByID(context.TODO(), id1)
	require.NoError(t, err)
	require.Equal(t, time2.In(time.UTC).String(), timer1.Watermark.In(time.UTC).String())
	require.Equal(t, time1.In(time.UTC).String(), timer1.EventStart.In(time.UTC).String())
	checkTimerRecordLocation(t, timer1, defaultTZ)

	timer2, err = timerStore.GetByID(context.TODO(), id2)
	require.NoError(t, err)
	require.Equal(t, time1.In(time.UTC).String(), timer2.Watermark.In(time.UTC).String())
	require.Equal(t, time2.In(time.UTC).String(), timer2.EventStart.In(time.UTC).String())
	checkTimerRecordLocation(t, timer2, defaultTZ)

	// update timezone
	require.NoError(t, timerStore.Update(context.TODO(), id1, &api.TimerUpdate{
		TimeZone: api.NewOptionalVal("Europe/Berlin"),
	}))

	timer1, err = timerStore.GetByID(context.TODO(), id1)
	require.NoError(t, err)
	require.Equal(t, time2.In(time.UTC).String(), timer1.Watermark.In(time.UTC).String())
	require.Equal(t, time1.In(time.UTC).String(), timer1.EventStart.In(time.UTC).String())
	checkTimerRecordLocation(t, timer1, "Europe/Berlin")

	require.NoError(t, timerStore.Update(context.TODO(), id1, &api.TimerUpdate{
		TimeZone: api.NewOptionalVal(""),
	}))

	timer1, err = timerStore.GetByID(context.TODO(), id1)
	require.NoError(t, err)
	require.Equal(t, time2.In(time.UTC).String(), timer1.Watermark.In(time.UTC).String())
	require.Equal(t, time1.In(time.UTC).String(), timer1.EventStart.In(time.UTC).String())
	checkTimerRecordLocation(t, timer1, defaultTZ)
}

func checkTimerRecordLocation(t *testing.T, r *api.TimerRecord, tz string) {
	require.Equal(t, tz, r.Location.String())
	require.Same(t, r.Location, r.Watermark.Location())
	require.Same(t, r.Location, r.CreateTime.Location())
	if !r.EventStart.IsZero() {
		require.Same(t, r.Location, r.EventStart.Location())
	}
}
