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

package api

import (
	"context"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestFieldOptional(t *testing.T) {
	var opt1 OptionalVal[string]
	require.False(t, opt1.Present())
	s, ok := opt1.Get()
	require.False(t, ok)
	require.Equal(t, "", s)

	opt1.Set("a1")
	require.True(t, opt1.Present())
	s, ok = opt1.Get()
	require.True(t, ok)
	require.Equal(t, "a1", s)

	opt1.Set("a2")
	require.True(t, opt1.Present())
	s, ok = opt1.Get()
	require.True(t, ok)
	require.Equal(t, "a2", s)

	opt1.Clear()
	require.False(t, opt1.Present())
	s, ok = opt1.Get()
	require.False(t, ok)
	require.Equal(t, "", s)

	type Foo struct {
		v int
	}
	var opt2 OptionalVal[*Foo]
	foo := &Foo{v: 1}

	f, ok := opt2.Get()
	require.False(t, ok)
	require.Nil(t, f)

	opt2.Set(foo)
	require.True(t, opt2.Present())
	f, ok = opt2.Get()
	require.True(t, ok)
	require.Same(t, foo, f)

	opt2.Set(nil)
	require.True(t, opt2.Present())
	f, ok = opt2.Get()
	require.True(t, ok)
	require.Nil(t, f)

	opt2.Clear()
	f, ok = opt2.Get()
	require.False(t, ok)
	require.Nil(t, f)
}

func TestFieldsReflect(t *testing.T) {
	var cond TimerCond
	require.Empty(t, cond.FieldsSet())

	cond.Key.Set("k1")
	require.Equal(t, []string{"Key"}, cond.FieldsSet())

	cond.ID.Set("22")
	require.Equal(t, []string{"ID", "Key"}, cond.FieldsSet())
	require.Equal(t, []string{"Key"}, cond.FieldsSet(unsafe.Pointer(&cond.ID)))

	cond.Key.Clear()
	require.Equal(t, []string{"ID"}, cond.FieldsSet())

	cond.KeyPrefix = true
	cond.Clear()
	require.Empty(t, cond.FieldsSet())
	require.False(t, cond.KeyPrefix)

	var update TimerUpdate
	require.Empty(t, update.FieldsSet())

	update.Watermark.Set(time.Now())
	require.Equal(t, []string{"Watermark"}, update.FieldsSet())

	update.Enable.Set(true)
	require.Equal(t, []string{"Enable", "Watermark"}, update.FieldsSet())
	require.Equal(t, []string{"Watermark"}, update.FieldsSet(unsafe.Pointer(&update.Enable)))

	update.Watermark.Clear()
	require.Equal(t, []string{"Enable"}, update.FieldsSet())

	update.Clear()
	require.Empty(t, update.FieldsSet())
}

func TestTimerRecordCond(t *testing.T) {
	tm := &TimerRecord{
		ID: "123",
		TimerSpec: TimerSpec{
			Namespace: "n1",
			Key:       "/path/to/key",
		},
	}

	// ID
	cond := &TimerCond{ID: NewOptionalVal("123")}
	require.True(t, cond.Match(tm))

	cond = &TimerCond{ID: NewOptionalVal("1")}
	require.False(t, cond.Match(tm))

	// Namespace
	cond = &TimerCond{Namespace: NewOptionalVal("n1")}
	require.True(t, cond.Match(tm))

	cond = &TimerCond{Namespace: NewOptionalVal("n2")}
	require.False(t, cond.Match(tm))

	// Key
	cond = &TimerCond{Key: NewOptionalVal("/path/to/key")}
	require.True(t, cond.Match(tm))

	cond = &TimerCond{Key: NewOptionalVal("/path/to/")}
	require.False(t, cond.Match(tm))

	// keyPrefix
	cond = &TimerCond{Key: NewOptionalVal("/path/to/"), KeyPrefix: true}
	require.True(t, cond.Match(tm))

	cond = &TimerCond{Key: NewOptionalVal("/path/to2"), KeyPrefix: true}
	require.False(t, cond.Match(tm))

	// Combined condition
	cond = &TimerCond{ID: NewOptionalVal("123"), Key: NewOptionalVal("/path/to/key")}
	require.True(t, cond.Match(tm))

	cond = &TimerCond{ID: NewOptionalVal("123"), Key: NewOptionalVal("/path/to/")}
	require.False(t, cond.Match(tm))
}

func TestOperatorCond(t *testing.T) {
	tm := &TimerRecord{
		ID: "123",
		TimerSpec: TimerSpec{
			Namespace: "n1",
			Key:       "/path/to/key",
		},
	}

	cond1 := &TimerCond{ID: NewOptionalVal("123")}
	cond2 := &TimerCond{ID: NewOptionalVal("456")}
	cond3 := &TimerCond{Namespace: NewOptionalVal("n1")}
	cond4 := &TimerCond{Namespace: NewOptionalVal("n2")}

	require.True(t, And(cond1, cond3).Match(tm))
	require.False(t, And(cond1, cond2, cond3).Match(tm))
	require.False(t, Or(cond2, cond4).Match(tm))
	require.True(t, Or(cond2, cond1, cond4).Match(tm))

	require.False(t, Not(And(cond1, cond3)).Match(tm))
	require.True(t, Not(And(cond1, cond2, cond3)).Match(tm))
	require.True(t, Not(Or(cond2, cond4)).Match(tm))
	require.False(t, Not(Or(cond2, cond1, cond4)).Match(tm))

	require.False(t, Not(cond1).Match(tm))
	require.True(t, Not(cond2).Match(tm))
}

func TestTimerUpdate(t *testing.T) {
	tpl := TimerRecord{
		ID: "123",
		TimerSpec: TimerSpec{
			Namespace: "n1",
			Key:       "/path/to/key",
		},
		Version: 567,
	}
	tm := tpl.Clone()

	// test check version
	update := &TimerUpdate{
		Enable:       NewOptionalVal(true),
		CheckVersion: NewOptionalVal(uint64(0)),
	}
	_, err := update.Apply(tm)
	require.Error(t, err)
	require.True(t, errors.ErrorEqual(err, ErrVersionNotMatch))
	require.Equal(t, tpl, *tm)

	// test check event id
	update = &TimerUpdate{
		Enable:       NewOptionalVal(true),
		CheckEventID: NewOptionalVal("aa"),
	}
	_, err = update.Apply(tm)
	require.Error(t, err)
	require.True(t, errors.ErrorEqual(err, ErrEventIDNotMatch))
	require.Equal(t, tpl, *tm)

	// test apply without check
	now := time.Now()
	update = &TimerUpdate{
		Enable:          NewOptionalVal(true),
		SchedPolicyType: NewOptionalVal(SchedEventInterval),
		SchedPolicyExpr: NewOptionalVal("5h"),
		Watermark:       NewOptionalVal(now),
		SummaryData:     NewOptionalVal([]byte("summarydata1")),
		EventStatus:     NewOptionalVal(SchedEventTrigger),
		EventID:         NewOptionalVal("event1"),
		EventData:       NewOptionalVal([]byte("eventdata1")),
		EventStart:      NewOptionalVal(now.Add(time.Second)),
	}

	require.Equal(t, reflect.ValueOf(update).Elem().NumField()-2, len(update.FieldsSet()))
	record, err := update.Apply(tm)
	require.NoError(t, err)
	require.True(t, record.Enable)
	require.Equal(t, SchedEventInterval, record.SchedPolicyType)
	require.Equal(t, "5h", record.SchedPolicyExpr)
	require.Equal(t, now, record.Watermark)
	require.Equal(t, []byte("summarydata1"), record.SummaryData)
	require.Equal(t, SchedEventTrigger, record.EventStatus)
	require.Equal(t, "event1", record.EventID)
	require.Equal(t, []byte("eventdata1"), record.EventData)
	require.Equal(t, now.Add(time.Second), record.EventStart)
	require.Equal(t, tpl, *tm)

	emptyUpdate := &TimerUpdate{}
	record, err = emptyUpdate.Apply(tm)
	require.NoError(t, err)
	require.Equal(t, tpl, *record)
}

func TestMemTimerStore(t *testing.T) {
	store := NewMemoryTimerStore()
	RunTimerStoreTest(t, store)
}

func TestMemTimerStoreWatch(t *testing.T) {
	store := NewMemoryTimerStore()
	RunTimerStoreWatchTest(t, store)
}

func RunTimerStoreTest(t *testing.T, store *TimerStore) {
	ctx := context.Background()
	timer := runTimerStoreInsertAndGet(ctx, t, store)
	runTimerStoreUpdate(ctx, t, store, timer)
	runTimerStoreDelete(ctx, t, store, timer)
	runTimerStoreInsertAndList(ctx, t, store)
}

func runTimerStoreInsertAndGet(ctx context.Context, t *testing.T, store *TimerStore) *TimerRecord {
	records, err := store.List(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, records)

	recordTpl := TimerRecord{
		TimerSpec: TimerSpec{
			Namespace:       "n1",
			Key:             "/path/to/key",
			SchedPolicyType: SchedEventInterval,
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
	recordTpl.EventStatus = SchedEventIdle

	// get by id
	record, err = store.GetByID(ctx, id)
	require.NoError(t, err)
	require.Equal(t, recordTpl.ID, record.ID)
	require.NotZero(t, record.Version)
	recordTpl.Version = record.Version
	require.False(t, record.CreateTime.IsZero())
	recordTpl.CreateTime = record.CreateTime
	require.Equal(t, recordTpl, *record)

	// id not exist
	_, err = store.GetByID(ctx, "noexist")
	require.True(t, errors.ErrorEqual(err, ErrTimerNotExist))

	// get by key
	record, err = store.GetByKey(ctx, "n1", "/path/to/key")
	require.NoError(t, err)
	require.Equal(t, recordTpl, *record)

	// key not exist
	_, err = store.GetByKey(ctx, "n1", "noexist")
	require.True(t, errors.ErrorEqual(err, ErrTimerNotExist))
	_, err = store.GetByKey(ctx, "n2", "/path/to/ke")
	require.True(t, errors.ErrorEqual(err, ErrTimerNotExist))

	// invalid insert
	invalid := &TimerRecord{}
	_, err = store.Create(ctx, invalid)
	require.EqualError(t, err, "field 'Namespace' should not be empty")

	invalid.Namespace = "n1"
	_, err = store.Create(ctx, invalid)
	require.EqualError(t, err, "field 'Key' should not be empty")

	invalid.Key = "k1"
	_, err = store.Create(ctx, invalid)
	require.EqualError(t, err, "field 'SchedPolicyType' should not be empty")

	invalid.SchedPolicyType = SchedEventInterval
	invalid.SchedPolicyExpr = "1x"
	_, err = store.Create(ctx, invalid)
	require.EqualError(t, err, "schedule event configuration is not valid: invalid schedule event expr '1x': unknown unit x")

	return &recordTpl
}

func runTimerStoreUpdate(ctx context.Context, t *testing.T, store *TimerStore, tpl *TimerRecord) {
	// normal update
	require.Equal(t, "1h", tpl.SchedPolicyExpr)
	err := store.Update(ctx, tpl.ID, &TimerUpdate{
		SchedPolicyExpr: NewOptionalVal("2h"),
	})
	require.NoError(t, err)
	record, err := store.GetByID(ctx, tpl.ID)
	require.NoError(t, err)
	require.Greater(t, record.Version, tpl.Version)
	tpl.Version = record.Version
	tpl.SchedPolicyExpr = "2h"
	require.Equal(t, *tpl, *record)

	// err update
	err = store.Update(ctx, tpl.ID, &TimerUpdate{
		SchedPolicyExpr: NewOptionalVal("2x"),
	})
	require.EqualError(t, err, "schedule event configuration is not valid: invalid schedule event expr '2x': unknown unit x")
	record, err = store.GetByID(ctx, tpl.ID)
	require.NoError(t, err)
	require.Equal(t, *tpl, *record)
}

func runTimerStoreDelete(ctx context.Context, t *testing.T, store *TimerStore, tpl *TimerRecord) {
	exist, err := store.Delete(ctx, tpl.ID)
	require.NoError(t, err)
	require.True(t, exist)

	_, err = store.GetByID(ctx, tpl.ID)
	require.True(t, errors.ErrorEqual(err, ErrTimerNotExist))

	exist, err = store.Delete(ctx, tpl.ID)
	require.NoError(t, err)
	require.False(t, exist)
}

func runTimerStoreInsertAndList(ctx context.Context, t *testing.T, store *TimerStore) {
	records, err := store.List(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, records)

	recordTpl1 := TimerRecord{
		ID: "id1",
		TimerSpec: TimerSpec{
			Namespace:       "n1",
			Key:             "/path/to/key1",
			SchedPolicyType: SchedEventInterval,
			SchedPolicyExpr: "1h",
		},
		Version:     1,
		CreateTime:  time.Now(),
		EventStatus: SchedEventIdle,
	}

	recordTpl2 := TimerRecord{
		ID: "id2",
		TimerSpec: TimerSpec{
			Namespace:       "n1",
			Key:             "/path/to/key2",
			SchedPolicyType: SchedEventInterval,
			SchedPolicyExpr: "2h",
		},
		Version:     2,
		CreateTime:  time.Now(),
		EventStatus: SchedEventIdle,
	}

	recordTpl3 := TimerRecord{
		ID: "id3",
		TimerSpec: TimerSpec{
			Namespace:       "n2",
			Key:             "/path/to/another",
			SchedPolicyType: SchedEventInterval,
			SchedPolicyExpr: "3h",
		},
		Version:     3,
		CreateTime:  time.Now(),
		EventStatus: SchedEventIdle,
	}

	id, err := store.Create(ctx, &recordTpl1)
	require.NoError(t, err)
	require.Equal(t, recordTpl1.ID, id)

	id, err = store.Create(ctx, &recordTpl2)
	require.NoError(t, err)
	require.Equal(t, recordTpl2.ID, id)

	id, err = store.Create(ctx, &recordTpl3)
	require.NoError(t, err)
	require.Equal(t, recordTpl3.ID, id)

	checkList := func(expected []*TimerRecord, list []*TimerRecord) {
		expectedMap := make(map[string]*TimerRecord, len(expected))
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
	checkList([]*TimerRecord{&recordTpl1, &recordTpl2, &recordTpl3}, timers)

	timers, err = store.List(ctx, &TimerCond{
		Key:       NewOptionalVal("/path/to/k"),
		KeyPrefix: true,
	})
	require.NoError(t, err)
	checkList([]*TimerRecord{&recordTpl1, &recordTpl2}, timers)
}

func RunTimerStoreWatchTest(t *testing.T, store *TimerStore) {
	require.True(t, store.WatchSupported())
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
	}()

	timer := TimerRecord{
		TimerSpec: TimerSpec{
			Namespace:       "n1",
			Key:             "/path/to/key",
			SchedPolicyType: SchedEventInterval,
			SchedPolicyExpr: "1h",
			Data:            []byte("data1"),
		},
	}

	ch := store.Watch(ctx)
	assertWatchEvent := func(tp WatchTimerEventType, id string) {
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
	assertWatchEvent(WatchTimerEventCreate, id)

	err = store.Update(ctx, id, &TimerUpdate{
		SchedPolicyExpr: NewOptionalVal("2h"),
	})
	require.NoError(t, err)
	assertWatchEvent(WatchTimerEventUpdate, id)

	exit, err := store.Delete(ctx, id)
	require.NoError(t, err)
	require.True(t, exit)
	assertWatchEvent(WatchTimerEventDelete, id)

	cancel()
	assertWatchEvent(0, "")
}
