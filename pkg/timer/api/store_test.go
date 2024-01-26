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
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/timeutil"
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
			Tags:      []string{"tagA1", "tagA2"},
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

	// Tags
	tm2 := tm.Clone()
	tm2.Tags = nil

	cond = &TimerCond{Tags: NewOptionalVal([]string{})}
	require.True(t, cond.Match(tm))
	require.True(t, cond.Match(tm2))

	cond = &TimerCond{Tags: NewOptionalVal([]string{"tagA"})}
	require.False(t, cond.Match(tm))
	require.False(t, cond.Match(tm2))

	cond = &TimerCond{Tags: NewOptionalVal([]string{"tagA1"})}
	require.True(t, cond.Match(tm))
	require.False(t, cond.Match(tm2))

	cond = &TimerCond{Tags: NewOptionalVal([]string{"tagA1", "tagA2"})}
	require.True(t, cond.Match(tm))

	cond = &TimerCond{Tags: NewOptionalVal([]string{"tagA1", "tagB1"})}
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
	timeutil.SetSystemTZ("Asia/Shanghai")
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
	_, err := update.apply(tm)
	require.Error(t, err)
	require.True(t, errors.ErrorEqual(err, ErrVersionNotMatch))
	require.Equal(t, tpl, *tm)

	// test check event id
	update = &TimerUpdate{
		Enable:       NewOptionalVal(true),
		CheckEventID: NewOptionalVal("aa"),
	}
	_, err = update.apply(tm)
	require.Error(t, err)
	require.True(t, errors.ErrorEqual(err, ErrEventIDNotMatch))
	require.Equal(t, tpl, *tm)

	// test apply without check for some common fields
	now := time.Now()
	update = &TimerUpdate{
		Enable:          NewOptionalVal(true),
		TimeZone:        NewOptionalVal("UTC"),
		SchedPolicyType: NewOptionalVal(SchedEventInterval),
		SchedPolicyExpr: NewOptionalVal("5h"),
		Watermark:       NewOptionalVal(now),
		SummaryData:     NewOptionalVal([]byte("summarydata1")),
		EventStatus:     NewOptionalVal(SchedEventTrigger),
		EventID:         NewOptionalVal("event1"),
		EventData:       NewOptionalVal([]byte("eventdata1")),
		EventStart:      NewOptionalVal(now.Add(time.Second)),
		Tags:            NewOptionalVal([]string{"l1", "l2"}),
		ManualRequest: NewOptionalVal(ManualRequest{
			ManualRequestID:   "req1",
			ManualRequestTime: time.Unix(123, 0),
			ManualTimeout:     time.Minute,
			ManualProcessed:   true,
			ManualEventID:     "event1",
		}),
		EventExtra: NewOptionalVal(EventExtra{
			EventManualRequestID: "req",
			EventWatermark:       time.Unix(456, 0),
		}),
	}

	require.Equal(t, reflect.ValueOf(update).Elem().NumField()-2, len(update.FieldsSet()))
	record, err := update.apply(tm)
	require.NoError(t, err)
	require.True(t, record.Enable)
	require.Equal(t, "UTC", record.TimeZone)
	require.Equal(t, time.UTC, record.Location)
	require.Equal(t, SchedEventInterval, record.SchedPolicyType)
	require.Equal(t, "5h", record.SchedPolicyExpr)
	require.Equal(t, now, record.Watermark)
	require.Equal(t, []byte("summarydata1"), record.SummaryData)
	require.Equal(t, SchedEventTrigger, record.EventStatus)
	require.Equal(t, "event1", record.EventID)
	require.Equal(t, []byte("eventdata1"), record.EventData)
	require.Equal(t, now.Add(time.Second), record.EventStart)
	require.Equal(t, []string{"l1", "l2"}, record.Tags)
	require.Equal(t, ManualRequest{
		ManualRequestID:   "req1",
		ManualRequestTime: time.Unix(123, 0),
		ManualTimeout:     time.Minute,
		ManualProcessed:   true,
		ManualEventID:     "event1",
	}, record.ManualRequest)
	require.False(t, record.IsManualRequesting())
	require.Equal(t, EventExtra{
		EventManualRequestID: "req",
		EventWatermark:       time.Unix(456, 0),
	}, record.EventExtra)
	require.Equal(t, tpl, *tm)

	// test apply without check for ManualRequest and EventExtra
	tpl = *record.Clone()
	tm = tpl.Clone()
	update = &TimerUpdate{
		ManualRequest: NewOptionalVal(ManualRequest{
			ManualRequestID:   "req2",
			ManualRequestTime: time.Unix(789, 0),
			ManualTimeout:     time.Minute,
		}),
		EventExtra: NewOptionalVal(EventExtra{
			EventManualRequestID: "req2",
		}),
	}
	record, err = update.apply(tm)
	require.NoError(t, err)
	require.Equal(t, ManualRequest{
		ManualRequestID:   "req2",
		ManualRequestTime: time.Unix(789, 0),
		ManualTimeout:     time.Minute,
	}, record.ManualRequest)
	require.True(t, record.IsManualRequesting())
	require.Equal(t, EventExtra{
		EventManualRequestID: "req2",
	}, record.EventExtra)
	require.Equal(t, tpl, *tm)

	// test apply without check for empty ManualRequest and EventExtra
	tpl = *record.Clone()
	tm = tpl.Clone()
	update = &TimerUpdate{
		ManualRequest: NewOptionalVal(ManualRequest{}),
		EventExtra:    NewOptionalVal(EventExtra{}),
	}
	record, err = update.apply(tm)
	require.NoError(t, err)
	require.Equal(t, ManualRequest{}, record.ManualRequest)
	require.False(t, record.IsManualRequesting())
	require.Equal(t, EventExtra{}, record.EventExtra)
	require.Equal(t, tpl, *tm)

	emptyUpdate := &TimerUpdate{}
	record, err = emptyUpdate.apply(tm)
	require.NoError(t, err)
	require.Equal(t, tpl, *record)
}
