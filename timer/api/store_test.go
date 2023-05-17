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
	"testing"
	"time"
	"unsafe"

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

func TestTimerUpdate(t *testing.T) {
	tm := &TimerRecord{
		ID: "123",
		TimerSpec: TimerSpec{
			Namespace: "n1",
			Key:       "/path/to/key",
		},
	}

	now := time.Now()
	data := []byte("aabbcc")
	update := &TimerUpdate{
		Enable:          NewOptionalVal(true),
		SchedPolicyType: NewOptionalVal(SchedEventInterval),
		SchedPolicyExpr: NewOptionalVal("1h"),
		Watermark:       NewOptionalVal(now),
		SummaryData:     NewOptionalVal(data),
	}

	require.NoError(t, update.Apply(tm))
	require.True(t, tm.Enable)
	require.Equal(t, SchedEventInterval, tm.SchedPolicyType)
	require.Equal(t, "1h", tm.SchedPolicyExpr)
	require.Equal(t, now, tm.Watermark)
	require.Equal(t, data, tm.SummaryData)

	emptyUpdate := &TimerUpdate{}
	tm2 := tm.Clone()
	require.NoError(t, emptyUpdate.Apply(tm2))
	require.Equal(t, tm, tm2)
}
