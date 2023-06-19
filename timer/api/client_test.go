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
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestGetTimerOption(t *testing.T) {
	var cond TimerCond
	require.Empty(t, cond.FieldsSet())

	// test 'Key' field
	require.False(t, cond.Key.Present())

	WithKey("k1")(&cond)
	key, ok := cond.Key.Get()
	require.True(t, ok)
	require.Equal(t, "k1", key)
	require.False(t, cond.KeyPrefix)
	require.Equal(t, []string{"Key"}, cond.FieldsSet())

	WithKeyPrefix("k2")(&cond)
	key, ok = cond.Key.Get()
	require.True(t, ok)
	require.Equal(t, "k2", key)
	require.True(t, cond.KeyPrefix)
	require.Equal(t, []string{"Key"}, cond.FieldsSet())

	WithKey("k3")(&cond)
	key, ok = cond.Key.Get()
	require.True(t, ok)
	require.Equal(t, "k3", key)
	require.False(t, cond.KeyPrefix)
	require.Equal(t, []string{"Key"}, cond.FieldsSet())

	// test 'ID' field
	require.False(t, cond.ID.Present())

	WithID("id1")(&cond)
	id, ok := cond.ID.Get()
	require.True(t, ok)
	require.Equal(t, "id1", id)
	require.Equal(t, []string{"ID", "Key"}, cond.FieldsSet())

	// test 'Tags' field
	require.False(t, cond.Tags.Present())
	WithTag("l1", "l2")(&cond)
	tags, ok := cond.Tags.Get()
	require.True(t, ok)
	require.Equal(t, []string{"l1", "l2"}, tags)
	require.Equal(t, []string{"ID", "Key", "Tags"}, cond.FieldsSet())
}

func TestUpdateTimerOption(t *testing.T) {
	var update TimerUpdate
	require.Empty(t, update)

	// test 'Enable' field
	require.False(t, update.Enable.Present())

	WithSetEnable(true)(&update)
	setEnable, ok := update.Enable.Get()
	require.True(t, ok)
	require.True(t, setEnable)
	require.Equal(t, []string{"Enable"}, update.FieldsSet())

	WithSetEnable(false)(&update)
	setEnable, ok = update.Enable.Get()
	require.True(t, ok)
	require.False(t, setEnable)
	require.Equal(t, []string{"Enable"}, update.FieldsSet())

	// test schedule policy
	require.False(t, update.SchedPolicyType.Present())
	require.False(t, update.SchedPolicyExpr.Present())

	WithSetSchedExpr(SchedEventInterval, "3h")(&update)
	stp, ok := update.SchedPolicyType.Get()
	require.True(t, ok)
	require.Equal(t, SchedEventInterval, stp)
	expr, ok := update.SchedPolicyExpr.Get()
	require.True(t, ok)
	require.Equal(t, "3h", expr)
	require.Equal(t, []string{"Enable", "SchedPolicyType", "SchedPolicyExpr"}, update.FieldsSet())

	WithSetSchedExpr(SchedEventInterval, "1h")(&update)
	stp, ok = update.SchedPolicyType.Get()
	require.True(t, ok)
	require.Equal(t, SchedEventInterval, stp)
	expr, ok = update.SchedPolicyExpr.Get()
	require.True(t, ok)
	require.Equal(t, "1h", expr)
	require.Equal(t, []string{"Enable", "SchedPolicyType", "SchedPolicyExpr"}, update.FieldsSet())

	// test 'Watermark' field
	require.False(t, update.Watermark.Present())

	WithSetWatermark(time.Unix(1234, 5678))(&update)
	watermark, ok := update.Watermark.Get()
	require.True(t, ok)
	require.Equal(t, time.Unix(1234, 5678), watermark)
	require.Equal(t, []string{"Enable", "SchedPolicyType", "SchedPolicyExpr", "Watermark"}, update.FieldsSet())

	// test 'SummaryData' field
	require.False(t, update.SummaryData.Present())

	WithSetSummaryData([]byte("hello"))(&update)
	summary, ok := update.SummaryData.Get()
	require.True(t, ok)
	require.Equal(t, []byte("hello"), summary)
	require.Equal(t, []string{"Enable", "SchedPolicyType", "SchedPolicyExpr", "Watermark", "SummaryData"}, update.FieldsSet())

	// test 'Tags' field
	require.False(t, update.Tags.Present())
	WithSetTags(nil)(&update)
	tags, ok := update.Tags.Get()
	require.True(t, ok)
	require.Equal(t, 0, len(tags))
	WithSetTags([]string{"l1", "l2"})(&update)
	tags, ok = update.Tags.Get()
	require.True(t, ok)
	require.Equal(t, []string{"l1", "l2"}, tags)
	require.Equal(t, []string{"Tags", "Enable", "SchedPolicyType", "SchedPolicyExpr", "Watermark", "SummaryData"}, update.FieldsSet())
}

func TestDefaultClient(t *testing.T) {
	store := NewMemoryTimerStore()
	cli := NewDefaultTimerClient(store)
	ctx := context.Background()
	spec := TimerSpec{
		Key:             "k1",
		SchedPolicyType: SchedEventInterval,
		SchedPolicyExpr: "1h",
		Data:            []byte("data1"),
		Tags:            []string{"l1", "l2"},
	}

	// create
	timer, err := cli.CreateTimer(ctx, spec)
	require.NoError(t, err)
	spec.Namespace = "default"
	require.NotEmpty(t, timer.ID)
	require.Equal(t, spec, timer.TimerSpec)
	require.Equal(t, SchedEventIdle, timer.EventStatus)
	require.Equal(t, "", timer.EventID)
	require.Empty(t, timer.EventData)
	require.Empty(t, timer.SummaryData)

	// get by id
	got, err := cli.GetTimerByID(ctx, timer.ID)
	require.NoError(t, err)
	require.Equal(t, timer, got)

	// get by key
	got, err = cli.GetTimerByKey(ctx, timer.Key)
	require.NoError(t, err)
	require.Equal(t, timer, got)

	// get by key prefix
	tms, err := cli.GetTimers(ctx, WithKeyPrefix("k"))
	require.NoError(t, err)
	require.Equal(t, 1, len(tms))
	require.Equal(t, timer, tms[0])

	// get by tag
	tms, err = cli.GetTimers(ctx, WithTag("l1"))
	require.NoError(t, err)
	require.Equal(t, 1, len(tms))
	require.Equal(t, timer, tms[0])

	tms, err = cli.GetTimers(ctx, WithTag("l1", "l2"))
	require.NoError(t, err)
	require.Equal(t, 1, len(tms))
	require.Equal(t, timer, tms[0])

	tms, err = cli.GetTimers(ctx, WithTag("l3"))
	require.NoError(t, err)
	require.Equal(t, 0, len(tms))

	// update
	err = cli.UpdateTimer(ctx, timer.ID, WithSetSchedExpr(SchedEventInterval, "3h"))
	require.NoError(t, err)
	timer.SchedPolicyType = SchedEventInterval
	timer.SchedPolicyExpr = "3h"
	got, err = cli.GetTimerByID(ctx, timer.ID)
	require.NoError(t, err)
	require.Greater(t, got.Version, timer.Version)
	timer.Version = got.Version
	require.Equal(t, timer, got)

	// close event
	eventStart := time.Now().Add(-time.Second)
	err = store.Update(ctx, timer.ID, &TimerUpdate{
		EventStatus: NewOptionalVal(SchedEventTrigger),
		EventID:     NewOptionalVal("event1"),
		EventData:   NewOptionalVal([]byte("d1")),
		SummaryData: NewOptionalVal([]byte("s1")),
		EventStart:  NewOptionalVal(eventStart),
	})
	require.NoError(t, err)
	err = cli.CloseTimerEvent(ctx, timer.ID, "event2")
	require.True(t, errors.ErrorEqual(ErrEventIDNotMatch, err))

	err = cli.CloseTimerEvent(ctx, timer.ID, "event2", WithSetSchedExpr(SchedEventInterval, "1h"))
	require.EqualError(t, err, "The field(s) [SchedPolicyType, SchedPolicyExpr] are not allowed to update when close event")

	err = cli.CloseTimerEvent(ctx, timer.ID, "event1")
	require.NoError(t, err)
	timer, err = cli.GetTimerByID(ctx, timer.ID)
	require.NoError(t, err)
	require.Equal(t, SchedEventIdle, timer.EventStatus)
	require.Empty(t, timer.EventID)
	require.Empty(t, timer.EventData)
	require.True(t, timer.EventStart.IsZero())
	require.Equal(t, []byte("s1"), timer.SummaryData)
	require.Equal(t, eventStart, timer.Watermark)

	// close event with option
	err = store.Update(ctx, timer.ID, &TimerUpdate{
		EventID:     NewOptionalVal("event1"),
		EventData:   NewOptionalVal([]byte("d1")),
		SummaryData: NewOptionalVal([]byte("s1")),
	})
	require.NoError(t, err)

	watermark := time.Now().Add(time.Hour)
	err = cli.CloseTimerEvent(ctx, timer.ID, "event1", WithSetWatermark(watermark), WithSetSummaryData([]byte("s2")))
	require.NoError(t, err)
	timer, err = cli.GetTimerByID(ctx, timer.ID)
	require.NoError(t, err)
	require.Equal(t, SchedEventIdle, timer.EventStatus)
	require.Empty(t, timer.EventID)
	require.Empty(t, timer.EventData)
	require.True(t, timer.EventStart.IsZero())
	require.Equal(t, []byte("s2"), timer.SummaryData)
	require.Equal(t, watermark, timer.Watermark)

	// delete
	exit, err := cli.DeleteTimer(ctx, timer.ID)
	require.NoError(t, err)
	require.True(t, exit)

	// delete no exist
	exit, err = cli.DeleteTimer(ctx, timer.ID)
	require.NoError(t, err)
	require.False(t, exit)
}
