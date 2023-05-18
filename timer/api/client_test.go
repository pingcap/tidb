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
}
