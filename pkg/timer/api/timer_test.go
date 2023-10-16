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

func TestTimerValidate(t *testing.T) {
	// invalid insert
	record := &TimerRecord{}
	err := record.Validate()
	require.EqualError(t, err, "field 'Namespace' should not be empty")

	record.Namespace = "n1"
	err = record.Validate()
	require.EqualError(t, err, "field 'Key' should not be empty")

	record.Key = "k1"
	err = record.Validate()
	require.EqualError(t, err, "field 'SchedPolicyType' should not be empty")

	record.SchedPolicyType = "aa"
	err = record.Validate()
	require.EqualError(t, err, "schedule event configuration is not valid: invalid schedule event type: 'aa'")

	record.SchedPolicyType = SchedEventInterval
	record.SchedPolicyExpr = "1x"
	err = record.Validate()
	require.EqualError(t, err, "schedule event configuration is not valid: invalid schedule event expr '1x': unknown unit x")

	record.SchedPolicyExpr = "1h"
	require.Nil(t, record.Validate())

	record.TimeZone = "a123"
	err = record.Validate()
	require.ErrorContains(t, err, "Unknown or incorrect time zone: 'a123'")

	record.TimeZone = "tidb"
	err = record.Validate()
	require.ErrorContains(t, err, "Unknown or incorrect time zone: 'tidb'")

	record.TimeZone = "+0800"
	require.NoError(t, record.Validate())

	record.TimeZone = "Asia/Shanghai"
	require.NoError(t, record.Validate())

	record.TimeZone = ""
	require.NoError(t, record.Validate())
}

func TestTimerNextEventTime(t *testing.T) {
	now := time.Now().In(time.UTC)
	record := &TimerRecord{
		TimerSpec: TimerSpec{
			SchedPolicyType: SchedEventInterval,
			SchedPolicyExpr: "1h",
			Watermark:       now,
			Enable:          true,
		},
	}

	next, ok, err := record.NextEventTime()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, now.Add(time.Hour), next)

	loc := time.FixedZone("UTC+1", 60*60)
	record.Location = loc
	next, ok, err = record.NextEventTime()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, now.Add(time.Hour).In(loc), next)

	record.Enable = false
	next, ok, err = record.NextEventTime()
	require.NoError(t, err)
	require.False(t, ok)
	require.True(t, next.IsZero())

	record.SchedPolicyExpr = "abcde"
	next, ok, err = record.NextEventTime()
	require.NoError(t, err)
	require.False(t, ok)
	require.True(t, next.IsZero())

	record.Enable = true
	next, ok, err = record.NextEventTime()
	require.ErrorContains(t, err, "invalid schedule event expr")
	require.False(t, ok)
	require.True(t, next.IsZero())

	record.SchedPolicyType = SchedEventCron
	record.SchedPolicyExpr = "0 0 30 2 *"
	next, ok, err = record.NextEventTime()
	require.NoError(t, err)
	require.False(t, ok)
	require.True(t, next.IsZero())
}
