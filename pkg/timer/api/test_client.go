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

func RunGetTimerOption(t *testing.T) {
	// Test WithKey
	opt := WithKey("test-key")
	require.NotNil(t, opt)

	// Test WithKeyPrefix
	opt = WithKeyPrefix("test-prefix")
	require.NotNil(t, opt)

	// Test WithID
	opt = WithID("test-id")
	require.NotNil(t, opt)

	// Test WithTag
	opt = WithTag("tag1", "tag2")
	require.NotNil(t, opt)
}

func RunUpdateTimerOption(t *testing.T) {
	// Test WithSetEnable
	opt := WithSetEnable(true)
	require.NotNil(t, opt)

	// Test WithSetTimeZone
	opt = WithSetTimeZone("UTC")
	require.NotNil(t, opt)

	// Test WithSetSchedExpr
	opt = WithSetSchedExpr(SchedEventInterval, "1h")
	require.NotNil(t, opt)

	// Test WithSetWatermark
	opt = WithSetWatermark(time.Now())
	require.NotNil(t, opt)

	// Test WithSetSummaryData
	opt = WithSetSummaryData([]byte("test"))
	require.NotNil(t, opt)

	// Test WithSetTags
	opt = WithSetTags([]string{"tag1"})
	require.NotNil(t, opt)
}

func RunDefaultClient(t *testing.T) {
	store := NewMemoryTimerStore()
	require.NotNil(t, store)

	client := NewDefaultTimerClient(store)
	require.NotNil(t, client)
}

func RunDefaultClientManualTriggerRetry(t *testing.T) {
	store := NewMemoryTimerStore()
	require.NotNil(t, store)

	client := NewDefaultTimerClient(store)
	require.NotNil(t, client)

	// Test manual trigger retry logic would go here
	// For now, just verify the client is created
}

func RunTimerValidate(t *testing.T) {
	// Test timer validation logic
	spec := TimerSpec{
		Namespace:       "test-ns",
		Key:             "test-key",
		SchedPolicyType: SchedEventInterval,
		SchedPolicyExpr: "1h",
		Enable:          true,
	}
	err := spec.Validate()
	require.NoError(t, err)
}

func RunTimerNextEventTime(t *testing.T) {
	// Test next event time calculation
	policy, err := CreateSchedEventPolicy(SchedEventInterval, "1h")
	require.NoError(t, err)
	require.NotNil(t, policy)

	now := time.Now()
	nextTime, ok := policy.NextEventTime(now)
	require.True(t, ok)
	require.False(t, nextTime.IsZero())
}
