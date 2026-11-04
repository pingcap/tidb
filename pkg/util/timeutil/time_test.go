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

package timeutil

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSleep(t *testing.T) {
	const contextTimeout, sleepTime = 10 * time.Millisecond, 10 * time.Second
	now := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
	defer cancel()
	Sleep(ctx, sleepTime)
	since := time.Since(now)
	require.Greater(t, since, contextTimeout)
	require.Less(t, since, sleepTime)
}

func TestIsUniqueLocalTime(t *testing.T) {
	for _, tc := range []struct {
		name, zone, instant string
		unique              bool
	}{
		{"NewYorkFirstFold", "America/New_York", "2024-11-03T01:30:00.123456789-04:00", false},
		{"NewYorkSecondFold", "America/New_York", "2024-11-03T01:30:00.123456789-05:00", false},
		{"NewYorkBeforeFold", "America/New_York", "2024-11-03T00:59:59.999999999-04:00", true},
		{"NewYorkAfterFold", "America/New_York", "2024-11-03T02:00:00-05:00", true},
		{"NewYorkBeforeGap", "America/New_York", "2024-03-10T01:59:59-05:00", true},
		{"NewYorkAfterGap", "America/New_York", "2024-03-10T03:00:00-04:00", true},
		{"LordHoweFirstFold", "Australia/Lord_Howe", "2024-04-07T01:45:00+11:00", false},
		{"LordHoweSecondFold", "Australia/Lord_Howe", "2024-04-07T01:45:00+10:30", false},
		{"LordHoweBeforeFold", "Australia/Lord_Howe", "2024-04-07T01:29:59+11:00", true},
		{"LordHoweAfterFold", "Australia/Lord_Howe", "2024-04-07T02:00:00+10:30", true},
		{"UTC", "UTC", "2024-11-03T01:30:00Z", true},
		{"Shanghai", "Asia/Shanghai", "2024-11-03T01:30:00+08:00", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loc, err := time.LoadLocation(tc.zone)
			require.NoError(t, err)
			instant, err := time.Parse(time.RFC3339Nano, tc.instant)
			require.NoError(t, err)
			instant = instant.In(loc)

			// Independently enumerate nearby UTC instants. The tested transitions
			// change by whole minutes; the reference does not use ZoneBounds.
			const civilLayout = "2006-01-02T15:04:05.999999999"
			matches := 0
			for minute := -240; minute <= 240; minute++ {
				candidate := instant.UTC().Add(time.Duration(minute) * time.Minute).In(loc)
				if candidate.Format(civilLayout) == instant.Format(civilLayout) {
					matches++
				}
			}
			require.Equal(t, tc.unique, matches == 1)
			require.Equal(t, matches == 1, IsUniqueLocalTime(instant))
		})
	}
}
