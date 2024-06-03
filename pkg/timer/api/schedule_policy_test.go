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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIntervalPolicy(t *testing.T) {
	watermark1 := time.Now()
	watermark2, err := time.Parse(time.RFC3339, "2021-11-21T11:21:31Z")
	require.NoError(t, err)

	cases := []struct {
		expr     string
		err      bool
		interval time.Duration
	}{
		{
			expr:     "6m",
			interval: 6 * time.Minute,
		},
		{
			expr:     "7h",
			interval: 7 * time.Hour,
		},
		{
			expr:     "8d",
			interval: 8 * 24 * time.Hour,
		},
		{
			expr: "11",
			err:  true,
		},
	}

	for _, c := range cases {
		p, err := CreateSchedEventPolicy(SchedEventInterval, c.expr)
		if c.err {
			require.Nil(t, p)
			require.ErrorContains(t, err, fmt.Sprintf("invalid schedule event expr '%s'", c.expr))
			continue
		}

		require.NotNil(t, p)
		require.IsType(t, &SchedIntervalPolicy{}, p)
		require.NoError(t, err)
		tm, ok := p.NextEventTime(watermark1)
		require.True(t, ok)
		require.Equal(t, watermark1.Add(c.interval), tm)
		tm, ok = p.NextEventTime(watermark2)
		require.True(t, ok)
		require.Equal(t, watermark2.Add(c.interval), tm)
	}
}

func TestCronPolicy(t *testing.T) {
	locE2 := time.FixedZone("UTC+1", 2*60*60)
	locW2 := time.FixedZone("UTC-1", -2*60*60)
	cases := []struct {
		expr string
		err  bool
		tm   time.Time
		next time.Time
	}{
		{
			expr: "",
			err:  true,
		},
		{
			expr: "aaa",
			err:  true,
		},
		{
			expr: "61 1 * * *",
			err:  true,
		},
		{
			expr: "@hourly",
			tm:   time.Date(2021, 11, 21, 11, 21, 31, 0, time.UTC),
			next: time.Date(2021, 11, 21, 12, 0, 0, 0, time.UTC),
		},
		{
			expr: "@hourly",
			tm:   time.Date(2021, 11, 21, 12, 0, 0, 0, time.Local),
			next: time.Date(2021, 11, 21, 13, 0, 0, 0, time.Local),
		},
		{
			expr: "@daily",
			tm:   time.Date(2021, 11, 21, 11, 21, 31, 0, time.Local),
			next: time.Date(2021, 11, 22, 0, 0, 0, 0, time.Local),
		},
		{
			expr: "@weekly",
			tm:   time.Date(2021, 11, 19, 11, 21, 31, 0, locE2), // Friday
			next: time.Date(2021, 11, 21, 0, 0, 0, 0, locE2),    // Sunday
		},
		{
			expr: "@monthly",
			tm:   time.Date(2021, 12, 19, 11, 21, 31, 0, locW2),
			next: time.Date(2022, 1, 1, 0, 0, 0, 0, locW2),
		},
		{
			expr: "@yearly",
			tm:   time.Date(2021, 12, 19, 11, 21, 31, 0, time.UTC),
			next: time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			expr: "12 12 * * *",
			tm:   time.Date(2021, 12, 19, 11, 21, 31, 0, time.Local),
			next: time.Date(2021, 12, 19, 12, 12, 0, 0, time.Local),
		},
		{
			expr: "5 4 21 2 *",
			tm:   time.Date(2021, 12, 19, 11, 21, 31, 0, locE2),
			next: time.Date(2022, 2, 21, 4, 5, 0, 0, locE2),
		},
		{
			expr: "55 16 * 12 0",
			tm:   time.Date(2021, 12, 21, 11, 21, 31, 0, locW2),
			next: time.Date(2021, 12, 26, 16, 55, 0, 0, locW2),
		},
		{
			expr: "12 8,16,19 * * *",
			tm:   time.Date(2021, 12, 21, 2, 21, 31, 0, locW2),
			next: time.Date(2021, 12, 21, 8, 12, 0, 0, locW2),
		},
		{
			expr: "12 8,16,19 * * *",
			tm:   time.Date(2021, 12, 21, 9, 21, 31, 0, locW2),
			next: time.Date(2021, 12, 21, 16, 12, 0, 0, locW2),
		},
		{
			expr: "12 8,16,19 * * *",
			tm:   time.Date(2021, 12, 21, 19, 21, 31, 0, locW2),
			next: time.Date(2021, 12, 22, 8, 12, 0, 0, locW2),
		},
		{
			expr: "12 8,16,19 * * *",
			tm:   time.Date(2021, 12, 21, 16, 12, 0, 0, time.Local),
			next: time.Date(2021, 12, 21, 19, 12, 0, 0, time.Local),
		},
		{
			expr: "* * 29 2 *",
			tm:   time.Date(2021, 12, 21, 16, 12, 0, 0, time.Local),
			next: time.Date(2024, 2, 29, 0, 0, 0, 0, time.Local),
		},
		{
			expr: "* * 30 2 *",
			tm:   time.Date(2021, 12, 21, 16, 12, 0, 0, time.Local),
			next: time.Time{},
		},
	}

	for _, c := range cases {
		p, err := CreateSchedEventPolicy(SchedEventCron, c.expr)
		if c.err {
			require.Nil(t, p)
			require.ErrorContains(t, err, fmt.Sprintf("invalid cron expr '%s'", c.expr))
			continue
		}

		require.NoError(t, err)
		require.NotNil(t, p)
		require.IsType(t, &CronPolicy{}, p)
		watermark := c.tm
		next, ok := p.NextEventTime(watermark)
		require.Equal(t, c.next, next)
		require.Equal(t, !next.IsZero(), ok)
	}
}
