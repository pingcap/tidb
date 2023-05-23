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
		p, err := NewSchedIntervalPolicy(c.expr)
		if c.err {
			require.ErrorContains(t, err, fmt.Sprintf("invalid schedule event expr '%s'", c.expr))
			continue
		}
		require.NoError(t, err)
		tm, ok := p.NextEventTime(watermark1)
		require.True(t, ok)
		require.Equal(t, watermark1.Add(c.interval), tm)
		tm, ok = p.NextEventTime(watermark2)
		require.True(t, ok)
		require.Equal(t, watermark2.Add(c.interval), tm)
	}
}
