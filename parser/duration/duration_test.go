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
// See the License for the specific language governing permissions and
// limitations under the License.

package duration

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseDuration(t *testing.T) {
	cases := []struct {
		str      string
		duration Duration
	}{
		{
			"1h",
			Duration{Hour: 1},
		},
		{
			"1h100m",
			Duration{Hour: 2, Minute: 40},
		},
		{
			"1d10000m",
			Duration{Day: 7, Hour: 22, Minute: 40},
		},
		{
			"1d100h",
			Duration{Day: 5, Hour: 4},
		},
	}

	for _, c := range cases {
		t.Run(c.str, func(t *testing.T) {
			d, err := ParseDuration(c.str)
			require.NoError(t, err)
			require.Equal(t, c.duration, d)
		})
	}
}

func TestFormatDuration(t *testing.T) {
	cases := []struct {
		str      string
		duration Duration
	}{
		{
			"1h",
			Duration{Hour: 1},
		},
		{
			"1h30m",
			Duration{Hour: 1, Minute: 30},
		},
		{
			"1d12h",
			Duration{Day: 1, Hour: 12},
		},
		{
			"5d1h40m",
			Duration{Day: 5, Minute: 100},
		},
		{
			"0m",
			Duration{Minute: 0},
		},
	}

	for _, c := range cases {
		t.Run(c.str, func(t *testing.T) {
			str := c.duration.String()
			require.Equal(t, c.str, str)
		})
	}
}
