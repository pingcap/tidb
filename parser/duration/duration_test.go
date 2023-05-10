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
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseDuration(t *testing.T) {
	cases := []struct {
		str      string
		duration time.Duration
	}{
		{
			"1h",
			time.Hour,
		},
		{
			"1h100m",
			time.Hour + 100*time.Minute,
		},
		{
			"1d10000m",
			24*time.Hour + 10000*time.Minute,
		},
		{
			"1d100h",
			24*time.Hour + 100*time.Hour,
		},
		{
			"1.5d",
			36 * time.Hour,
		},
		{
			"1d1.5h",
			24*time.Hour + time.Hour + 30*time.Minute,
		},
		{
			"1d3.555h",
			24*time.Hour + time.Duration(3.555*float64(time.Hour)),
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
