// Copyright 2020 PingCAP, Inc.
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

package mathutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRandWithTime(t *testing.T) {
	rng1 := NewWithTime()
	// NOTE: On windows platform, this Sleep is necessary. Because time.Now() is
	// imprecise, calling UnixNano() twice returns the same value. We have to make
	// sure the elapsed time is longer than 1ms to get different values.
	time.Sleep(time.Millisecond)
	rng2 := NewWithTime()
	got1 := rng1.Gen()
	got2 := rng2.Gen()
	require.True(t, got1 < 1.0)
	require.True(t, got1 >= 0.0)
	require.True(t, got1 != rng1.Gen())
	require.True(t, got2 < 1.0)
	require.True(t, got2 >= 0.0)
	require.True(t, got2 != rng2.Gen())
	require.True(t, got1 != got2)
}

func TestRandWithSeed(t *testing.T) {
	tests := [4]struct {
		seed  int64
		once  float64
		twice float64
	}{{0, 0.15522042769493574, 0.620881741513388},
		{1, 0.40540353712197724, 0.8716141803857071},
		{-1, 0.9050373219931845, 0.37014932126752037},
		{9223372036854775807, 0.9050373219931845, 0.37014932126752037}}
	for _, test := range tests {
		rng := NewWithSeed(test.seed)
		got1 := rng.Gen()
		require.Equal(t, got1, test.once)
		got2 := rng.Gen()
		require.Equal(t, got2, test.twice)
	}
}

func TestRandWithSeed1AndSeed2(t *testing.T) {
	seed1 := uint32(10000000)
	seed2 := uint32(1000000)

	rng := NewWithTime()
	rng.SetSeed1(seed1)
	rng.SetSeed2(seed2)

	require.Equal(t, rng.Gen(), 0.028870999839968048)
	require.Equal(t, rng.Gen(), 0.11641535266900002)
	require.Equal(t, rng.Gen(), 0.49546379455874096)
}
