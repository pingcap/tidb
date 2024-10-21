// Copyright 2022 PingCAP, Inc.
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

package window

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func GetRollingPolicy() *RollingPolicy[float64] {
	w := NewWindow[float64](Options{Size: 3})
	return NewRollingPolicy[float64](w, RollingPolicyOpts{BucketDuration: 100 * time.Millisecond})
}

func TestRollingPolicy_Add(t *testing.T) {
	// test func timespan return real span
	tests := []struct {
		timeSleep []int
		offset    []int
		points    []int
	}{
		{
			timeSleep: []int{150, 51},
			offset:    []int{1, 2},
			points:    []int{1, 1},
		},
		{
			timeSleep: []int{94, 250},
			offset:    []int{0, 0},
			points:    []int{1, 1},
		},
		{
			timeSleep: []int{150, 300, 600},
			offset:    []int{1, 1, 1},
			points:    []int{1, 1, 1},
		},
	}

	for _, test := range tests {
		t.Run("test policy add", func(t *testing.T) {
			var totalTs, lastOffset int
			timeSleep := test.timeSleep
			policy := GetRollingPolicy()
			for i, n := range timeSleep {
				totalTs += n
				time.Sleep(time.Duration(n) * time.Millisecond)
				policy.Add(float64(test.points[i]))
				offset, points := test.offset[i], test.points[i]

				require.Equal(t, points, int(policy.window.buckets[offset].Points[0]),
					fmt.Sprintf("error, time since last append: %vms, last offset: %v", totalTs, lastOffset))
				lastOffset = offset
			}
		})
	}
}

func TestRollingPolicy_AddWithTimespan(t *testing.T) {
	t.Run("timespan < bucket number", func(t *testing.T) {
		policy := GetRollingPolicy()
		// bucket 0
		policy.Add(0)
		// bucket 1
		time.Sleep(101 * time.Millisecond)
		policy.Add(1)
		// bucket 2
		time.Sleep(101 * time.Millisecond)
		policy.Add(2)
		// bucket 1
		time.Sleep(201 * time.Millisecond)
		policy.Add(4)

		for _, bkt := range policy.window.buckets {
			t.Logf("%+v", bkt)
		}

		require.Equal(t, 0, len(policy.window.buckets[0].Points))
		require.Equal(t, 4, int(policy.window.buckets[1].Points[0]))
		require.Equal(t, 2, int(policy.window.buckets[2].Points[0]))
	})

	t.Run("timespan > bucket number", func(t *testing.T) {
		policy := GetRollingPolicy()

		// bucket 0
		policy.Add(0)
		// bucket 1
		time.Sleep(101 * time.Millisecond)
		policy.Add(1)
		// bucket 2
		time.Sleep(101 * time.Millisecond)
		policy.Add(2)
		// bucket 1
		time.Sleep(501 * time.Millisecond)
		policy.Add(4)

		for _, bkt := range policy.window.buckets {
			t.Logf("%+v", bkt)
		}

		require.Equal(t, 0, len(policy.window.buckets[0].Points))
		require.Equal(t, 4, int(policy.window.buckets[1].Points[0]))
		require.Equal(t, 0, len(policy.window.buckets[2].Points))
	})
}
