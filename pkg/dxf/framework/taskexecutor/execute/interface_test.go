// Copyright 2025 PingCAP, Inc.
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

package execute

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSubtaskSummaryGetSpeed(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(*SubtaskSummary)
		endTime     time.Time
		duration    time.Duration
		expected    int64
		description string
	}{
		{
			name: "insufficient data points",
			setup: func(s *SubtaskSummary) {
				s.Progresses = []Progress{
					{Bytes: 100, UpdateTime: time.Unix(1000, 0)},
				}
			},
			endTime:     time.Unix(1010, 0),
			duration:    time.Second * 10,
			expected:    0,
			description: "should return 0 when less than 2 data points",
		},
		{
			name: "no overlap with data range",
			setup: func(s *SubtaskSummary) {
				baseTime := time.Unix(1000, 0)
				s.Progresses = []Progress{
					{Bytes: 0, UpdateTime: baseTime},
					{Bytes: 100, UpdateTime: baseTime.Add(1 * time.Second)},
				}
			},
			endTime:     time.Unix(1010, 0),
			duration:    time.Second, // [9, 10]
			expected:    0,
			description: "should return 0 when time range doesn't overlap with data",
		},
		{
			name: "partial time range overlap",
			setup: func(s *SubtaskSummary) {
				baseTime := time.Unix(1000, 0)
				s.Progresses = []Progress{
					{Bytes: 0, UpdateTime: baseTime},
					{Bytes: 50, UpdateTime: baseTime.Add(1 * time.Second)},
					{Bytes: 100, UpdateTime: baseTime.Add(2 * time.Second)},
					{Bytes: 150, UpdateTime: baseTime.Add(3 * time.Second)},
				}
			},
			endTime:     time.Unix(1002, 500000000),
			duration:    time.Second, // [1.5, 2.5]
			expected:    50,
			description: "should handle partial time range overlap correctly",
		},
		{
			name: "partial time range overlap",
			setup: func(s *SubtaskSummary) {
				baseTime := time.Unix(1000, 0)
				s.Progresses = []Progress{
					{Bytes: 0, UpdateTime: baseTime},
					{Bytes: 30, UpdateTime: baseTime.Add(1 * time.Second)},
					{Bytes: 60, UpdateTime: baseTime.Add(2 * time.Second)},
					{Bytes: 90, UpdateTime: baseTime.Add(3 * time.Second)},
				}
			},
			endTime:     time.Unix(1004, 0),
			duration:    time.Millisecond * 1500, // [1002.5, 1004]
			expected:    10,
			description: "should handle partial time range overlap correctly",
		},
		{
			name: "multiple overlapping",
			setup: func(s *SubtaskSummary) {
				baseTime := time.Unix(1000, 0)
				s.Progresses = []Progress{
					{Bytes: 0, UpdateTime: baseTime},
					{Bytes: 60, UpdateTime: baseTime.Add(1 * time.Second)},
					{Bytes: 120, UpdateTime: baseTime.Add(2 * time.Second)},
					{Bytes: 180, UpdateTime: baseTime.Add(3 * time.Second)},
					{Bytes: 240, UpdateTime: baseTime.Add(4 * time.Second)},
				}
			},
			endTime:     time.Unix(1004, 500000000),
			duration:    time.Second * 2, // [1002.5, 1004.5]
			expected:    45,
			description: "should handle multiple overlapping segments correctly",
		},
		{
			name: "exact match the range",
			setup: func(s *SubtaskSummary) {
				baseTime := time.Unix(1000, 0)
				s.Progresses = []Progress{
					{Bytes: 0, UpdateTime: baseTime},
					{Bytes: 60, UpdateTime: baseTime.Add(1 * time.Second)},
					{Bytes: 120, UpdateTime: baseTime.Add(2 * time.Second)},
					{Bytes: 180, UpdateTime: baseTime.Add(3 * time.Second)},
					{Bytes: 240, UpdateTime: baseTime.Add(4 * time.Second)},
				}
			},
			endTime:     time.Unix(1004, 0),
			duration:    time.Second * 4, // [1000, 1004]
			expected:    60,
			description: "should handle range correctly",
		},
		{
			name: "whole range",
			setup: func(s *SubtaskSummary) {
				baseTime := time.Unix(1001, 0)
				s.Progresses = []Progress{
					{Bytes: 0, UpdateTime: baseTime},
					{Bytes: 60, UpdateTime: baseTime.Add(1 * time.Second)},
					{Bytes: 120, UpdateTime: baseTime.Add(2 * time.Second)},
					{Bytes: 180, UpdateTime: baseTime.Add(3 * time.Second)},
					{Bytes: 240, UpdateTime: baseTime.Add(4 * time.Second)},
				}
			},
			endTime:     time.Unix(1006, 500000000),
			duration:    time.Second * 6, // [1000, 1006]
			expected:    40,
			description: "should handle multiple overlapping segments correctly",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SubtaskSummary{}
			tt.setup(s)

			result := s.GetSpeedInTimeRange(tt.endTime, tt.duration)

			require.Equal(t, tt.expected, result, tt.description)
		})
	}
}
