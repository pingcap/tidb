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

package tikv

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExceedEndKey(t *testing.T) {
	tests := []struct {
		name     string
		current  []byte
		endKey   []byte
		expected bool
	}{
		{
			name:     "empty end key",
			current:  []byte("abc"),
			endKey:   nil,
			expected: false,
		},
		{
			name:     "empty end key with slice",
			current:  []byte("abc"),
			endKey:   []byte{},
			expected: false,
		},
		{
			name:     "current equals end key",
			current:  []byte("abc"),
			endKey:   []byte("abc"),
			expected: true,
		},
		{
			name:     "current greater than end key",
			current:  []byte("bcd"),
			endKey:   []byte("abc"),
			expected: true,
		},
		{
			name:     "current less than end key",
			current:  []byte("abc"),
			endKey:   []byte("bcd"),
			expected: false,
		},
		{
			name:     "current empty, end key not empty",
			current:  []byte{},
			endKey:   []byte("abc"),
			expected: false,
		},
		{
			name:     "both empty",
			current:  []byte{},
			endKey:   []byte{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := exceedEndKey(tt.current, tt.endKey)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSortAndDedupHashVals(t *testing.T) {
	tests := []struct {
		name     string
		input    []uint64
		expected []uint64
	}{
		{
			name:     "empty slice",
			input:    []uint64{},
			expected: []uint64{},
		},
		{
			name:     "single element",
			input:    []uint64{1},
			expected: []uint64{1},
		},
		{
			name:     "already sorted without duplicates",
			input:    []uint64{1, 2, 3, 4, 5},
			expected: []uint64{1, 2, 3, 4, 5},
		},
		{
			name:     "unsorted without duplicates",
			input:    []uint64{5, 3, 1, 4, 2},
			expected: []uint64{1, 2, 3, 4, 5},
		},
		{
			name:     "with duplicates",
			input:    []uint64{3, 1, 4, 1, 5, 9, 2, 6, 5, 3},
			expected: []uint64{1, 2, 3, 4, 5, 6, 9},
		},
		{
			name:     "all same elements",
			input:    []uint64{7, 7, 7, 7},
			expected: []uint64{7},
		},
		{
			name:     "two elements same",
			input:    []uint64{3, 3},
			expected: []uint64{3},
		},
		{
			name:     "two different elements",
			input:    []uint64{3, 1},
			expected: []uint64{1, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy to avoid modifying the original test data
			input := slices.Clone(tt.input)

			result := sortAndDedupHashVals(input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSafeCopy(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected []byte
	}{
		{
			name:     "nil input",
			input:    nil,
			expected: nil,
		},
		{
			name:     "empty slice",
			input:    []byte{},
			expected: []byte{},
		},
		{
			name:     "single byte",
			input:    []byte{65},
			expected: []byte{65},
		},
		{
			name:     "multiple bytes",
			input:    []byte("hello world"),
			expected: []byte("hello world"),
		},
		{
			name:     "binary data",
			input:    []byte{0, 1, 2, 255, 254},
			expected: []byte{0, 1, 2, 255, 254},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := safeCopy(tt.input)

			// Check that content is equal
			assert.Equal(t, tt.expected, result)

			// Check that it's a different slice (not the same memory)
			if len(tt.input) > 0 {
				// Modify the original to ensure copy is independent
				original := slices.Clone(tt.input)
				tt.input[0] = 255 // Modify original

				// The copy should still match the expected value
				assert.Equal(t, tt.expected, result)

				// And should not be affected by the modification
				if len(original) > 0 && original[0] != 255 {
					assert.NotEqual(t, tt.input[0], result[0])
				}
			}
		})
	}
}
