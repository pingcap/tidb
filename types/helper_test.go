// Copyright 2018 PingCAP, Inc.
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

package types

import (
	"strconv"
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestStrToInt(t *testing.T) {
	tests := []struct {
		input  string
		output string
		err    error
	}{
		{"9223372036854775806", "9223372036854775806", nil},
		{"9223372036854775807", "9223372036854775807", nil},
		{"9223372036854775808", "9223372036854775807", ErrBadNumber},
		{"-9223372036854775807", "-9223372036854775807", nil},
		{"-9223372036854775808", "-9223372036854775808", nil},
		{"-9223372036854775809", "-9223372036854775808", ErrBadNumber},
	}
	for _, tt := range tests {
		output, err := strToInt(tt.input)
		require.Equal(t, tt.err, errors.Cause(err))
		require.Equal(t, tt.output, strconv.FormatInt(output, 10))
	}
}

func TestTruncate(t *testing.T) {
	tests := []struct {
		f        float64
		dec      int
		expected float64
	}{
		{123.45, 0, 123},
		{123.45, 1, 123.4},
		{123.45, 2, 123.45},
		{123.45, 3, 123.450},
	}
	for _, tt := range tests {
		res := Truncate(tt.f, tt.dec)
		require.Equal(t, tt.expected, res)
	}
}

func TestTruncateFloatToString(t *testing.T) {
	tests := []struct {
		f        float64
		dec      int
		expected string
	}{
		{12.13, -1, "10"},
		{13.15, 0, "13"},
		{0, 2, "0"},
		{0.001, 2, "0"},
		{0.539, 2, "0.53"},
		{0.9951, 2, "0.99"},
		{1, 2, "1"},
		{-0.456, 2, "-0.45"},
	}
	for _, tt := range tests {
		res := TruncateFloatToString(tt.f, tt.dec)
		require.Equal(t, tt.expected, res)
	}
}
