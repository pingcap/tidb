// Copyright 2026 PingCAP, Inc.
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

package tikvhandler

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseUintRejectsNegative(t *testing.T) {
	// strconv.ParseUint should reject negative values
	_, err := strconv.ParseUint("-1", 0, 64)
	require.Error(t, err)
	require.Contains(t, err.Error(), "sign")

	// strconv.ParseUint should accept normal values
	v, err := strconv.ParseUint("0", 0, 64)
	require.NoError(t, err)
	require.Equal(t, uint64(0), v)

	v, err = strconv.ParseUint("12345", 0, 64)
	require.NoError(t, err)
	require.Equal(t, uint64(12345), v)

	// strconv.ParseUint should accept max valid value
	v, err = strconv.ParseUint("18446744073709551615", 0, 64)
	require.NoError(t, err)
	require.Equal(t, uint64(18446744073709551615), v)

	// strconv.ParseInt with negative - uint64 cast produces huge number (old behavior)
	oldVal, err := strconv.ParseInt("-1", 0, 64)
	require.NoError(t, err)
	require.Equal(t, int64(-1), oldVal)
	require.Equal(t, uint64(18446744073709551615), uint64(oldVal))
}
