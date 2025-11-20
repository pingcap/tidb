// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConvertReadableSizeToByteSize(t *testing.T) {
	var ci configInspection

	testcases := []struct {
		input  string
		output uint64
		err    bool
	}{
		{"100", 100, false},
		{"abc", 0, true},
		{"1KiB", 1024, false},
		{"1MiB", 1048576, false},
		{"1GiB", 1073741824, false},
		{"1TiB", 1099511627776, false},
		{"1PiB", 1125899906842624, false},
		{"100B", 100, false},
	}

	for _, tc := range testcases {
		r, err := ci.convertReadableSizeToByteSize(tc.input)
		if tc.err {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, tc.output, r)
		}
	}
}
