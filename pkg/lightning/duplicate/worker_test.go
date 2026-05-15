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

package duplicate

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenSplitKey(t *testing.T) {
	testCases := []struct {
		startKey []byte
		endKey   []byte
		splitKey []byte
	}{
		{
			startKey: []byte{1, 2},
			endKey:   []byte{1, 2},
			splitKey: []byte{1, 2},
		},
		{
			startKey: []byte{1, 2},
			endKey:   []byte{1, 2, 3, 4, 5},
			splitKey: []byte{1, 2, 1},
		},
		{
			startKey: []byte{1, 2, 3, 4, 5, 6},
			endKey:   []byte{1, 2, 5, 6, 7, 8},
			splitKey: []byte{1, 2, 4},
		},
		{
			startKey: []byte{1, 2, 3, 4},
			endKey:   []byte{1, 2, 4, 5},
			splitKey: []byte{1, 2, 3, 0xff},
		},
		{
			startKey: []byte{1, 2, 3, 0xff, 4},
			endKey:   []byte{1, 2, 4, 5},
			splitKey: []byte{1, 2, 3, 0xff, 0xff},
		},
		{
			startKey: []byte{1, 2, 3, 0xff, 0xff},
			endKey:   []byte{1, 2, 4, 5},
			splitKey: []byte{1, 2, 3, 0xff, 0xff, 0xff},
		},
	}

	for _, tc := range testCases {
		splitKey := genSplitKey(tc.startKey, tc.endKey)
		require.Equal(t, tc.splitKey, splitKey)
	}
}
