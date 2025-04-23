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
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInternalKey(t *testing.T) {
	inputs := []internalKey{
		{key: []byte{}, keyID: []byte{}},
		{key: []byte{}, keyID: []byte{1, 2, 3, 4}},
		{key: []byte{0}, keyID: []byte{2, 3, 4, 5}},
		{key: []byte{0, 1}, keyID: []byte{3, 4, 5, 6}},
		{key: []byte{0, 1, 2}, keyID: []byte{4, 5, 6, 7}},
		{key: []byte{0, 1, 2, 3}, keyID: []byte{5, 6, 7, 8}},
		{key: []byte{0, 1, 2, 3, 4}, keyID: []byte{6, 7, 8, 9}},
		{key: []byte{0, 1, 2, 3, 4, 5}, keyID: []byte{7, 8, 9, 10}},
		{key: []byte{0, 1, 2, 3, 4, 5, 6}, keyID: []byte{8, 9, 10, 11}},
		{key: []byte{0, 1, 2, 3, 4, 5, 6, 7}, keyID: []byte{9, 10, 11, 12}},
		{key: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8}, keyID: []byte{10, 11, 12, 13}},
	}

	encoded := make([][]byte, 0, len(inputs))
	for _, input := range inputs {
		output := encodeInternalKey(nil, input)
		var decoded internalKey
		require.NoError(t, decodeInternalKey(output, &decoded))
		if len(input.key) == 0 {
			require.Empty(t, decoded.key)
		} else {
			require.Equal(t, input.key, decoded.key)
		}
		if len(input.keyID) == 0 {
			require.Empty(t, decoded.keyID)
		} else {
			require.Equal(t, input.keyID, decoded.keyID)
		}
		encoded = append(encoded, output)
	}

	for i := 0; i < len(inputs); i++ {
		for j := i + 1; j < len(inputs); j++ {
			require.Equalf(t,
				compareInternalKey(inputs[i], inputs[j]),
				bytes.Compare(encoded[i], encoded[j]),
				"the order of encoded keys should be the same as the order of internal keys",
			)
		}
	}
}
