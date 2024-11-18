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

package ucadata

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnicode0400IsTheSame(t *testing.T) {
	require.Equal(t, len(DUCET0400Table.MapTable4), len(mapTable))
	for idx := range DUCET0400Table.MapTable4 {
		require.Equal(t, DUCET0400Table.MapTable4[idx], mapTable[idx],
			"0x%X %c: 0x%X in new, 0x%X in old", idx, rune(idx), DUCET0400Table.MapTable4[idx], mapTable[idx])
	}
	for k, v := range DUCET0400Table.LongRuneMap {
		require.Equal(t, v[0], longRuneMap[k][0],
			"%c[0]: 0x%X in new, 0x%X in old", k, v[0], longRuneMap[k][0])
		require.Equal(t, v[1], longRuneMap[k][1],
			"%c[1]: 0x%X in new, 0x%X in old", k, v[1], longRuneMap[k][1])
	}
}

func TestAllItemInLongRUneMapIsUnique(t *testing.T) {
	for k1, v1 := range DUCET0400Table.LongRuneMap {
		for k2, v2 := range DUCET0400Table.LongRuneMap {
			if k1 == k2 {
				continue
			}

			require.NotEqual(t, v1, v2,
				"%c((0x%X) and %c(0x%X) are equal", k1, k1, k2, k2)
		}
	}

	for k1, v1 := range DUCET0900Table.LongRuneMap {
		for k2, v2 := range DUCET0900Table.LongRuneMap {
			if k1 == k2 {
				continue
			}

			require.NotEqual(t, v1, v2,
				"%c((0x%X) and %c(0x%X) are equal", k1, k1, k2, k2)
		}
	}
}
