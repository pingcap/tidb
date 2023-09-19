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

func TestHangulJamoHasOnlyOneWeight(t *testing.T) {
	for i := 0x1100; i < 0x11FF; i++ {
		require.Equal(t, uint64(0), DUCET0900Table.MapTable4[rune(i)]&0xFFFFFFFFFFFF0000)
	}
}

func TestFirstIsNotZero(t *testing.T) {
	// the logic depends on the fact that at least one of the first uint16 is not 0
	for _, weights := range DUCET0900Table.LongRuneMap {
		require.NotEqual(t, weights[0], 0)
	}
}
