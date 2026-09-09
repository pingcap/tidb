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

package collate

import (
	"testing"

	"github.com/pingcap/tidb/pkg/util/collate/ucadata"
	"github.com/stretchr/testify/require"
)

func TestUnicode0900BoundaryRuneUsesImplicitWeight(t *testing.T) {
	collator := &unicode0900AICICollator{}
	lastTableRune := rune(len(ucadata.DUCET0900Table.MapTable4) - 1)
	boundaryRune := lastTableRune + 1
	boundaryStr := string(boundaryRune)

	// The first rune outside MapTable4 must use the fallback path instead of indexing the table.
	lastFirst, lastSecond := convertRuneUnicodeCI0900(lastTableRune)
	require.Equal(t, ucadata.DUCET0900Table.MapTable4[lastTableRune], lastFirst)
	require.Zero(t, lastSecond)

	first, second := convertRuneUnicodeCI0900(boundaryRune)
	require.NotZero(t, first)
	require.Zero(t, second)
	require.Greater(t, first, lastFirst)
	require.NotEmpty(t, collator.Key(boundaryStr))
	require.Equal(t, -1, collator.Compare(string(lastTableRune), boundaryStr))
	require.Equal(t, 0, collator.Compare(boundaryStr, boundaryStr))
	require.Equal(t, -1, collator.Compare(boundaryStr, string(boundaryRune+1)))

	pattern := collator.Pattern()
	pattern.Compile(boundaryStr, '\\')
	require.True(t, pattern.DoMatch(boundaryStr))
}
