// Copyright 2024 PingCAP, Inc.
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

package memo_test

import (
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/planner/cascades/memo"
	"github.com/stretchr/testify/require"
)

func RunGroupIDGenerator_NextGroupID(t *testing.T) {
	g := memo.ExportedNewGroupIDGenerator()
	got := g.NextGroupID()
	require.Equal(t, memo.ExportedGroupID(1), got)
	got = g.NextGroupID()
	require.Equal(t, memo.ExportedGroupID(2), got)
	got = g.NextGroupID()
	require.Equal(t, memo.ExportedGroupID(3), got)

	// adjust the id.
	memo.ExportedSetGroupIDGeneratorID(g, 100)
	got = g.NextGroupID()
	require.Equal(t, memo.ExportedGroupID(101), got)
	got = g.NextGroupID()
	require.Equal(t, memo.ExportedGroupID(102), got)
	got = g.NextGroupID()
	require.Equal(t, memo.ExportedGroupID(103), got)

	memo.ExportedSetGroupIDGeneratorID(g, math.MaxUint64)
	got = g.NextGroupID()
	// rewire to 0.
	require.Equal(t, memo.ExportedGroupID(0), got)
	got = g.NextGroupID()
	require.Equal(t, memo.ExportedGroupID(1), got)
}
