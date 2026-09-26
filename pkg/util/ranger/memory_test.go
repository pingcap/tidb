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

package ranger

import (
	"testing"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/stretchr/testify/require"
)

func TestEstimateMemUsageForBuildFromIn(t *testing.T) {
	points := make([]*point, 0, 4)
	points = append(points,
		&point{value: types.NewIntDatum(1)},
		&point{value: types.NewStringDatum("keep-extra-payload-only")},
	)

	expected := int64(cap(points))*size.SizeOfPointer + int64(len(points))*emptyPointSize
	for _, pt := range points {
		expected += pt.value.MemUsage() - types.EmptyDatumSize
	}

	require.Equal(t, expected, estimateMemUsageForBuildFromIn(points))
}
