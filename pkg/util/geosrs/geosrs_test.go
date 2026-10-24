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

package geosrs

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSupported(t *testing.T) {
	require.True(t, Supported(SRID0))
	require.True(t, Supported(SRID4326))
	// 3857 is in MySQL's catalog but not in TiDB's.
	for _, srid := range []uint32{1, 3857, 4269, 4325, 4327, math.MaxUint32} {
		require.False(t, Supported(srid), "SRID %d", srid)
	}
}
