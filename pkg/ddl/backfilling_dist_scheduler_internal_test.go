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

package ddl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type regionMetaForTest struct {
	startKey []byte
}

func (r regionMetaForTest) StartKey() []byte {
	return r.startKey
}

func TestDeduplicateSortedRegionMetasByStartKey(t *testing.T) {
	regions := []regionMetaForTest{
		{startKey: []byte("a")},
		{startKey: []byte("a")},
		{startKey: []byte("b")},
		{startKey: []byte("b")},
		{startKey: []byte("d")},
	}

	regions = deduplicateSortedRegionMetasByStartKey(regions)

	require.Equal(t, []regionMetaForTest{
		{startKey: []byte("a")},
		{startKey: []byte("b")},
		{startKey: []byte("d")},
	}, regions)
}
