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

package importinto

import (
	"testing"

	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/stretchr/testify/require"
)

func TestKVGroupConflictInfosAddConflictInfo(t *testing.T) {
	// Test adding with zero count, should do nothing.
	gci := &KVGroupConflictInfos{}
	gci.addDataConflictInfo(&engineapi.ConflictInfo{Count: 0})
	require.Nil(t, gci.ConflictInfos)

	// Test adding to a nil map.
	info1 := &engineapi.ConflictInfo{Count: 10}
	gci.addDataConflictInfo(info1)
	require.NotNil(t, gci.ConflictInfos)
	require.Len(t, gci.ConflictInfos, 1)
	require.EqualValues(t, 10, gci.ConflictInfos[external.DataKVGroup].Count)

	// Test adding a new group.
	info2 := &engineapi.ConflictInfo{Count: 20}
	gci.addIndexConflictInfo(1, info2)
	require.Len(t, gci.ConflictInfos, 2)
	require.EqualValues(t, 20, gci.ConflictInfos["1"].Count)

	// Test merging into an existing group.
	info3 := &engineapi.ConflictInfo{Count: 5}
	gci.addDataConflictInfo(info3)
	require.Len(t, gci.ConflictInfos, 2)
	require.EqualValues(t, 15, gci.ConflictInfos[external.DataKVGroup].Count)

	// Test merging with zero count, should not change anything.
	gci.addDataConflictInfo(&engineapi.ConflictInfo{Count: 0})
	require.EqualValues(t, 15, gci.ConflictInfos[external.DataKVGroup].Count)
}
