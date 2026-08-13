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

package vardef

import (
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/stretchr/testify/require"
)

func TestIsReadOnlyVarInNextGen(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("This test is only for next-gen TiDB")
	}
	require.False(t, IsReadOnlyVarInNextGen("abc"))
	require.True(t, IsReadOnlyVarInNextGen(TiDBEnableMDL))
	require.True(t, IsReadOnlyVarInNextGen("TIDB_ENABLE_METADATA_LOCK"))
	require.True(t, IsReadOnlyVarInNextGen(TiDBMaxDistTaskNodes))
	require.True(t, IsReadOnlyVarInNextGen(TiDBDDLReorgMaxWriteSpeed))
	require.True(t, IsReadOnlyVarInNextGen(TiDBDDLDiskQuota))
	require.True(t, IsReadOnlyVarInNextGen(TiDBDDLEnableFastReorg))
	require.True(t, IsReadOnlyVarInNextGen(TiDBEnableDistTask))
}
