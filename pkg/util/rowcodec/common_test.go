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

package rowcodec

import (
	"encoding/hex"
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
)

func TestRemoveKeyspacePrefix(t *testing.T) {
	// this is the default and expected value for UT, else some UT might change
	// them and forget to revert them back.
	require.True(t, intest.InTest)
	require.False(t, kv.StandAloneTiDB)
	nextGenKey, err := hex.DecodeString("78000001748000fffffffffffe5F728000000000000002")
	require.NoError(t, err)
	classicKey := nextGenKey[4:]

	if kerneltype.IsClassic() {
		require.EqualValues(t, nextGenKey, RemoveKeyspacePrefix(nextGenKey))
		require.EqualValues(t, classicKey, RemoveKeyspacePrefix(classicKey))
		return
	}

	t.Run("when intest enabled, keyspace prefix should be removed", func(t *testing.T) {
		require.EqualValues(t, nextGenKey[4:], RemoveKeyspacePrefix(nextGenKey))
		require.EqualValues(t, classicKey, RemoveKeyspacePrefix(classicKey))
		kv.StandAloneTiDB = true
		t.Cleanup(func() {
			kv.StandAloneTiDB = false
		})
		require.EqualValues(t, nextGenKey[4:], RemoveKeyspacePrefix(nextGenKey))
		require.EqualValues(t, classicKey, RemoveKeyspacePrefix(classicKey))
	})

	t.Run("when intest disabled, keyspace prefix should be removed only when run in standalone mode", func(t *testing.T) {
		intest.InTest = false
		t.Cleanup(func() {
			intest.InTest = true
		})
		require.EqualValues(t, nextGenKey, RemoveKeyspacePrefix(nextGenKey))
		require.EqualValues(t, classicKey, RemoveKeyspacePrefix(classicKey))
		kv.StandAloneTiDB = true
		t.Cleanup(func() {
			kv.StandAloneTiDB = false
		})
		require.EqualValues(t, nextGenKey[4:], RemoveKeyspacePrefix(nextGenKey))
		require.EqualValues(t, classicKey, RemoveKeyspacePrefix(classicKey))
	})
}
