// Copyright 2021 PingCAP, Inc.
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

package server_test

import (
	"runtime"
	"testing"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/stretchr/testify/require"
)

func TestMemoryLeak(t *testing.T) {
	initAndCloseTiDB := func() {
		store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
		require.NoError(t, err)
		defer func() { require.NoError(t, store.Close()) }()

		dom, err := session.BootstrapSession(store)
		require.NoError(t, err)
		dom.Close()
	}

	runtime.GC()
	memStat := runtime.MemStats{}
	runtime.ReadMemStats(&memStat)
	oldHeapInUse := memStat.HeapInuse

	for i := 0; i < 20; i++ {
		initAndCloseTiDB()
	}

	runtime.GC()
	runtime.ReadMemStats(&memStat)
	// before the fix, initAndCloseTiDB for 20 times will cost 900 MB memory, so we test for a quite loose upper bound.
	require.Less(t, memStat.HeapInuse-oldHeapInUse, uint64(300*units.MiB))
}
