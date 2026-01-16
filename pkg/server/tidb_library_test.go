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
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit/testflag"
	"github.com/pingcap/tidb/pkg/util/syncutil"
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

	iterations := 14
	if testflag.Long() {
		iterations = 20
	}
	for range iterations {
		initAndCloseTiDB()
	}

	runtime.GC()
	runtime.ReadMemStats(&memStat)
	// Before the fix, initAndCloseTiDB for 20 times cost ~900 MiB memory, so we test for a loose per-iteration upper bound.
	scaleLimit := func(base uint64) uint64 {
		return base * uint64(iterations) / 20
	}
	if syncutil.EnableDeadlock {
		require.Less(t, memStat.HeapInuse-oldHeapInUse, scaleLimit(uint64(5400*units.MiB)))
	} else {
		require.Less(t, memStat.HeapInuse-oldHeapInUse, scaleLimit(uint64(900*units.MiB)))
	}
}
