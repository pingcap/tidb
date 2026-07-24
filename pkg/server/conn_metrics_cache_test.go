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

package server

import (
	"testing"

	"github.com/pingcap/tidb/pkg/resourcegroup"
	"github.com/stretchr/testify/require"
)

func TestQueryMetricsObserverCache(t *testing.T) {
	cache := queryMetricsObserverCache{}
	duration, rpc := cache.get("Select", "", resourcegroup.DefaultResourceGroupName)
	cachedDuration, cachedRPC := cache.get("Select", "", resourcegroup.DefaultResourceGroupName)
	require.Same(t, duration, cachedDuration)
	require.Same(t, rpc, cachedRPC)

	otherDuration, sameRPC := cache.get("Select", "", "rg1")
	require.NotSame(t, duration, otherDuration)
	require.Same(t, rpc, sameRPC)
	cachedDuration, cachedRPC = cache.get("Select", "", resourcegroup.DefaultResourceGroupName)
	require.Same(t, duration, cachedDuration)
	require.Same(t, rpc, cachedRPC)

	otherDBDuration, otherDBRPC := cache.get("Select", "test", resourcegroup.DefaultResourceGroupName)
	require.NotSame(t, duration, otherDBDuration)
	require.NotSame(t, rpc, otherDBRPC)

	otherSQLDuration, otherSQLRPC := cache.get("Update", "", resourcegroup.DefaultResourceGroupName)
	require.NotSame(t, otherDuration, otherSQLDuration)
	require.NotSame(t, sameRPC, otherSQLRPC)
}
