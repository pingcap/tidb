// Copyright 2023 PingCAP, Inc.
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

package backend_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/stretchr/testify/require"
)

func TestSPFreshBuildRoutingUsesEndpointShardIDs(t *testing.T) {
	endpoints := []string{
		"http://worker-1:19000",
		"http://worker-2:19000",
		"http://worker-3:19000",
		"http://worker-4:19000",
		"http://worker-5:19000",
		"http://worker-6:19000",
		"http://worker-7:19000",
		"http://worker-8:19000",
		"http://worker-9:19000",
		"http://worker-10:19000",
	}
	routePlan := &backend.SPFreshBuildRoutePlan{
		KeyspaceID:      1,
		JobID:           2,
		PhysicalTableID: 3,
		IndexID:         4,
		BaseSnapshotTS:  5,
		OwnerEndpoints:  endpoints,
	}
	routing, err := backend.NewSPFreshBuildRoutingConfig(routePlan)
	require.NoError(t, err)
	routePlan.OwnerEndpoints[0] = "http://mutated:19000"

	firstRoute, err := routing.RouteForWriterSlot(1)
	require.NoError(t, err)
	require.Equal(t, backend.SPFreshBuildShardOwner{
		BuildShardID: "1-2-3-4-endpoint-1",
		Owner:        "http://worker-1:19000",
	}, firstRoute)

	wrappedRoute, err := routing.RouteForWriterSlot(11)
	require.NoError(t, err)
	require.Equal(t, firstRoute, wrappedRoute)

	allOwners, err := routePlan.AllBuildShardOwners()
	require.NoError(t, err)
	require.Len(t, allOwners, len(endpoints))
	require.Equal(t, backend.SPFreshBuildShardOwner{
		BuildShardID: "1-2-3-4-endpoint-10",
		Owner:        endpoints[9],
	}, allOwners[9])

	routePlan.OwnerEndpoints = append(routePlan.OwnerEndpoints, routePlan.OwnerEndpoints[0])
	require.ErrorContains(t, routePlan.Validate(), "duplicate owner endpoint")

	routePlan.OwnerEndpoints = make([]string, 1025)
	for i := range routePlan.OwnerEndpoints {
		routePlan.OwnerEndpoints[i] = fmt.Sprintf("http://worker-%d:19000", i)
	}
	require.ErrorContains(t, routePlan.Validate(), "max 1024")
}
