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

package registry

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/br/pkg/restore/nameroute"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/types"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/stretchr/testify/require"
)

func TestNormalizeRegistrationRoutes(t *testing.T) {
	first := RegistrationInfo{RouteStrings: []string{"b.t:z.t", "a:b"}}
	second := RegistrationInfo{RouteStrings: []string{"a:b", "b.t:z.t"}}
	require.NoError(t, normalizeRegistrationRoutes(&first))
	require.NoError(t, normalizeRegistrationRoutes(&second))
	require.Equal(t, first.RouteStrings, second.RouteStrings)
	require.Equal(t, first.RouteHash, second.RouteHash)
	require.Len(t, first.RouteHash, 64)

	invalid := RegistrationInfo{RouteHash: "unexpected"}
	require.Error(t, normalizeRegistrationRoutes(&invalid))

	large := RegistrationInfo{RouteStrings: make([]string, 2500)}
	for i := range large.RouteStrings {
		large.RouteStrings[i] = fmt.Sprintf("source.t%04d:target.t%04d", i, i)
	}
	require.NoError(t, normalizeRegistrationRoutes(&large))
	encoded, err := marshalRouteStrings(large.RouteStrings)
	require.NoError(t, err)
	require.Greater(t, len(encoded), 64*1024)
}

func TestRegistrationClaimsRoutedTarget(t *testing.T) {
	testCases := []struct {
		name        string
		filters     []string
		routes      []string
		targetDB    string
		targetTable string
		claimed     bool
	}{
		{
			name:        "identity route",
			filters:     []string{"target.t"},
			targetDB:    "target",
			targetTable: "t",
			claimed:     true,
		},
		{
			name:        "different source exact routes collide",
			filters:     []string{"source.t"},
			routes:      []string{"source.t:target.t"},
			targetDB:    "target",
			targetTable: "t",
			claimed:     true,
		},
		{
			name:        "schema route claims corresponding table",
			filters:     []string{"source.*"},
			routes:      []string{"source:target"},
			targetDB:    "target",
			targetTable: "t",
			claimed:     true,
		},
		{
			name:        "schema route does not expand one selected table to entire target schema",
			filters:     []string{"source.t1"},
			routes:      []string{"source:target"},
			targetDB:    "target",
			targetTable: "t2",
			claimed:     false,
		},
		{
			name:        "exact table override wins over schema route",
			filters:     []string{"source.t1"},
			routes:      []string{"source:target", "source.t1:other.copy"},
			targetDB:    "target",
			targetTable: "t1",
			claimed:     false,
		},
		{
			name:        "exact table override claims its own target",
			filters:     []string{"source.t1"},
			routes:      []string{"source:target", "source.t1:other.copy"},
			targetDB:    "other",
			targetTable: "copy",
			claimed:     true,
		},
		{
			name:     "exact table route claims its target schema",
			filters:  []string{"source.t1"},
			routes:   []string{"source.t1:target.copy"},
			targetDB: "target",
			claimed:  true,
		},
		{
			name:        "source routed away no longer claims original name",
			filters:     []string{"source.t"},
			routes:      []string{"source.t:other.t"},
			targetDB:    "source",
			targetTable: "t",
			claimed:     false,
		},
		{
			name:        "unselected route source does not claim target",
			filters:     []string{"unrelated.t"},
			routes:      []string{"source.t:target.t"},
			targetDB:    "target",
			targetTable: "t",
			claimed:     false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			f, err := filter.Parse(testCase.filters)
			require.NoError(t, err)
			routerInfo := RegistrationInfo{RouteStrings: testCase.routes}
			require.NoError(t, normalizeRegistrationRoutes(&routerInfo))
			router, err := parseRegistrationRouter(routerInfo)
			require.NoError(t, err)
			require.Equal(t, testCase.claimed,
				registrationClaimsTarget(router, filter.CaseInsensitive(f),
					testCase.targetDB, testCase.targetTable, false))
		})
	}
}

func TestRestoreRegistryRouteSchemaReadiness(t *testing.T) {
	columnNames := []string{"filter_strings", "filter_hash", "source_filter_strings", "route_strings", "route_hash",
		"start_ts", "restored_ts", "upstream_cluster_id", "with_sys_table", "cmd"}
	tableInfo := &model.TableInfo{}
	for offset, name := range columnNames {
		tableInfo.Columns = append(tableInfo.Columns, &model.ColumnInfo{
			Name:   ast.NewCIStr(name),
			Offset: offset,
			State:  model.StatePublic,
		})
	}
	index := &model.IndexInfo{
		Name:   ast.NewCIStr(restoreRegistryRouteIndexName),
		State:  model.StatePublic,
		Unique: true,
	}
	for _, expected := range restoreRegistryRouteIndexColumns {
		index.Columns = append(index.Columns, &model.IndexColumn{
			Name:   ast.NewCIStr(expected.name),
			Length: expected.length,
		})
	}
	tableInfo.Indices = []*model.IndexInfo{index}
	require.True(t, hasRestoreRegistryRouteSchema(tableInfo))
	tableInfo.Indices = append(tableInfo.Indices, &model.IndexInfo{
		Name: ast.NewCIStr(restoreRegistryLegacyIndexName), State: model.StatePublic, Unique: true,
	})
	require.False(t, hasRestoreRegistryRouteSchema(tableInfo))
	tableInfo.Indices = tableInfo.Indices[:1]

	index.Columns[1].Name = ast.NewCIStr("start_ts")
	require.False(t, hasRestoreRegistryRouteSchema(tableInfo))
	index.Columns[1].Name = ast.NewCIStr("route_hash")
	index.Columns[len(index.Columns)-1].Length = types.UnspecifiedLength
	require.False(t, hasRestoreRegistryRouteSchema(tableInfo))
	index.Columns[len(index.Columns)-1].Length = 256
	index.Unique = false
	require.False(t, hasRestoreRegistryRouteSchema(tableInfo))
	index.Unique = true
	tableInfo.Columns[4].State = model.StateWriteOnly
	require.False(t, hasRestoreRegistryRouteSchema(tableInfo))
}

func TestPiTRRegistrationsConflictAtRoutedTargetSchema(t *testing.T) {
	tracker := utils.NewPiTRIdTracker()
	tracker.TrackTableName("current", "orders")
	currentRouter, err := nameroute.Parse([]string{"current.orders:target.orders"})
	require.NoError(t, err)
	registeredRouter, err := nameroute.Parse([]string{"registered.customers:target.customers"})
	require.NoError(t, err)
	registeredFilter, err := filter.Parse([]string{"registered.customers"})
	require.NoError(t, err)

	registry := &Registry{}
	err = registry.checkForTableConflicts(
		tracker,
		nil,
		nil,
		RegistrationInfoWithID{RegistrationInfo: RegistrationInfo{Cmd: "Point Restore"}, restoreID: 1},
		filter.CaseInsensitive(registeredFilter),
		currentRouter,
		registeredRouter,
		2,
	)
	require.Error(t, err)

	disjointRouter, err := nameroute.Parse([]string{"registered.customers:other.customers"})
	require.NoError(t, err)
	require.NoError(t, registry.checkForTableConflicts(
		tracker,
		nil,
		nil,
		RegistrationInfoWithID{RegistrationInfo: RegistrationInfo{Cmd: "Point Restore"}, restoreID: 1},
		filter.CaseInsensitive(registeredFilter),
		currentRouter,
		disjointRouter,
		2,
	))

	emptySchemaTracker := utils.NewPiTRIdTracker()
	emptySchemaTracker.DBNameToTableNames["target"] = map[string]struct{}{}
	identityRouter, err := nameroute.Parse(nil)
	require.NoError(t, err)
	err = registry.checkForTableConflicts(
		emptySchemaTracker,
		nil,
		nil,
		RegistrationInfoWithID{RegistrationInfo: RegistrationInfo{Cmd: "Point Restore"}, restoreID: 1},
		filter.CaseInsensitive(registeredFilter),
		identityRouter,
		registeredRouter,
		2,
	)
	require.Error(t, err)
}

func parseRegistrationRouter(info RegistrationInfo) (*nameroute.Router, error) {
	return nameroute.Parse(info.RouteStrings)
}
