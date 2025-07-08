// Copyright 2024 PingCAP, Inc.
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

package session

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/stretchr/testify/require"
)

func TestGetStartMode(t *testing.T) {
	require.Equal(t, ddl.Normal, getStartMode(currentBootstrapVersion))
	require.Equal(t, ddl.Normal, getStartMode(currentBootstrapVersion+1))
	require.Equal(t, ddl.Upgrade, getStartMode(currentBootstrapVersion-1))
	require.Equal(t, ddl.Bootstrap, getStartMode(0))
}

func TestDDLTableVersionTables(t *testing.T) {
	require.True(t, slices.IsSortedFunc(ddlTableVersionTables, func(a, b versionedDDLTables) int {
		return cmp.Compare(a.ver, b.ver)
	}), "ddlTableVersionTables should be sorted by version")
	allDDLTables := make([]TableBasicInfo, 0, len(ddlTableVersionTables)*2)
	for _, v := range ddlTableVersionTables {
		allDDLTables = append(allDDLTables, v.tables...)
	}
	require.True(t, slices.IsSortedFunc(allDDLTables, func(a, b TableBasicInfo) int {
		return cmp.Compare(b.ID, a.ID)
	}), "ddlTableVersionTables should be sorted by table ID in descending order")
	for _, vt := range allDDLTables {
		require.Equal(t, strings.ToLower(vt.Name), vt.Name, "table name should be in lower case")
		require.Contains(t, vt.SQL, fmt.Sprintf(" mysql.%s (", vt.Name),
			"table SQL should contain table name and follow the format 'mysql.<table_name> ('")
	}
}
