// Copyright 2026 PingCAP, Inc.
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
package restore

import (
	"context"
	"regexp"
	"testing"

	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func (rc *Client) CheckPrivilegeTableRowsCollateCompatibility(
	ctx context.Context,
	dbNameL, tableNameL string,
	upstreamTable, downstreamTable *model.TableInfo,
) error {
	return rc.checkPrivilegeTableRowsCollateCompatibility(ctx, dbNameL, tableNameL, upstreamTable, downstreamTable)
}

func testTableInfo(name string) *model.TableInfo {
	return &model.TableInfo{
		Name: model.NewCIStr(name),
	}
}

func TestGenerateResetSQL(t *testing.T) {
	// case #1: ignore non-mysql databases
	mockDB := &database{
		ExistingTables: map[string]*model.TableInfo{},
		Name:           model.NewCIStr("non-mysql"),
		TemporaryName:  utils.TemporaryDBName("non-mysql"),
	}
	for name := range sysPrivilegeTableMap {
		mockDB.ExistingTables[name] = testTableInfo(name)
	}
	resetUsers := []string{"cloud_admin", "root"}
	require.Equal(t, 0, len(generateResetSQLs(mockDB, resetUsers)))

	// case #2: ignore non expected table
	mockDB = &database{
		ExistingTables: map[string]*model.TableInfo{},
		Name:           model.NewCIStr("mysql"),
		TemporaryName:  utils.TemporaryDBName("mysql"),
	}
	for name := range sysPrivilegeTableMap {
		name += "non_available"
		mockDB.ExistingTables[name] = testTableInfo(name)
	}
	resetUsers = []string{"cloud_admin", "root"}
	require.Equal(t, 0, len(generateResetSQLs(mockDB, resetUsers)))

	// case #3: only reset cloud admin account
	for name := range sysPrivilegeTableMap {
		mockDB.ExistingTables[name] = testTableInfo(name)
	}
	resetUsers = []string{"cloud_admin"}
	sqls := generateResetSQLs(mockDB, resetUsers)
	require.Equal(t, 8, len(sqls))
	for _, sql := range sqls {
		// for cloud_admin we only generate DELETE sql
		require.Regexp(t, regexp.MustCompile("DELETE*"), sql)
	}

	// case #4: reset cloud admin/other account
	resetUsers = []string{"cloud_admin", "cloud_other"}
	sqls = generateResetSQLs(mockDB, resetUsers)
	require.Equal(t, 16, len(sqls))
	for _, sql := range sqls {
		// for cloud_admin/cloud_other we only generate DELETE sql
		require.Regexp(t, regexp.MustCompile("DELETE*"), sql)
	}

	// case #5: reset cloud admin && root account
	resetUsers = []string{"cloud_admin", "root"}
	sqls = generateResetSQLs(mockDB, resetUsers)
	// 8 DELETE sqls for cloud admin and 1 UPDATE sql for root
	require.Equal(t, 9, len(sqls))
}
