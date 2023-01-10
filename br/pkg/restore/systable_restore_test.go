// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"regexp"
	"testing"

	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/parser/model"
	"github.com/stretchr/testify/require"
)

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
