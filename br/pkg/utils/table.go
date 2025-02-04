// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// TemporaryCleanupTablePrefix is the prefix for tables that will be cleaned up after restore
const TemporaryCleanupTablePrefix = "__TiDB_BR_Cleanup_"

// TemporaryCleanupTableName makes a cleanup table name by adding the cleanup prefix
func TemporaryCleanupTableName(tableName string) ast.CIStr {
	return ast.NewCIStr(TemporaryCleanupTablePrefix + tableName)
}
