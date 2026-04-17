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

package metadef

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

var (
	// InformationSchemaName is the `INFORMATION_SCHEMA` database name.
	InformationSchemaName = ast.NewCIStr("INFORMATION_SCHEMA")
	// PerformanceSchemaName is the `PERFORMANCE_SCHEMA` database name.
	PerformanceSchemaName = ast.NewCIStr("PERFORMANCE_SCHEMA")
	// MetricSchemaName is the `METRICS_SCHEMA` database name.
	MetricSchemaName = ast.NewCIStr("METRICS_SCHEMA")
	// ClusterTableInstanceColumnName is the `INSTANCE` column name of the cluster table.
	ClusterTableInstanceColumnName = "INSTANCE"
)

const (
	temporaryDBNamePrefix = "__TiDB_BR_Temporary_"
)

// IsMemOrSysDB uses to check whether dbLowerName is memory database or system database.
func IsMemOrSysDB(dbLowerName string) bool {
	return IsMemDB(dbLowerName) || IsSystemRelatedDB(dbLowerName)
}

// IsMemDB checks whether dbLowerName is memory database.
func IsMemDB(dbLowerName string) bool {
	switch dbLowerName {
	case InformationSchemaName.L,
		PerformanceSchemaName.L,
		MetricSchemaName.L:
		return true
	}
	return false
}

// IsSystemRelatedDB checks whether dbLowerName is system related database.
func IsSystemRelatedDB(dbLowerName string) bool {
	return IsSystemDB(dbLowerName) || dbLowerName == mysql.SysDB || dbLowerName == mysql.WorkloadSchema
}

// IsSystemDB checks whether dbLowerName is the system database.
func IsSystemDB(dbLowerName string) bool {
	return dbLowerName == mysql.SystemDB
}

// IsBRRelatedDB checks whether dbOriginName is a temporary database created by BR.
func IsBRRelatedDB(dbOriginName string) bool {
	return strings.HasPrefix(dbOriginName, temporaryDBNamePrefix)
}
