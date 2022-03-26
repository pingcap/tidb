// Copyright 2022 PingCAP, Inc.
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

package filter

import (
	"strings"
)

var (
	// DMHeartbeatSchema is the heartbeat schema name
	DMHeartbeatSchema = "DM_HEARTBEAT"
	// DMHeartbeatTable is heartbeat table name
	DMHeartbeatTable = "HEARTBEAT"
	// InformationSchemaName is the `INFORMATION_SCHEMA` database name.
	InformationSchemaName = "INFORMATION_SCHEMA"
	// PerformanceSchemaName is the `PERFORMANCE_SCHEMA` database name.
	PerformanceSchemaName = "PERFORMANCE_SCHEMA"
	// MetricSchemaName is the `METRICS_SCHEMA` database name.
	MetricSchemaName = "METRICS_SCHEMA"
	// InspectionSchemaName is the `INSPECTION_SCHEMA` database name
	InspectionSchemaName = "INSPECTION_SCHEMA"
)

// IsSystemSchema checks whether schema is system schema or not.
// case insensitive
func IsSystemSchema(schema string) bool {
	schema = strings.ToUpper(schema)
	switch schema {
	case DMHeartbeatSchema, // do not create table in it manually
		"SYS",   // https://dev.mysql.com/doc/refman/8.0/en/sys-schema.html
		"MYSQL", // the name of system database.
		InformationSchemaName,
		InspectionSchemaName,
		PerformanceSchemaName,
		MetricSchemaName:
		return true
	default:
		return false
	}
}
