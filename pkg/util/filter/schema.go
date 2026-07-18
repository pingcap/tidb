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

	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/util/intest"
)

var (
	// DMHeartbeatSchema is the heartbeat schema name
	DMHeartbeatSchema = "dm_heartbeat"
	// InspectionSchemaName is the `INSPECTION_SCHEMA` database name
	InspectionSchemaName = "inspection_schema"
)

// IsSystemSchema checks whether schema is system schema or not.
// case insensitive
func IsSystemSchema(schema string) bool {
	intest.AssertFunc(func() bool {
		return schema == strings.ToLower(schema)
	})
	return schema == DMHeartbeatSchema || schema == InspectionSchemaName ||
		metadef.IsMemOrSysDB(schema)
}
