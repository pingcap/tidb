// Copyright 2015 PingCAP, Inc.
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

package ddl

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

var (
	serverID         = "server_id"
	ddlSchemaVersion = "ddl_schema_version"
)

// GetScope gets the status variables scope.
func (*ddl) GetScope(_ string) variable.ScopeFlag {
	// Now ddl status variables scope are all default scope.
	return variable.DefaultStatusVarScopeFlag
}

// Stats returns the DDL statistics.
func (d *ddl) Stats(_ *variable.SessionVars) (map[string]any, error) {
	s, err := d.sessPool.Get()
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer d.sessPool.Put(s)

	ddlInfo, err := GetDDLInfoWithNewTxn(s)
	if err != nil {
		return nil, errors.Trace(err)
	}
	m := make(map[string]any)
	m[serverID] = d.uuid
	m[ddlSchemaVersion] = ddlInfo.SchemaVer
	return m, nil
}
