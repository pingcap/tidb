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
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/admin"
)

var (
	serverID             = "server_id"
	ddlSchemaVersion     = "ddl_schema_version"
	ddlOwnerID           = "ddl_owner_id"
	ddlOwnerLastUpdateTS = "ddl_owner_last_update_ts"
	ddlJobID             = "ddl_job_id"
	ddlJobAction         = "ddl_job_action"
	ddlJobStartTS        = "ddl_job_start_ts"
	ddlJobState          = "ddl_job_state"
	ddlJobError          = "ddl_job_error"
	ddlJobRows           = "ddl_job_row_count"
	ddlJobSchemaState    = "ddl_job_schema_state"
	ddlJobSchemaID       = "ddl_job_schema_id"
	ddlJobTableID        = "ddl_job_table_id"
	ddlJobSnapshotVer    = "ddl_job_snapshot_ver"
	ddlJobReorgHandle    = "ddl_job_reorg_handle"
	ddlJobArgs           = "ddl_job_args"
)

// GetScope gets the status variables scope.
func (d *ddl) GetScope(status string) variable.ScopeFlag {
	// Now ddl status variables scope are all default scope.
	return variable.DefaultStatusVarScopeFlag
}

// Stats returns the DDL statistics.
func (d *ddl) Stats(vars *variable.SessionVars) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	m[serverID] = d.uuid
	var ddlInfo *admin.DDLInfo

	err := kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		var err1 error
		ddlInfo, err1 = admin.GetDDLInfo(txn)
		if err1 != nil {
			return errors.Trace(err1)
		}
		return errors.Trace(err1)
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	m[ddlSchemaVersion] = ddlInfo.SchemaVer
	// TODO: Get the owner information.
	if ddlInfo.Job != nil {
		m[ddlJobID] = ddlInfo.Job.ID
		m[ddlJobAction] = ddlInfo.Job.Type.String()
		m[ddlJobStartTS] = ddlInfo.Job.StartTS / 1e9 // unit: second
		m[ddlJobState] = ddlInfo.Job.State.String()
		m[ddlJobRows] = ddlInfo.Job.RowCount
		if ddlInfo.Job.Error == nil {
			m[ddlJobError] = ""
		} else {
			m[ddlJobError] = ddlInfo.Job.Error.Error()
		}
		m[ddlJobSchemaState] = ddlInfo.Job.SchemaState.String()
		m[ddlJobSchemaID] = ddlInfo.Job.SchemaID
		m[ddlJobTableID] = ddlInfo.Job.TableID
		m[ddlJobSnapshotVer] = ddlInfo.Job.SnapshotVer
		m[ddlJobReorgHandle] = ddlInfo.ReorgHandle
		m[ddlJobArgs] = ddlInfo.Job.Args
	}
	return m, nil
}
