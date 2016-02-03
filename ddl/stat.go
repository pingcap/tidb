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
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/inspectkv"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/variable"
)

var (
	ddlPrefix            = "ddl"
	ddlServerID          = "ddl_server_id"
	ddlSchemaVersion     = "ddl_schema_version"
	ddlOwnerID           = "ddl_owner_id"
	ddlOwnerLastUpdateTS = "ddl_owner_last_update_ts"
	ddlJobID             = "ddl_job_id"
	ddlJobAction         = "ddl_job_action"
	ddlJobLastUpdateTS   = "ddl_job_last_update_ts"
	ddlJobState          = "ddl_job_state"
	ddlJobError          = "ddl_job_error"
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
	return variable.DefaultScopeFlag
}

func buildPrefix(str, old, new string) string {
	return strings.Replace(str, old, new, 1)
}

func stats(store kv.Storage, m map[string]interface{}, flag JobType) error {
	var prefix string
	var info *inspectkv.DDLInfo

	err := kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		var err1 error
		switch flag {
		case ddlJobFlag:
			info, err1 = inspectkv.GetDDLInfo(txn)
			prefix = flag.String()
		case bgJobFlag:
			info, err1 = inspectkv.GetDDLBgInfo(txn)
			prefix = flag.String()
		default:
			err1 = errInvalidJobFlag
		}

		return errors.Trace(err1)
	})
	if err != nil {
		return errors.Trace(err)
	}

	m[buildPrefix(ddlSchemaVersion, ddlPrefix, prefix)] = info.SchemaVer

	if info.Owner != nil {
		m[buildPrefix(ddlOwnerID, ddlPrefix, prefix)] = info.Owner.OwnerID
		// LastUpdateTS uses nanosecond.
		m[buildPrefix(ddlOwnerLastUpdateTS, ddlPrefix, prefix)] = info.Owner.LastUpdateTS / 1e9
	}

	if info.Job == nil {
		return nil
	}

	m[buildPrefix(ddlJobID, ddlPrefix, prefix)] = info.Job.ID
	m[buildPrefix(ddlJobAction, ddlPrefix, prefix)] = info.Job.Type.String()
	m[buildPrefix(ddlJobLastUpdateTS, ddlPrefix, prefix)] = info.Job.LastUpdateTS / 1e9
	m[buildPrefix(ddlJobState, ddlPrefix, prefix)] = info.Job.State.String()
	m[buildPrefix(ddlJobError, ddlPrefix, prefix)] = info.Job.Error
	m[buildPrefix(ddlJobSchemaState, ddlPrefix, prefix)] = info.Job.SchemaState.String()
	m[buildPrefix(ddlJobSchemaID, ddlPrefix, prefix)] = info.Job.SchemaID
	m[buildPrefix(ddlJobTableID, ddlPrefix, prefix)] = info.Job.TableID
	m[buildPrefix(ddlJobSnapshotVer, ddlPrefix, prefix)] = info.Job.SnapshotVer
	m[buildPrefix(ddlJobReorgHandle, ddlPrefix, prefix)] = info.ReorgHandle
	m[buildPrefix(ddlJobArgs, ddlPrefix, prefix)] = info.Job.Args

	return nil
}

// Stat returns the DDL statistics.
func (d *ddl) Stats() (map[string]interface{}, error) {
	m := make(map[string]interface{})
	m[ddlServerID] = d.uuid

	if err := stats(d.store, m, ddlJobFlag); err != nil {
		return nil, errors.Trace(err)
	}
	if err := stats(d.store, m, bgJobFlag); err != nil {
		return nil, errors.Trace(err)
	}

	return m, nil
}
