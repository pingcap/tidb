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
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/variable"
)

var (
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

// Stat returns the DDL statistics.
func (d *ddl) Stats() (map[string]interface{}, error) {
	var (
		owner       *model.Owner
		job         *model.Job
		schemaVer   int64
		reorgHandle int64
	)
	err := kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err error
		owner, err = t.GetDDLOwner()
		if err != nil {
			return errors.Trace(err)
		}

		job, err = t.GetDDLJob(0)
		if err != nil {
			return errors.Trace(err)
		}

		schemaVer, err = t.GetSchemaVersion()
		if err != nil {
			return errors.Trace(err)
		}

		if job != nil {
			reorgHandle, err = t.GetDDLReorgHandle(job)
			if err != nil {
				return errors.Trace(err)
			}
		}

		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	m := make(map[string]interface{})
	m[ddlServerID] = d.uuid

	m[ddlSchemaVersion] = schemaVer

	if owner != nil {
		m[ddlOwnerID] = owner.OwnerID
		// LastUpdateTS uses nanosecond.
		m[ddlOwnerLastUpdateTS] = owner.LastUpdateTS / 1e9
	}

	if job != nil {
		m[ddlJobID] = job.ID
		m[ddlJobAction] = job.Type.String()
		m[ddlJobLastUpdateTS] = job.LastUpdateTS / 1e9
		m[ddlJobState] = job.State.String()
		m[ddlJobError] = job.Error
		m[ddlJobSchemaState] = job.SchemaState.String()
		m[ddlJobSchemaID] = job.SchemaID
		m[ddlJobTableID] = job.TableID
		m[ddlJobSnapshotVer] = job.SnapshotVer
		m[ddlJobReorgHandle] = reorgHandle
		m[ddlJobArgs] = job.Args
	}

	return m, nil
}
