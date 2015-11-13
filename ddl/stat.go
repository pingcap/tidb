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
	ddl_server_id            = "ddl_server_id"
	ddl_schema_version       = "ddl_schema_version"
	ddl_owner_id             = "ddl_owner_id"
	ddl_owner_last_update_ts = "ddl_owner_last_update_ts"
	ddl_job_id               = "ddl_job_id"
	ddl_job_action           = "ddl_job_action"
	ddl_job_last_update_ts   = "ddl_job_last_update_ts"
	ddl_job_state            = "ddl_job_state"
	ddl_job_error            = "ddl_job_error"
	ddl_job_schema_state     = "ddl_job_schema_state"
	ddl_job_schema_id        = "ddl_job_schema_id"
	ddl_job_table_id         = "ddl_job_table_id"
	ddl_job_snapshot_ver     = "ddl_job_snapshot_ver"
	ddl_job_reorg_handle     = "ddl_job_reorg_handle"
	ddl_job_args             = "ddl_job_args"
)

var defaultStatusVars map[string]variables.ScopeFlag = map[string]variables.ScopeFlag{
	{ScopeGlobal | ScopeSession, ddl_server_id},
	{ScopeGlobal | ScopeSession, ddl_schema_version},
	{ScopeGlobal | ScopeSession, ddl_owner_id},
	{ScopeGlobal | ScopeSession, ddl_owner_last_update_ts},
	{ScopeGlobal | ScopeSession, ddl_job_id},
	{ScopeGlobal | ScopeSession, ddl_job_action},
	{ScopeGlobal | ScopeSession, ddl_job_last_update_ts},
	{ScopeGlobal | ScopeSession, ddl_job_state},
	{ScopeGlobal | ScopeSession, ddl_job_error},
	{ScopeGlobal | ScopeSession, ddl_job_schema_state},
	{ScopeGlobal | ScopeSession, ddl_job_schema_id},
	{ScopeGlobal | ScopeSession, ddl_job_table_id},
	{ScopeGlobal | ScopeSession, ddl_job_snapshot_ver},
	{ScopeGlobal | ScopeSession, ddl_job_reorg_handle},
	{ScopeGlobal | ScopeSession, ddl_job_args},
	{ScopeGlobal | ScopeSession, ddl_last_reload_schema_ts},
}

func (d *ddl) SetStatusScope(status string, scope variables.ScopeFlag) {
	defaultStatusVars[status] = scope
}

func (d *ddl) FillStatusVal(status string, value interface{}) variables.StatusVal {
	ok, scope := defaultStatusVars[status]
	if !ok {
		scope = variables.DefaultScopeFlag
	}

	return &variables.StatusVal{Scope: scope, Value: value}
}

func (d *ddl) Stat() (map[string]*variables.StatusVal, error) {
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
	m[ddl_server_id] = d.FillStatusVal(ddl_server_id, d.uuid)

	m[ddl_schema_version] = d.FillStatusVal(ddl_schema_version, schemaVer)

	if owner != nil {
		m[ddl_owner_id] = d.FillStatusVal(ddl_owner_id, owner.OwnerID)
		// LastUpdateTs uses nanosecond.
		m[ddl_owner_last_update_ts] = d.FillStatusVal(ddl_owner_last_update_ts, owner.LastUpdateTS/1e9)
	}

	if job != nil {
		m[ddl_job_id] = d.FillStatusVal(ddl_job_id, job.ID)
		m[ddl_job_action] = d.FillStatusVal(ddl_job_action, job.Type.String())
		m[ddl_job_last_update_ts] = d.FillStatusVal(ddl_job_last_update_ts, job.LastUpdateTS)
		m[ddl_job_state] = d.FillStatusVal(ddl_job_state, job.State.String())
		m[ddl_job_error] = d.FillStatusVal(ddl_job_error, job.Error)
		m[ddl_job_schema_state] = d.FillStatusVal(ddl_job_schema_state, job.SchemaState.String())
		m[ddl_job_schema_id] = d.FillStatusVal(ddl_job_schema_id, job.SchemaID)
		m[ddl_job_table_id] = d.FillStatusVal(ddl_job_table_id, job.TableID)
		m[ddl_job_snapshot_ver] = d.FillStatusVal(ddl_job_snapshot_ver, job.SnapshotVer)
		m[ddl_job_reorg_handle] = d.FillStatusVal(ddl_job_reorg_handle, reorgHandle)
		m[ddl_job_args] = d.FillStatusVal(ddl_job_args, job.Args)
	}

	return m, nil
}
