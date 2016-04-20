// Copyright 2016 PingCAP, Inc.
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
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
)

func (d *ddl) onCreateForeignKey(t *meta.Meta, job *model.Job) error {
	schemaID := job.SchemaID
	tblInfo, err := d.getTableInfo(t, job)
	if err != nil {
		return errors.Trace(err)
	}

	var (
		fkInfo   model.FKInfo
		fkName   model.CIStr
		fkID     int64
		keys     []*ast.IndexColName
		refTable *ast.TableName
		refKeys  []*ast.IndexColName
		onDelete ast.ReferOptionType
		onUpdate ast.ReferOptionType
	)
	err = job.DecodeArgs(&fkName, &fkID, &keys, &refTable, &refKeys, &onDelete, &onUpdate)
	if err != nil {
		job.State = model.JobCancelled
		return errors.Trace(err)
	}

	fkInfo.ID = fkID
	fkInfo.Cols = make([]model.CIStr, len(keys))
	for i, key := range keys {
		fkInfo.Cols[i] = key.Column.Name
	}

	fkInfo.Name = fkName
	fkInfo.RefTable = refTable.Name
	fkInfo.RefCols = make([]model.CIStr, len(refKeys))
	for i, key := range refKeys {
		fkInfo.RefCols[i] = key.Column.Name
	}

	fkInfo.OnDelete = int(onDelete)
	fkInfo.OnUpdate = int(onUpdate)

	tblInfo.ForeignKeys = append(tblInfo.ForeignKeys, &fkInfo)

	_, err = t.GenSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}

	switch fkInfo.State {
	case model.StateNone:
		// none -> public
		job.SchemaState = model.StatePublic
		fkInfo.State = model.StatePublic
		err = t.UpdateTable(schemaID, tblInfo)
		if err != nil {
			return errors.Trace(err)
		}
		// finish this job
		job.State = model.JobDone
		return nil
	default:
		return ErrInvalidForeignKeyState.Gen("invalid fk state %v", fkInfo.State)
	}
}

func (d *ddl) onDropForeignKey(t *meta.Meta, job *model.Job) error {
	schemaID := job.SchemaID
	tblInfo, err := d.getTableInfo(t, job)

	if err != nil {
		return errors.Trace(err)
	}

	var (
		fkName model.CIStr
		found  bool
		fkInfo model.FKInfo
	)
	err = job.DecodeArgs(&fkName)
	if err != nil {
		job.State = model.JobCancelled
		return errors.Trace(err)
	}

	for _, fk := range tblInfo.ForeignKeys {
		if fk.Name.L == fkName.L {
			found = true
			fkInfo = *fk
		}
	}

	if !found {
		return infoschema.ErrForeignKeyNotExists.Gen("foreign key doesn't exist", fkName)
	}

	nfks := tblInfo.ForeignKeys[:0]
	for _, fk := range tblInfo.ForeignKeys {
		if fk.Name.L != fkName.L {
			nfks = append(nfks, fk)
		}
	}

	tblInfo.ForeignKeys = nfks

	_, err = t.GenSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}

	switch fkInfo.State {
	case model.StatePublic:
		// public -> none
		job.SchemaState = model.StateNone
		fkInfo.State = model.StateNone
		err = t.UpdateTable(schemaID, tblInfo)
		if err != nil {
			return errors.Trace(err)
		}
		// finish this job
		job.State = model.JobDone
		return nil
	default:
		return ErrInvalidForeignKeyState.Gen("invalid fk state %v", fkInfo.State)
	}

}
