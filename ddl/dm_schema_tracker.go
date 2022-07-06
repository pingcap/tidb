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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package ddl

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/table"
	pumpcli "github.com/pingcap/tidb/tidb-binlog/pump_client"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/dbterror"
)

var _ DDL = SchemaTracker{}

// SchemaTracker is used to track schema changes by DM. It implements DDL interface and by applying DDL, it updates the
// table structure to keep tracked with upstream changes.
type SchemaTracker struct {
	*infoschema.InfoStore
}

// NewSchemaTracker creates a SchemaTracker. lowerCaseTableNames has the same meaning as MySQL variable lower_case_table_names.
func NewSchemaTracker(lowerCaseTableNames int) SchemaTracker {
	return SchemaTracker{
		InfoStore: infoschema.NewInfoStore(lowerCaseTableNames),
	}
}

// CreateSchema implements the DDL interface.
func (d SchemaTracker) CreateSchema(ctx sessionctx.Context, stmt *ast.CreateDatabaseStmt) error {
	// we only consider explicit charset/collate, if not found, fallback to default charset/collate.
	charsetOpt := ast.CharsetOpt{}
	for _, val := range stmt.Options {
		switch val.Tp {
		case ast.DatabaseOptionCharset:
			charsetOpt.Chs = val.Value
		case ast.DatabaseOptionCollate:
			charsetOpt.Col = val.Value
		}
	}

	chs, coll, err := ResolveCharsetCollation(charsetOpt)
	if err != nil {
		return errors.Trace(err)
	}

	dbInfo := &model.DBInfo{Name: stmt.Name, Charset: chs, Collate: coll}
	onExist := OnExistError
	if stmt.IfNotExists {
		onExist = OnExistIgnore
	}
	return d.CreateSchemaWithInfo(ctx, dbInfo, onExist)
}

// CreateSchemaWithInfo implements the DDL interface.
func (d SchemaTracker) CreateSchemaWithInfo(ctx sessionctx.Context, dbInfo *model.DBInfo, onExist OnExist) error {
	oldInfo := d.SchemaByName(dbInfo.Name)
	if oldInfo != nil {
		if onExist == OnExistIgnore {
			return nil
		}
		// not support MariaDB's CREATE OR REPLACE SCHEMA
		return infoschema.ErrDatabaseExists.GenWithStackByArgs(dbInfo.Name)
	}
	d.PutSchema(dbInfo)
	return nil
}

// AlterSchema implements the DDL interface.
func (d SchemaTracker) AlterSchema(ctx sessionctx.Context, stmt *ast.AlterDatabaseStmt) error {
	dbInfo := d.SchemaByName(stmt.Name)
	if dbInfo == nil {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(stmt.Name.O)
	}

	// Resolve target charset and collation from options.
	var (
		toCharset, toCollate string
		err                  error
	)

	for _, val := range stmt.Options {
		switch val.Tp {
		case ast.DatabaseOptionCharset:
			if toCharset == "" {
				toCharset = val.Value
			} else if toCharset != val.Value {
				return dbterror.ErrConflictingDeclarations.GenWithStackByArgs(toCharset, val.Value)
			}
		case ast.DatabaseOptionCollate:
			info, errGetCollate := collate.GetCollationByName(val.Value)
			if errGetCollate != nil {
				return errors.Trace(errGetCollate)
			}
			if toCharset == "" {
				toCharset = info.CharsetName
			} else if toCharset != info.CharsetName {
				return dbterror.ErrConflictingDeclarations.GenWithStackByArgs(toCharset, info.CharsetName)
			}
			toCollate = info.Name
		}
	}
	if toCharset == "" {
		if toCollate, err = charset.GetDefaultCollation(toCharset); err != nil {
			return errors.Trace(err)
		}
	}

	dbInfo.Charset = toCharset
	dbInfo.Collate = toCollate

	return nil
}

// DropSchema implements the DDL interface.
func (d SchemaTracker) DropSchema(ctx sessionctx.Context, stmt *ast.DropDatabaseStmt) error {
	ok := d.DeleteSchema(stmt.Name)
	if !ok {
		if stmt.IfExists {
			return nil
		}
		return infoschema.ErrDatabaseDropExists.GenWithStackByArgs(stmt.Name)
	}
	return nil
}

// CreateTable implements the DDL interface.
func (d SchemaTracker) CreateTable(ctx sessionctx.Context, s *ast.CreateTableStmt) error {
	panic("not implemented")
}

// CreateTableWithInfo implements the DDL interface.
func (d SchemaTracker) CreateTableWithInfo(
	ctx sessionctx.Context,
	dbName model.CIStr,
	info *model.TableInfo,
	onExist OnExist,
) error {
	panic("not implemented")

}

// CreateView implements the DDL interface.
func (d SchemaTracker) CreateView(ctx sessionctx.Context, s *ast.CreateViewStmt) error {
	panic("not implemented")
}

// DropTable implements the DDL interface.
func (d SchemaTracker) DropTable(ctx sessionctx.Context, stmt *ast.DropTableStmt) (err error) {
	panic("not implemented")
}

// RecoverTable implements the DDL interface, which is no-op in DM's case.
func (d SchemaTracker) RecoverTable(ctx sessionctx.Context, recoverInfo *RecoverInfo) (err error) {
	return nil
}

// DropView implements the DDL interface.
func (d SchemaTracker) DropView(ctx sessionctx.Context, stmt *ast.DropTableStmt) (err error) {
	panic("not implemented")
}

// CreateIndex implements the DDL interface.
func (d SchemaTracker) CreateIndex(ctx sessionctx.Context, stmt *ast.CreateIndexStmt) error {
	panic("not implemented")
}

// DropIndex implements the DDL interface.
func (d SchemaTracker) DropIndex(ctx sessionctx.Context, stmt *ast.DropIndexStmt) error {
	panic("not implemented")
}

// AlterTable implements the DDL interface.
func (d SchemaTracker) AlterTable(ctx context.Context, sctx sessionctx.Context, stmt *ast.AlterTableStmt) error {
	panic("not implemented")
}

// TruncateTable implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) TruncateTable(ctx sessionctx.Context, tableIdent ast.Ident) error {
	return nil
}

// RenameTable implements the DDL interface.
func (d SchemaTracker) RenameTable(ctx sessionctx.Context, stmt *ast.RenameTableStmt) error {
	panic("not implemented")
}

// LockTables implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) LockTables(ctx sessionctx.Context, stmt *ast.LockTablesStmt) error {
	return nil
}

// UnlockTables implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) UnlockTables(ctx sessionctx.Context, lockedTables []model.TableLockTpInfo) error {
	return nil

}

// CleanupTableLock implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) CleanupTableLock(ctx sessionctx.Context, tables []*ast.TableName) error {
	return nil

}

// UpdateTableReplicaInfo implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) UpdateTableReplicaInfo(ctx sessionctx.Context, physicalID int64, available bool) error {
	return nil

}

// RepairTable implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) RepairTable(ctx sessionctx.Context, table *ast.TableName, createStmt *ast.CreateTableStmt) error {
	return nil

}

// CreateSequence implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) CreateSequence(ctx sessionctx.Context, stmt *ast.CreateSequenceStmt) error {
	return nil
}

// DropSequence implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) DropSequence(ctx sessionctx.Context, stmt *ast.DropSequenceStmt) (err error) {
	return nil

}

// AlterSequence implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) AlterSequence(ctx sessionctx.Context, stmt *ast.AlterSequenceStmt) error {
	return nil
}

// CreatePlacementPolicy implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) CreatePlacementPolicy(ctx sessionctx.Context, stmt *ast.CreatePlacementPolicyStmt) error {
	return nil

}

// DropPlacementPolicy implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) DropPlacementPolicy(ctx sessionctx.Context, stmt *ast.DropPlacementPolicyStmt) error {
	return nil

}

// AlterPlacementPolicy implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) AlterPlacementPolicy(ctx sessionctx.Context, stmt *ast.AlterPlacementPolicyStmt) error {
	return nil

}

// BatchCreateTableWithInfo implements the DDL interface, it will call CreateTableWithInfo for each table.
func (d SchemaTracker) BatchCreateTableWithInfo(ctx sessionctx.Context, schema model.CIStr, info []*model.TableInfo, onExist OnExist) error {
	panic("not implemented")
}

// CreatePlacementPolicyWithInfo implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) CreatePlacementPolicyWithInfo(ctx sessionctx.Context, policy *model.PolicyInfo, onExist OnExist) error {
	return nil
}

// Start implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) Start(ctxPool *pools.ResourcePool) error {
	return nil

}

// GetLease implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) GetLease() time.Duration {
	return 0
}

// Stats implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) Stats(vars *variable.SessionVars) (map[string]interface{}, error) {
	return nil, nil
}

// GetScope implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) GetScope(status string) variable.ScopeFlag {
	return 0
}

// Stop implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) Stop() error {
	return nil
}

// RegisterStatsHandle implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) RegisterStatsHandle(handle *handle.Handle) {}

// SchemaSyncer implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) SchemaSyncer() util.SchemaSyncer {
	return nil
}

// OwnerManager implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) OwnerManager() owner.Manager {
	return nil
}

// GetID implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) GetID() string {
	return "schema-tracker"
}

// GetTableMaxHandle implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) GetTableMaxHandle(ctx *JobContext, startTS uint64, tbl table.PhysicalTable) (kv.Handle, bool, error) {
	return nil, false, nil
}

// SetBinlogClient implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) SetBinlogClient(client *pumpcli.PumpsClient) {}

// GetHook implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) GetHook() Callback {
	return nil
}

// SetHook implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) SetHook(h Callback) {}

// DoDDLJob implements the DDL interface, it's no-op in DM's case.
func (d SchemaTracker) DoDDLJob(ctx sessionctx.Context, job *model.Job) error {
	return nil
}

var (
	ConstructResultOfShowCreateDatabase func(ctx sessionctx.Context, dbInfo *model.DBInfo, ifNotExists bool, buf *bytes.Buffer) (err error)
	ConstructResultOfShowCreateTable    func(ctx sessionctx.Context, tableInfo *model.TableInfo, allocators autoid.Allocators, buf *bytes.Buffer) (err error)
)

// Checker is used to check the result of SchemaTracker is same as real DDL.
type Checker struct {
	RealDDL *ddl
	Tracker SchemaTracker

	closed bool
}

// NewChecker creates a Checker.
func NewChecker(realDDL DDL) *Checker {
	return &Checker{
		RealDDL: realDDL.(*ddl),
		Tracker: NewSchemaTracker(2),
	}
}

// Disable turns off check.
func (d *Checker) Disable() {
	d.closed = true
}

// Enable turns on check.
func (d *Checker) Enable() {
	d.closed = false
}

func (d Checker) checkDBInfo(ctx sessionctx.Context, dbName model.CIStr) {
	if d.closed {
		return
	}
	dbInfo, _ := d.RealDDL.GetInfoSchemaWithInterceptor(ctx).SchemaByName(dbName)
	dbInfo2 := d.Tracker.SchemaByName(dbName)

	result := bytes.NewBuffer(make([]byte, 0, 512))
	err := ConstructResultOfShowCreateDatabase(ctx, dbInfo, false, result)
	if err != nil {
		panic(err)
	}
	result2 := bytes.NewBuffer(make([]byte, 0, 512))
	err = ConstructResultOfShowCreateDatabase(ctx, dbInfo2, false, result2)
	if err != nil {
		panic(err)
	}
	s1 := result.String()
	s2 := result2.String()
	if s1 != s2 {
		errStr := fmt.Sprintf("%s != %s", s1, s2)
		panic(errStr)
	}
}

func (d Checker) CreateSchema(ctx sessionctx.Context, stmt *ast.CreateDatabaseStmt) error {
	err := d.RealDDL.CreateSchema(ctx, stmt)
	if err != nil {
		return err
	}
	err = d.Tracker.CreateSchema(ctx, stmt)
	if err != nil {
		panic(err)
	}

	d.checkDBInfo(ctx, stmt.Name)
	return nil
}

func (d Checker) AlterSchema(sctx sessionctx.Context, stmt *ast.AlterDatabaseStmt) error {
	err := d.RealDDL.AlterSchema(sctx, stmt)
	if err != nil {
		return err
	}
	err = d.Tracker.AlterSchema(sctx, stmt)
	if err != nil {
		panic(err)
	}

	d.checkDBInfo(sctx, stmt.Name)
	return nil
}

func (d Checker) DropSchema(ctx sessionctx.Context, stmt *ast.DropDatabaseStmt) error {
	err := d.RealDDL.DropSchema(ctx, stmt)
	if err != nil {
		return err
	}
	err = d.Tracker.DropSchema(ctx, stmt)
	if err != nil {
		panic(err)
	}
	return nil
}

func (d Checker) CreateTable(ctx sessionctx.Context, stmt *ast.CreateTableStmt) error {
	//TODO implement me
	panic("implement me")
}

func (d Checker) CreateView(ctx sessionctx.Context, stmt *ast.CreateViewStmt) error {
	//TODO implement me
	panic("implement me")
}

func (d Checker) DropTable(ctx sessionctx.Context, stmt *ast.DropTableStmt) (err error) {
	//TODO implement me
	panic("implement me")
}

func (d Checker) RecoverTable(ctx sessionctx.Context, recoverInfo *RecoverInfo) (err error) {
	//TODO implement me
	panic("implement me")
}

func (d Checker) DropView(ctx sessionctx.Context, stmt *ast.DropTableStmt) (err error) {
	//TODO implement me
	panic("implement me")
}

func (d Checker) CreateIndex(ctx sessionctx.Context, stmt *ast.CreateIndexStmt) error {
	//TODO implement me
	panic("implement me")
}

func (d Checker) DropIndex(ctx sessionctx.Context, stmt *ast.DropIndexStmt) error {
	//TODO implement me
	panic("implement me")
}

func (d Checker) AlterTable(ctx context.Context, sctx sessionctx.Context, stmt *ast.AlterTableStmt) error {
	//TODO implement me
	panic("implement me")
}

func (d Checker) TruncateTable(ctx sessionctx.Context, tableIdent ast.Ident) error {
	//TODO implement me
	panic("implement me")
}

func (d Checker) RenameTable(ctx sessionctx.Context, stmt *ast.RenameTableStmt) error {
	//TODO implement me
	panic("implement me")
}

func (d Checker) LockTables(ctx sessionctx.Context, stmt *ast.LockTablesStmt) error {
	//TODO implement me
	panic("implement me")
}

func (d Checker) UnlockTables(ctx sessionctx.Context, lockedTables []model.TableLockTpInfo) error {
	//TODO implement me
	panic("implement me")
}

func (d Checker) CleanupTableLock(ctx sessionctx.Context, tables []*ast.TableName) error {
	//TODO implement me
	panic("implement me")
}

func (d Checker) UpdateTableReplicaInfo(ctx sessionctx.Context, physicalID int64, available bool) error {
	//TODO implement me
	panic("implement me")
}

func (d Checker) RepairTable(ctx sessionctx.Context, table *ast.TableName, createStmt *ast.CreateTableStmt) error {
	//TODO implement me
	panic("implement me")
}

func (d Checker) CreateSequence(ctx sessionctx.Context, stmt *ast.CreateSequenceStmt) error {
	//TODO implement me
	panic("implement me")
}

func (d Checker) DropSequence(ctx sessionctx.Context, stmt *ast.DropSequenceStmt) (err error) {
	//TODO implement me
	panic("implement me")
}

func (d Checker) AlterSequence(ctx sessionctx.Context, stmt *ast.AlterSequenceStmt) error {
	//TODO implement me
	panic("implement me")
}

func (d Checker) CreatePlacementPolicy(ctx sessionctx.Context, stmt *ast.CreatePlacementPolicyStmt) error {
	//TODO implement me
	panic("implement me")
}

func (d Checker) DropPlacementPolicy(ctx sessionctx.Context, stmt *ast.DropPlacementPolicyStmt) error {
	//TODO implement me
	panic("implement me")
}

func (d Checker) AlterPlacementPolicy(ctx sessionctx.Context, stmt *ast.AlterPlacementPolicyStmt) error {
	//TODO implement me
	panic("implement me")
}

func (d Checker) CreateSchemaWithInfo(ctx sessionctx.Context, info *model.DBInfo, onExist OnExist) error {
	err := d.RealDDL.CreateSchemaWithInfo(ctx, info, onExist)
	if err != nil {
		return err
	}
	err = d.Tracker.CreateSchemaWithInfo(ctx, info, onExist)
	if err != nil {
		panic(err)
	}

	d.checkDBInfo(ctx, info.Name)
	return nil
}

func (d Checker) CreateTableWithInfo(ctx sessionctx.Context, schema model.CIStr, info *model.TableInfo, onExist OnExist) error {
	//TODO implement me
	panic("implement me")
}

func (d Checker) BatchCreateTableWithInfo(ctx sessionctx.Context, schema model.CIStr, info []*model.TableInfo, onExist OnExist) error {
	//TODO implement me
	panic("implement me")
}

func (d Checker) CreatePlacementPolicyWithInfo(ctx sessionctx.Context, policy *model.PolicyInfo, onExist OnExist) error {
	//TODO implement me
	panic("implement me")
}

func (d Checker) Start(ctxPool *pools.ResourcePool) error {
	return d.RealDDL.Start(ctxPool)
}

func (d Checker) GetLease() time.Duration {
	return d.RealDDL.GetLease()
}

func (d Checker) Stats(vars *variable.SessionVars) (map[string]interface{}, error) {
	return d.RealDDL.Stats(vars)
}

func (d Checker) GetScope(status string) variable.ScopeFlag {
	return d.RealDDL.GetScope(status)
}

func (d Checker) Stop() error {
	return d.RealDDL.Stop()
}

func (d Checker) RegisterStatsHandle(h *handle.Handle) {
	d.RealDDL.RegisterStatsHandle(h)
}

func (d Checker) SchemaSyncer() util.SchemaSyncer {
	return d.RealDDL.SchemaSyncer()
}

func (d Checker) OwnerManager() owner.Manager {
	return d.RealDDL.OwnerManager()
}

func (d Checker) GetID() string {
	return d.RealDDL.GetID()
}

func (d Checker) GetTableMaxHandle(ctx *JobContext, startTS uint64, tbl table.PhysicalTable) (kv.Handle, bool, error) {
	return d.RealDDL.GetTableMaxHandle(ctx, startTS, tbl)
}

func (d Checker) SetBinlogClient(client *pumpcli.PumpsClient) {
	d.RealDDL.SetBinlogClient(client)
}

func (d Checker) GetHook() Callback {
	return d.RealDDL.GetHook()
}

func (d Checker) SetHook(h Callback) {
	d.RealDDL.SetHook(h)
}

func (d Checker) DoDDLJob(ctx sessionctx.Context, job *model.Job) error {
	return d.RealDDL.DoDDLJob(ctx, job)
}
