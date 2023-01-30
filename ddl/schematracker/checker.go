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

package schematracker

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/syncer"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table"
	pumpcli "github.com/pingcap/tidb/tidb-binlog/pump_client"
)

var (
	// ConstructResultOfShowCreateDatabase should be assigned to executor.ConstructResultOfShowCreateDatabase.
	// It is used to break cycle import.
	ConstructResultOfShowCreateDatabase func(sessionctx.Context, *model.DBInfo, bool, *bytes.Buffer) error
	// ConstructResultOfShowCreateTable should be assigned to executor.ConstructResultOfShowCreateTable.
	// It is used to break cycle import.
	ConstructResultOfShowCreateTable func(sessionctx.Context, *model.TableInfo, autoid.Allocators, *bytes.Buffer) error
)

func init() {
	mockstore.DDLCheckerInjector = NewStorageDDLInjector
}

// Checker is used to check the result of SchemaTracker is same as real DDL.
type Checker struct {
	realDDL ddl.DDL
	tracker SchemaTracker

	closed bool
}

// NewChecker creates a Checker.
func NewChecker(realDDL ddl.DDL) *Checker {
	return &Checker{
		realDDL: realDDL,
		tracker: NewSchemaTracker(2),
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

// CreateTestDB creates a `test` database like the default behaviour of TiDB.
func (d Checker) CreateTestDB() {
	d.tracker.CreateTestDB()
}

func (d Checker) checkDBInfo(ctx sessionctx.Context, dbName model.CIStr) {
	if d.closed {
		return
	}
	dbInfo, _ := d.realDDL.GetInfoSchemaWithInterceptor(ctx).SchemaByName(dbName)
	dbInfo2 := d.tracker.SchemaByName(dbName)

	if dbInfo == nil || dbInfo2 == nil {
		if dbInfo == nil && dbInfo2 == nil {
			return
		}
		errStr := fmt.Sprintf("inconsistent dbInfo, dbName: %s, real ddl: %p, schematracker：%p", dbName, dbInfo, dbInfo2)
		panic(errStr)
	}

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

func (d Checker) checkTableInfo(ctx sessionctx.Context, dbName, tableName model.CIStr) {
	if d.closed {
		return
	}

	if dbName.L == mysql.SystemDB {
		// no need to check system tables.
		return
	}

	tableInfo, _ := d.realDDL.GetInfoSchemaWithInterceptor(ctx).TableByName(dbName, tableName)
	tableInfo2, _ := d.tracker.TableByName(dbName, tableName)

	if tableInfo == nil || tableInfo2 == nil {
		if tableInfo == nil && tableInfo2 == nil {
			return
		}
		errStr := fmt.Sprintf("inconsistent tableInfo, dbName: %s, tableName: %s, real ddl: %p, schematracker：%p",
			dbName, tableName, tableInfo, tableInfo2)
		panic(errStr)
	}

	result := bytes.NewBuffer(make([]byte, 0, 512))
	err := ConstructResultOfShowCreateTable(ctx, tableInfo.Meta(), autoid.Allocators{}, result)
	if err != nil {
		panic(err)
	}
	result2 := bytes.NewBuffer(make([]byte, 0, 512))
	err = ConstructResultOfShowCreateTable(ctx, tableInfo2, autoid.Allocators{}, result2)
	if err != nil {
		panic(err)
	}

	// SchemaTracker will always use NONCLUSTERED so it can support more types of DDL.
	removeClusteredIndexComment := func(s string) string {
		ret := strings.ReplaceAll(s, " /*T![clustered_index] NONCLUSTERED */", "")
		ret = strings.ReplaceAll(ret, " /*T![clustered_index] CLUSTERED */", "")
		return ret
	}

	s1 := removeClusteredIndexComment(result.String())
	s2 := removeClusteredIndexComment(result2.String())

	if s1 != s2 {
		errStr := fmt.Sprintf("%s != %s", s1, s2)
		panic(errStr)
	}
}

// CreateSchema implements the DDL interface.
func (d Checker) CreateSchema(ctx sessionctx.Context, stmt *ast.CreateDatabaseStmt) error {
	err := d.realDDL.CreateSchema(ctx, stmt)
	if err != nil {
		return err
	}
	err = d.tracker.CreateSchema(ctx, stmt)
	if err != nil {
		panic(err)
	}

	d.checkDBInfo(ctx, stmt.Name)
	return nil
}

// AlterSchema implements the DDL interface.
func (d Checker) AlterSchema(sctx sessionctx.Context, stmt *ast.AlterDatabaseStmt) error {
	err := d.realDDL.AlterSchema(sctx, stmt)
	if err != nil {
		return err
	}
	err = d.tracker.AlterSchema(sctx, stmt)
	if err != nil {
		panic(err)
	}

	d.checkDBInfo(sctx, stmt.Name)
	return nil
}

// DropSchema implements the DDL interface.
func (d Checker) DropSchema(ctx sessionctx.Context, stmt *ast.DropDatabaseStmt) error {
	err := d.realDDL.DropSchema(ctx, stmt)
	if err != nil {
		return err
	}
	err = d.tracker.DropSchema(ctx, stmt)
	if err != nil {
		panic(err)
	}

	d.checkDBInfo(ctx, stmt.Name)
	return nil
}

// RecoverSchema implements the DDL interface.
func (d Checker) RecoverSchema(ctx sessionctx.Context, recoverSchemaInfo *ddl.RecoverSchemaInfo) (err error) {
	return nil
}

// CreateTable implements the DDL interface.
func (d Checker) CreateTable(ctx sessionctx.Context, stmt *ast.CreateTableStmt) error {
	err := d.realDDL.CreateTable(ctx, stmt)
	if err != nil {
		return err
	}

	// some unit test will also check warnings, we reset the warnings after SchemaTracker use session context again.
	count := ctx.GetSessionVars().StmtCtx.WarningCount()
	// backup old session variables because CreateTable will change them.
	strictSQLMode := ctx.GetSessionVars().StrictSQLMode
	enableClusteredIndex := ctx.GetSessionVars().EnableClusteredIndex

	err = d.tracker.CreateTable(ctx, stmt)
	if err != nil {
		panic(err)
	}

	ctx.GetSessionVars().StrictSQLMode = strictSQLMode
	ctx.GetSessionVars().EnableClusteredIndex = enableClusteredIndex
	ctx.GetSessionVars().StmtCtx.TruncateWarnings(int(count))

	d.checkTableInfo(ctx, stmt.Table.Schema, stmt.Table.Name)
	return nil
}

// CreateView implements the DDL interface.
func (d Checker) CreateView(ctx sessionctx.Context, stmt *ast.CreateViewStmt) error {
	err := d.realDDL.CreateView(ctx, stmt)
	if err != nil {
		return err
	}
	err = d.tracker.CreateView(ctx, stmt)
	if err != nil {
		panic(err)
	}

	d.checkTableInfo(ctx, stmt.ViewName.Schema, stmt.ViewName.Name)
	return nil
}

// DropTable implements the DDL interface.
func (d Checker) DropTable(ctx sessionctx.Context, stmt *ast.DropTableStmt) (err error) {
	err = d.realDDL.DropTable(ctx, stmt)
	_ = d.tracker.DropTable(ctx, stmt)

	for _, tableName := range stmt.Tables {
		d.checkTableInfo(ctx, tableName.Schema, tableName.Name)
	}
	return err
}

// RecoverTable implements the DDL interface.
func (d Checker) RecoverTable(ctx sessionctx.Context, recoverInfo *ddl.RecoverInfo) (err error) {
	//TODO implement me
	panic("implement me")
}

// FlashbackCluster implements the DDL interface.
func (d Checker) FlashbackCluster(ctx sessionctx.Context, flashbackTS uint64) (err error) {
	//TODO implement me
	panic("implement me")
}

// DropView implements the DDL interface.
func (d Checker) DropView(ctx sessionctx.Context, stmt *ast.DropTableStmt) (err error) {
	err = d.realDDL.DropView(ctx, stmt)
	if err != nil {
		return err
	}
	err = d.tracker.DropView(ctx, stmt)
	if err != nil {
		panic(err)
	}

	for _, tableName := range stmt.Tables {
		d.checkTableInfo(ctx, tableName.Schema, tableName.Name)
	}
	return nil
}

// CreateIndex implements the DDL interface.
func (d Checker) CreateIndex(ctx sessionctx.Context, stmt *ast.CreateIndexStmt) error {
	err := d.realDDL.CreateIndex(ctx, stmt)
	if err != nil {
		return err
	}
	err = d.tracker.CreateIndex(ctx, stmt)
	if err != nil {
		panic(err)
	}

	d.checkTableInfo(ctx, stmt.Table.Schema, stmt.Table.Name)
	return nil
}

// DropIndex implements the DDL interface.
func (d Checker) DropIndex(ctx sessionctx.Context, stmt *ast.DropIndexStmt) error {
	err := d.realDDL.DropIndex(ctx, stmt)
	if err != nil {
		return err
	}
	err = d.tracker.DropIndex(ctx, stmt)
	if err != nil {
		panic(err)
	}

	d.checkTableInfo(ctx, stmt.Table.Schema, stmt.Table.Name)
	return nil
}

// AlterTable implements the DDL interface.
func (d Checker) AlterTable(ctx context.Context, sctx sessionctx.Context, stmt *ast.AlterTableStmt) error {
	err := d.realDDL.AlterTable(ctx, sctx, stmt)
	if err != nil {
		return err
	}

	// some unit test will also check warnings, we reset the warnings after SchemaTracker use session context again.
	count := sctx.GetSessionVars().StmtCtx.WarningCount()
	err = d.tracker.AlterTable(ctx, sctx, stmt)
	if err != nil {
		panic(err)
	}
	sctx.GetSessionVars().StmtCtx.TruncateWarnings(int(count))

	d.checkTableInfo(sctx, stmt.Table.Schema, stmt.Table.Name)
	return nil
}

// TruncateTable implements the DDL interface.
func (d Checker) TruncateTable(ctx sessionctx.Context, tableIdent ast.Ident) error {
	//TODO implement me
	panic("implement me")
}

// RenameTable implements the DDL interface.
func (d Checker) RenameTable(ctx sessionctx.Context, stmt *ast.RenameTableStmt) error {
	err := d.realDDL.RenameTable(ctx, stmt)
	if err != nil {
		return err
	}
	err = d.tracker.RenameTable(ctx, stmt)
	if err != nil {
		panic(err)
	}

	for _, tableName := range stmt.TableToTables {
		d.checkTableInfo(ctx, tableName.OldTable.Schema, tableName.OldTable.Name)
		d.checkTableInfo(ctx, tableName.NewTable.Schema, tableName.NewTable.Name)
	}
	return nil
}

// LockTables implements the DDL interface.
func (d Checker) LockTables(ctx sessionctx.Context, stmt *ast.LockTablesStmt) error {
	return d.realDDL.LockTables(ctx, stmt)
}

// UnlockTables implements the DDL interface.
func (d Checker) UnlockTables(ctx sessionctx.Context, lockedTables []model.TableLockTpInfo) error {
	return d.realDDL.UnlockTables(ctx, lockedTables)
}

// CleanupTableLock implements the DDL interface.
func (d Checker) CleanupTableLock(ctx sessionctx.Context, tables []*ast.TableName) error {
	return d.realDDL.CleanupTableLock(ctx, tables)
}

// UpdateTableReplicaInfo implements the DDL interface.
func (d Checker) UpdateTableReplicaInfo(ctx sessionctx.Context, physicalID int64, available bool) error {
	//TODO implement me
	panic("implement me")
}

// RepairTable implements the DDL interface.
func (d Checker) RepairTable(ctx sessionctx.Context, table *ast.TableName, createStmt *ast.CreateTableStmt) error {
	//TODO implement me
	panic("implement me")
}

// CreateSequence implements the DDL interface.
func (d Checker) CreateSequence(ctx sessionctx.Context, stmt *ast.CreateSequenceStmt) error {
	//TODO implement me
	panic("implement me")
}

// DropSequence implements the DDL interface.
func (d Checker) DropSequence(ctx sessionctx.Context, stmt *ast.DropSequenceStmt) (err error) {
	//TODO implement me
	panic("implement me")
}

// AlterSequence implements the DDL interface.
func (d Checker) AlterSequence(ctx sessionctx.Context, stmt *ast.AlterSequenceStmt) error {
	//TODO implement me
	panic("implement me")
}

// CreatePlacementPolicy implements the DDL interface.
func (d Checker) CreatePlacementPolicy(ctx sessionctx.Context, stmt *ast.CreatePlacementPolicyStmt) error {
	//TODO implement me
	panic("implement me")
}

// DropPlacementPolicy implements the DDL interface.
func (d Checker) DropPlacementPolicy(ctx sessionctx.Context, stmt *ast.DropPlacementPolicyStmt) error {
	//TODO implement me
	panic("implement me")
}

// AlterPlacementPolicy implements the DDL interface.
func (d Checker) AlterPlacementPolicy(ctx sessionctx.Context, stmt *ast.AlterPlacementPolicyStmt) error {
	//TODO implement me
	panic("implement me")
}

// CreateResourceGroup implements the DDL interface.
// ResourceGroup do not affect the transaction.
func (d Checker) CreateResourceGroup(ctx sessionctx.Context, stmt *ast.CreateResourceGroupStmt) error {
	return nil
}

// DropResourceGroup implements the DDL interface.
func (d Checker) DropResourceGroup(ctx sessionctx.Context, stmt *ast.DropResourceGroupStmt) error {
	return nil
}

// AlterResourceGroup implements the DDL interface.
func (d Checker) AlterResourceGroup(ctx sessionctx.Context, stmt *ast.AlterResourceGroupStmt) error {
	return nil
}

// CreateSchemaWithInfo implements the DDL interface.
func (d Checker) CreateSchemaWithInfo(ctx sessionctx.Context, info *model.DBInfo, onExist ddl.OnExist) error {
	err := d.realDDL.CreateSchemaWithInfo(ctx, info, onExist)
	if err != nil {
		return err
	}
	err = d.tracker.CreateSchemaWithInfo(ctx, info, onExist)
	if err != nil {
		panic(err)
	}

	d.checkDBInfo(ctx, info.Name)
	return nil
}

// CreateTableWithInfo implements the DDL interface.
func (d Checker) CreateTableWithInfo(ctx sessionctx.Context, schema model.CIStr, info *model.TableInfo, cs ...ddl.CreateTableWithInfoConfigurier) error {
	//TODO implement me
	panic("implement me")
}

// BatchCreateTableWithInfo implements the DDL interface.
func (d Checker) BatchCreateTableWithInfo(ctx sessionctx.Context, schema model.CIStr, info []*model.TableInfo, cs ...ddl.CreateTableWithInfoConfigurier) error {
	//TODO implement me
	panic("implement me")
}

// CreatePlacementPolicyWithInfo implements the DDL interface.
func (d Checker) CreatePlacementPolicyWithInfo(ctx sessionctx.Context, policy *model.PolicyInfo, onExist ddl.OnExist) error {
	//TODO implement me
	panic("implement me")
}

// Start implements the DDL interface.
func (d Checker) Start(ctxPool *pools.ResourcePool) error {
	return d.realDDL.Start(ctxPool)
}

// GetLease implements the DDL interface.
func (d Checker) GetLease() time.Duration {
	return d.realDDL.GetLease()
}

// Stats implements the DDL interface.
func (d Checker) Stats(vars *variable.SessionVars) (map[string]interface{}, error) {
	return d.realDDL.Stats(vars)
}

// GetScope implements the DDL interface.
func (d Checker) GetScope(status string) variable.ScopeFlag {
	return d.realDDL.GetScope(status)
}

// Stop implements the DDL interface.
func (d Checker) Stop() error {
	return d.realDDL.Stop()
}

// RegisterStatsHandle implements the DDL interface.
func (d Checker) RegisterStatsHandle(h *handle.Handle) {
	d.realDDL.RegisterStatsHandle(h)
}

// SchemaSyncer implements the DDL interface.
func (d Checker) SchemaSyncer() syncer.SchemaSyncer {
	return d.realDDL.SchemaSyncer()
}

// OwnerManager implements the DDL interface.
func (d Checker) OwnerManager() owner.Manager {
	return d.realDDL.OwnerManager()
}

// GetID implements the DDL interface.
func (d Checker) GetID() string {
	return d.realDDL.GetID()
}

// GetTableMaxHandle implements the DDL interface.
func (d Checker) GetTableMaxHandle(ctx *ddl.JobContext, startTS uint64, tbl table.PhysicalTable) (kv.Handle, bool, error) {
	return d.realDDL.GetTableMaxHandle(ctx, startTS, tbl)
}

// SetBinlogClient implements the DDL interface.
func (d Checker) SetBinlogClient(client *pumpcli.PumpsClient) {
	d.realDDL.SetBinlogClient(client)
}

// GetHook implements the DDL interface.
func (d Checker) GetHook() ddl.Callback {
	return d.realDDL.GetHook()
}

// SetHook implements the DDL interface.
func (d Checker) SetHook(h ddl.Callback) {
	d.realDDL.SetHook(h)
}

// GetInfoSchemaWithInterceptor implements the DDL interface.
func (d Checker) GetInfoSchemaWithInterceptor(ctx sessionctx.Context) infoschema.InfoSchema {
	return d.realDDL.GetInfoSchemaWithInterceptor(ctx)
}

// DoDDLJob implements the DDL interface.
func (d Checker) DoDDLJob(ctx sessionctx.Context, job *model.Job) error {
	return d.realDDL.DoDDLJob(ctx, job)
}

// StorageDDLInjector wraps kv.Storage to inject checker to domain's DDL in bootstrap time.
type StorageDDLInjector struct {
	kv.Storage
	kv.EtcdBackend
	Injector func(ddl.DDL) *Checker
}

var _ kv.EtcdBackend = StorageDDLInjector{}

// EtcdAddrs implements the kv.EtcdBackend interface.
func (s StorageDDLInjector) EtcdAddrs() ([]string, error) {
	return s.EtcdBackend.EtcdAddrs()
}

// TLSConfig implements the kv.EtcdBackend interface.
func (s StorageDDLInjector) TLSConfig() *tls.Config {
	return s.EtcdBackend.TLSConfig()
}

// StartGCWorker implements the kv.EtcdBackend interface.
func (s StorageDDLInjector) StartGCWorker() error {
	return s.EtcdBackend.StartGCWorker()
}

// NewStorageDDLInjector creates a new StorageDDLInjector to inject Checker.
func NewStorageDDLInjector(s kv.Storage) kv.Storage {
	ret := StorageDDLInjector{
		Storage:  s,
		Injector: NewChecker,
	}
	if ebd, ok := s.(kv.EtcdBackend); ok {
		ret.EtcdBackend = ebd
	}
	return ret
}

// UnwrapStorage unwraps StorageDDLInjector for one level.
func UnwrapStorage(s kv.Storage) kv.Storage {
	injector, ok := s.(StorageDDLInjector)
	if !ok {
		return s
	}
	return injector.Storage
}
