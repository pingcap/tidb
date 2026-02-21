// Copyright 2020 PingCAP, Inc.
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

package executor

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/printer"
	pd "github.com/tikv/pd/client"
)

type tidbGlue struct {
	// the session context of the brie task
	se       sessionctx.Context
	progress *brieTaskProgress
	info     *brieTaskInfo
}

// GetDomain implements glue.Glue
func (gs *tidbGlue) GetDomain(_ kv.Storage) (*domain.Domain, error) {
	return domain.GetDomain(gs.se), nil
}

// CreateSession implements glue.Glue
func (gs *tidbGlue) CreateSession(_ kv.Storage) (glue.Session, error) {
	newSCtx, err := CreateSession(gs.se)
	if err != nil {
		return nil, err
	}
	return &tidbGlueSession{se: newSCtx}, nil
}

// Open implements glue.Glue
func (gs *tidbGlue) Open(string, pd.SecurityOption) (kv.Storage, error) {
	return gs.se.GetStore(), nil
}

// OwnsStorage implements glue.Glue
func (*tidbGlue) OwnsStorage() bool {
	return false
}

// StartProgress implements glue.Glue
func (gs *tidbGlue) StartProgress(_ context.Context, cmdName string, total int64, _ bool) glue.Progress {
	gs.progress.lock.Lock()
	gs.progress.cmd = cmdName
	gs.progress.total = total
	atomic.StoreInt64(&gs.progress.current, 0)
	gs.progress.lock.Unlock()
	return gs.progress
}

// Record implements glue.Glue
func (gs *tidbGlue) Record(name string, value uint64) {
	switch name {
	case "BackupTS":
		gs.info.backupTS = value
	case "RestoreTS":
		gs.info.restoreTS = value
	case "Size":
		gs.info.archiveSize = value
	}
}

func (*tidbGlue) GetVersion() string {
	return "TiDB\n" + printer.GetTiDBInfo()
}

// UseOneShotSession implements glue.Glue
func (gs *tidbGlue) UseOneShotSession(_ kv.Storage, _ bool, fn func(se glue.Session) error) error {
	// In SQL backup, we don't need to close domain,
	// but need to create an new session.
	newSCtx, err := CreateSession(gs.se)
	if err != nil {
		return err
	}
	glueSession := &tidbGlueSession{se: newSCtx}
	defer func() {
		CloseSession(newSCtx)
		log.Info("one shot session from brie closed")
	}()
	return fn(glueSession)
}

func (*tidbGlue) GetClient() glue.GlueClient {
	return glue.ClientSql
}

type tidbGlueSession struct {
	// the session context of the brie task's subtask, such as `CREATE TABLE`.
	se sessionctx.Context
}

// Execute implements glue.Session
// These queries execute without privilege checking, since the calling statements
// such as BACKUP and RESTORE have already been privilege checked.
// NOTE: Maybe drain the restult too? See `gluetidb.tidbSession.ExecuteInternal` for more details.
func (gs *tidbGlueSession) Execute(ctx context.Context, sql string) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)
	_, _, err := gs.se.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, nil, sql)
	return err
}

func (gs *tidbGlueSession) ExecuteInternal(ctx context.Context, sql string, args ...any) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)
	exec := gs.se.GetSQLExecutor()
	_, err := exec.ExecuteInternal(ctx, sql, args...)
	return err
}

// CreateDatabaseOnExistError implements glue.Session
func (gs *tidbGlueSession) CreateDatabaseOnExistError(_ context.Context, schema *model.DBInfo) error {
	return BRIECreateDatabase(gs.se, schema, "")
}

// CreateTable implements glue.Session
func (gs *tidbGlueSession) CreateTable(_ context.Context, dbName ast.CIStr, clonedTable *model.TableInfo, cs ...ddl.CreateTableOption) error {
	return BRIECreateTable(gs.se, dbName, clonedTable, "", cs...)
}

// CreateTables implements glue.BatchCreateTableSession.
func (gs *tidbGlueSession) CreateTables(_ context.Context,
	clonedTables map[string][]*model.TableInfo, cs ...ddl.CreateTableOption) error {
	return BRIECreateTables(gs.se, clonedTables, "", cs...)
}

// CreatePlacementPolicy implements glue.Session
func (gs *tidbGlueSession) CreatePlacementPolicy(_ context.Context, policy *model.PolicyInfo) error {
	originQueryString := gs.se.Value(sessionctx.QueryString)
	defer gs.se.SetValue(sessionctx.QueryString, originQueryString)
	gs.se.SetValue(sessionctx.QueryString, ConstructResultOfShowCreatePlacementPolicy(policy))
	d := domain.GetDomain(gs.se).DDLExecutor()
	// the default behaviour is ignoring duplicated policy during restore.
	return d.CreatePlacementPolicyWithInfo(gs.se, policy, ddl.OnExistIgnore)
}

// Close implements glue.Session
func (gs *tidbGlueSession) Close() {
	CloseSession(gs.se)
}

// GetGlobalVariable implements glue.Session.
func (gs *tidbGlueSession) GetGlobalVariable(name string) (string, error) {
	return gs.se.GetSessionVars().GlobalVarsAccessor.GetTiDBTableValue(name)
}

// GetGlobalSysVar gets the global system variable value for name.
func (gs *tidbGlueSession) GetGlobalSysVar(name string) (string, error) {
	return gs.se.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(name)
}

// GetSessionCtx implements glue.Glue
func (gs *tidbGlueSession) GetSessionCtx() sessionctx.Context {
	return gs.se
}

// AlterTableMode implements glue.Session.
func (gs *tidbGlueSession) AlterTableMode(
	_ context.Context,
	schemaID int64,
	tableID int64,
	tableMode model.TableMode) error {
	originQueryString := gs.se.Value(sessionctx.QueryString)
	defer gs.se.SetValue(sessionctx.QueryString, originQueryString)
	d := domain.GetDomain(gs.se).DDLExecutor()
	gs.se.SetValue(sessionctx.QueryString,
		fmt.Sprintf("ALTER TABLE MODE SCHEMA_ID=%d TABLE_ID=%d TO %s", schemaID, tableID, tableMode.String()))
	args := &model.AlterTableModeArgs{
		SchemaID:  schemaID,
		TableID:   tableID,
		TableMode: tableMode,
	}
	return d.AlterTableMode(gs.se, args)
}

// RefreshMeta implements glue.Session.
func (gs *tidbGlueSession) RefreshMeta(
	_ context.Context,
	args *model.RefreshMetaArgs) error {
	originQueryString := gs.se.Value(sessionctx.QueryString)
	defer gs.se.SetValue(sessionctx.QueryString, originQueryString)
	d := domain.GetDomain(gs.se).DDLExecutor()
	gs.se.SetValue(sessionctx.QueryString,
		fmt.Sprintf("REFRESH META SCHEMA_ID=%d TABLE_ID=%d INVOLVED_DB=%s INVOLVED_TABLE=%s",
			args.SchemaID, args.TableID, args.InvolvedDB, args.InvolvedTable))
	return d.RefreshMeta(gs.se, args)
}

func restoreQuery(stmt *ast.BRIEStmt) string {
	out := bytes.NewBuffer(nil)
	rc := format.NewRestoreCtx(format.RestoreNameBackQuotes|format.RestoreStringSingleQuotes, out)
	if err := stmt.Restore(rc); err != nil {
		return "N/A"
	}
	return out.String()
}
