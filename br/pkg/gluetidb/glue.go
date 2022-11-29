// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package gluetidb

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/gluetikv"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// Asserting Glue implements glue.ConsoleGlue and glue.Glue at compile time.
var (
	_ glue.ConsoleGlue = Glue{}
	_ glue.Glue        = Glue{}
)

const (
	defaultCapOfCreateTable    = 512
	defaultCapOfCreateDatabase = 64
	brComment                  = `/*from(br)*/`
)

// New makes a new tidb glue.
func New() Glue {
	log.Debug("enabling no register config")
	config.UpdateGlobal(func(conf *config.Config) {
		conf.SkipRegisterToDashboard = true
		conf.Log.EnableSlowLog.Store(false)
	})
	return Glue{}
}

// Glue is an implementation of glue.Glue using a new TiDB session.
type Glue struct {
	glue.StdIOGlue

	tikvGlue gluetikv.Glue
}

type tidbSession struct {
	se session.Session
}

// GetDomain implements glue.Glue.
func (Glue) GetDomain(store kv.Storage) (*domain.Domain, error) {
	se, err := session.CreateSession(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	dom, err := session.GetDomain(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// create stats handler for backup and restore.
	err = dom.UpdateTableStatsLoop(se)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return dom, nil
}

// CreateSession implements glue.Glue.
func (Glue) CreateSession(store kv.Storage) (glue.Session, error) {
	se, err := session.CreateSession(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tiSession := &tidbSession{
		se: se,
	}
	return tiSession, nil
}

// Open implements glue.Glue.
func (g Glue) Open(path string, option pd.SecurityOption) (kv.Storage, error) {
	return g.tikvGlue.Open(path, option)
}

// OwnsStorage implements glue.Glue.
func (Glue) OwnsStorage() bool {
	return true
}

// StartProgress implements glue.Glue.
func (g Glue) StartProgress(ctx context.Context, cmdName string, total int64, redirectLog bool) glue.Progress {
	return g.tikvGlue.StartProgress(ctx, cmdName, total, redirectLog)
}

// Record implements glue.Glue.
func (g Glue) Record(name string, value uint64) {
	g.tikvGlue.Record(name, value)
}

// GetVersion implements glue.Glue.
func (g Glue) GetVersion() string {
	return g.tikvGlue.GetVersion()
}

// UseOneShotSession implements glue.Glue.
func (g Glue) UseOneShotSession(store kv.Storage, closeDomain bool, fn func(glue.Session) error) error {
	se, err := session.CreateSession(store)
	if err != nil {
		return errors.Trace(err)
	}
	glueSession := &tidbSession{
		se: se,
	}
	defer func() {
		se.Close()
		log.Info("one shot session closed")
	}()
	// dom will be created during session.CreateSession.
	dom, err := session.GetDomain(store)
	if err != nil {
		return errors.Trace(err)
	}
	// because domain was created during the whole program exists.
	// and it will register br info to info syncer.
	// we'd better close it as soon as possible.
	if closeDomain {
		defer func() {
			dom.Close()
			log.Info("one shot domain closed")
		}()
	}
	err = fn(glueSession)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// GetSessionCtx implements glue.Glue
func (gs *tidbSession) GetSessionCtx() sessionctx.Context {
	return gs.se
}

// Execute implements glue.Session.
func (gs *tidbSession) Execute(ctx context.Context, sql string) error {
	return gs.ExecuteInternal(ctx, sql)
}

func (gs *tidbSession) ExecuteInternal(ctx context.Context, sql string, args ...interface{}) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)
	rs, err := gs.se.ExecuteInternal(ctx, sql, args...)
	if err != nil {
		return errors.Trace(err)
	}
	// Some of SQLs (like ADMIN RECOVER INDEX) may lazily take effect
	// when we polling the result set.
	// At least call `next` once for triggering theirs side effect.
	// (Maybe we'd better drain all returned rows?)
	if rs != nil {
		//nolint: errcheck
		defer rs.Close()
		c := rs.NewChunk(nil)
		if err := rs.Next(ctx, c); err != nil {
			log.Warn("Error during draining result of internal sql.", logutil.Redact(zap.String("sql", sql)), logutil.ShortError(err))
			return nil
		}
	}
	return nil
}

// CreateDatabase implements glue.Session.
func (gs *tidbSession) CreateDatabase(ctx context.Context, schema *model.DBInfo) error {
	d := domain.GetDomain(gs.se).DDL()
	query, err := gs.showCreateDatabase(schema)
	if err != nil {
		return errors.Trace(err)
	}
	gs.se.SetValue(sessionctx.QueryString, query)
	schema = schema.Clone()
	if len(schema.Charset) == 0 {
		schema.Charset = mysql.DefaultCharset
	}
	return d.CreateSchemaWithInfo(gs.se, schema, ddl.OnExistIgnore)
}

// CreatePlacementPolicy implements glue.Session.
func (gs *tidbSession) CreatePlacementPolicy(ctx context.Context, policy *model.PolicyInfo) error {
	d := domain.GetDomain(gs.se).DDL()
	gs.se.SetValue(sessionctx.QueryString, gs.showCreatePlacementPolicy(policy))
	// the default behaviour is ignoring duplicated policy during restore.
	return d.CreatePlacementPolicyWithInfo(gs.se, policy, ddl.OnExistIgnore)
}

// CreateTables implements glue.BatchCreateTableSession.
func (gs *tidbSession) CreateTables(ctx context.Context, tables map[string][]*model.TableInfo, cs ...ddl.CreateTableWithInfoConfigurier) error {
	d := domain.GetDomain(gs.se).DDL()
	var dbName model.CIStr

	for db, tablesInDB := range tables {
		dbName = model.NewCIStr(db)
		queryBuilder := strings.Builder{}
		cloneTables := make([]*model.TableInfo, 0, len(tablesInDB))
		for _, table := range tablesInDB {
			query, err := gs.showCreateTable(table)
			if err != nil {
				return errors.Trace(err)
			}

			queryBuilder.WriteString(query)
			queryBuilder.WriteString(";")

			table = table.Clone()
			// Clone() does not clone partitions yet :(
			if table.Partition != nil {
				newPartition := *table.Partition
				newPartition.Definitions = append([]model.PartitionDefinition{}, table.Partition.Definitions...)
				table.Partition = &newPartition
			}
			cloneTables = append(cloneTables, table)
		}
		gs.se.SetValue(sessionctx.QueryString, queryBuilder.String())
		err := d.BatchCreateTableWithInfo(gs.se, dbName, cloneTables, append(cs, ddl.OnExistIgnore)...)
		if err != nil {
			//It is possible to failure when TiDB does not support model.ActionCreateTables.
			//In this circumstance, BatchCreateTableWithInfo returns errno.ErrInvalidDDLJob,
			//we fall back to old way that creating table one by one
			log.Warn("batch create table from tidb failure", zap.Error(err))
			return err
		}
	}

	return nil
}

// CreateTable implements glue.Session.
func (gs *tidbSession) CreateTable(ctx context.Context, dbName model.CIStr, table *model.TableInfo, cs ...ddl.CreateTableWithInfoConfigurier) error {
	d := domain.GetDomain(gs.se).DDL()
	query, err := gs.showCreateTable(table)
	if err != nil {
		return errors.Trace(err)
	}
	gs.se.SetValue(sessionctx.QueryString, query)
	// Clone() does not clone partitions yet :(
	table = table.Clone()
	if table.Partition != nil {
		newPartition := *table.Partition
		newPartition.Definitions = append([]model.PartitionDefinition{}, table.Partition.Definitions...)
		table.Partition = &newPartition
	}

	return d.CreateTableWithInfo(gs.se, dbName, table, append(cs, ddl.OnExistIgnore)...)
}

// Close implements glue.Session.
func (gs *tidbSession) Close() {
	gs.se.Close()
}

// GetGlobalVariables implements glue.Session.
func (gs *tidbSession) GetGlobalVariable(name string) (string, error) {
	if utils.IsTempSysDB(name) {
		slice := strings.Split(name, ".")
		if len(slice) < 2 {
			return "", errors.Trace(errors.New(fmt.Sprintf("not found %s", name)))
		}
		return gs.getTempDBGlobalVariable(slice[0], slice[1])
	}
	return gs.se.GetSessionVars().GlobalVarsAccessor.GetTiDBTableValue(name)
}

// showCreateTable shows the result of SHOW CREATE TABLE from a TableInfo.
func (gs *tidbSession) showCreateTable(tbl *model.TableInfo) (string, error) {
	table := tbl.Clone()
	table.AutoIncID = 0
	result := bytes.NewBuffer(make([]byte, 0, defaultCapOfCreateTable))
	// this can never fail.
	_, _ = result.WriteString(brComment)
	if err := executor.ConstructResultOfShowCreateTable(gs.se, tbl, autoid.Allocators{}, result); err != nil {
		return "", errors.Trace(err)
	}
	return result.String(), nil
}

// showCreateDatabase shows the result of SHOW CREATE DATABASE from a dbInfo.
func (gs *tidbSession) showCreateDatabase(db *model.DBInfo) (string, error) {
	result := bytes.NewBuffer(make([]byte, 0, defaultCapOfCreateDatabase))
	// this can never fail.
	_, _ = result.WriteString(brComment)
	if err := executor.ConstructResultOfShowCreateDatabase(gs.se, db, true, result); err != nil {
		return "", errors.Trace(err)
	}
	return result.String(), nil
}

func (gs *tidbSession) showCreatePlacementPolicy(policy *model.PolicyInfo) string {
	return executor.ConstructResultOfShowCreatePlacementPolicy(policy)
}

func (gs *tidbSession) getTempDBGlobalVariable(dbName, varName string) (string, error) {
	sql := fmt.Sprintf("SELECT variable_value from %s.tidb where variable_name=\"%s\";", dbName, varName)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBR)
	rs, err := gs.se.ExecuteInternal(ctx, sql)
	if err != nil {
		return "", errors.Trace(err)
	}
	if rs == nil {
		return "", errors.New("Wrong number of Recordset")
	}
	defer rs.Close()

	log.Info("get tidb_server_version successfully", zap.String("sql", sql))
	var tidb_server_version string
	req := rs.NewChunk(nil)
	it := chunk.NewIterator4Chunk(req)
	err = rs.Next(ctx, req)
	for err == nil && req.NumRows() != 0 {
		for row := it.Begin(); row != it.End(); row = it.Next() {
			tidb_server_version = row.GetString(0)
		}
		err = rs.Next(ctx, req)
	}
	return tidb_server_version, nil
}

func (gs *tidbSession) Upgrude(ctx context.Context, currVersion, targetVersion int64, charset, collate string, restoreTblSet map[string]struct{}) error {
	log.Info("upgrade", zap.Int64("currVersion", currVersion), zap.Int64("targetVersion", targetVersion),
		zap.String("charset", charset), zap.String("collate", collate))

	var execErr error
	doReentrantDDLInBR := func(s session.Session, sql string, ignorableErrs ...error) {
		parser := parser.New()
		astNode, err := parser.ParseOneStmt(sql, charset, collate)
		if err != nil {
			log.Error("parse sql statement error", zap.Error(err), zap.String("sql", sql))
			execErr = errors.Trace(err)
			return
		}

		// DDL for upgrade includes `alter` and `create`
		switch stmt := astNode.(type) {
		case *ast.AlterTableStmt:
			log.Info("DDL origin table name", zap.Any("table", stmt.Table))
			stmt.Table.Schema = utils.TemporaryDBName(mysql.SystemDB)
			log.Info("current table name", zap.Any("table", stmt.Table))
			tblName := stmt.Table.Name.L
			log.Info("DDL", zap.String("table", tblName))
			if _, ok := restoreTblSet[tblName]; !ok {
				log.Info("DDL filter out", zap.String("table", tblName))
				return
			}
			_, err = s.ExecuteStmt(ctx, stmt)
		default:
			return
		}

		if err != nil {
			log.Error("Execute Stmt failed", zap.Error(err))
			execErr = errors.Trace(err)
			return
		}
		log.Info("exec DDL successfully", zap.String("sql", sql))
		// Do I need to call rs.Next? Confirm whether alter is lazily take effect. TODO
	}

	mustExecuteInBR := func(s session.Session, sql string, args ...interface{}) {
		log.Info("In mustExecuteInBR", zap.String("sql", sql))

		parser := parser.New()
		astNode, err := parser.ParseOneStmt(sql, charset, collate)
		if err != nil {
			log.Error("parse sql statement error", zap.Error(err), zap.String("sql", sql))
			execErr = errors.Trace(err)
			return
		}

		// DML for upgrade includes `update` and `insert`
		switch stmt := astNode.(type) {
		case *ast.UpdateStmt:
			log.Info("DML origin table name", zap.Any("table", stmt.TableRefs.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Schema))
			stmt.TableRefs.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Schema = utils.TemporaryDBName(mysql.SystemDB)
			log.Info("current table name", zap.Any("table", stmt.TableRefs.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Schema))
			tblName := stmt.TableRefs.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name.L
			log.Info("DML", zap.String("table", tblName))
			if _, ok := restoreTblSet[tblName]; !ok {
				log.Info("DML filter out", zap.String("table", tblName))
				return
			}
			_, err = s.ExecuteStmt(ctx, stmt)
		default:
			return
		}

		if err != nil {
			log.Error("Execute Stmt failed", zap.Error(err))
			execErr = errors.Trace(err)
			return
		}
		log.Info("exec DML successfully", zap.String("sql", sql))
	}

	for _, b := range session.BootstrapVersion {
		if b.Version <= currVersion || b.Version > targetVersion {
			continue
		}
		log.Info("upgrade", zap.Int64("version", b.Version))
		b.UpgradeFunc(gs.se, currVersion, doReentrantDDLInBR, mustExecuteInBR)
		if execErr != nil {
			log.Error("upgrade failed", zap.Error(execErr))
			return errors.Trace(execErr)
		}
	}

	return nil
}

// mockSession is used for test.
type mockSession struct {
	se         session.Session
	globalVars map[string]string
}

// GetSessionCtx implements glue.Glue
func (s *mockSession) GetSessionCtx() sessionctx.Context {
	return s.se
}

// Execute implements glue.Session.
func (s *mockSession) Execute(ctx context.Context, sql string) error {
	return s.ExecuteInternal(ctx, sql)
}

func (s *mockSession) ExecuteInternal(ctx context.Context, sql string, args ...interface{}) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)
	rs, err := s.se.ExecuteInternal(ctx, sql, args...)
	if err != nil {
		return err
	}
	// Some of SQLs (like ADMIN RECOVER INDEX) may lazily take effect
	// when we polling the result set.
	// At least call `next` once for triggering theirs side effect.
	// (Maybe we'd better drain all returned rows?)
	if rs != nil {
		//nolint: errcheck
		defer rs.Close()
		c := rs.NewChunk(nil)
		if err := rs.Next(ctx, c); err != nil {
			return nil
		}
	}
	return nil
}

// CreateDatabase implements glue.Session.
func (s *mockSession) CreateDatabase(ctx context.Context, schema *model.DBInfo) error {
	log.Fatal("unimplemented CreateDatabase for mock session")
	return nil
}

// CreatePlacementPolicy implements glue.Session.
func (s *mockSession) CreatePlacementPolicy(ctx context.Context, policy *model.PolicyInfo) error {
	log.Fatal("unimplemented CreateDatabase for mock session")
	return nil
}

// CreateTables implements glue.BatchCreateTableSession.
func (s *mockSession) CreateTables(ctx context.Context, tables map[string][]*model.TableInfo, cs ...ddl.CreateTableWithInfoConfigurier) error {
	log.Fatal("unimplemented CreateDatabase for mock session")
	return nil
}

// CreateTable implements glue.Session.
func (s *mockSession) CreateTable(ctx context.Context, dbName model.CIStr, table *model.TableInfo, cs ...ddl.CreateTableWithInfoConfigurier) error {
	log.Fatal("unimplemented CreateDatabase for mock session")
	return nil
}

// Close implements glue.Session.
func (s *mockSession) Close() {
	s.se.Close()
}

// GetGlobalVariables implements glue.Session.
func (s *mockSession) GetGlobalVariable(name string) (string, error) {
	if ret, ok := s.globalVars[name]; ok {
		return ret, nil
	}
	return "True", nil
}

func (s *mockSession) Upgrude(ctx context.Context, currVersion, targetVersion int64, charset, collate string, restoreTblSet map[string]struct{}) error {
	// pass
	return nil
}

// MockGlue only used for test
type MockGlue struct {
	se         session.Session
	GlobalVars map[string]string
}

func (m *MockGlue) SetSession(se session.Session) {
	m.se = se
}

// GetDomain implements glue.Glue.
func (*MockGlue) GetDomain(store kv.Storage) (*domain.Domain, error) {
	return nil, nil
}

// CreateSession implements glue.Glue.
func (m *MockGlue) CreateSession(store kv.Storage) (glue.Session, error) {
	glueSession := &mockSession{
		se:         m.se,
		globalVars: m.GlobalVars,
	}
	return glueSession, nil
}

// Open implements glue.Glue.
func (*MockGlue) Open(path string, option pd.SecurityOption) (kv.Storage, error) {
	return nil, nil
}

// OwnsStorage implements glue.Glue.
func (*MockGlue) OwnsStorage() bool {
	return true
}

// StartProgress implements glue.Glue.
func (*MockGlue) StartProgress(ctx context.Context, cmdName string, total int64, redirectLog bool) glue.Progress {
	return nil
}

// Record implements glue.Glue.
func (*MockGlue) Record(name string, value uint64) {
}

// GetVersion implements glue.Glue.
func (*MockGlue) GetVersion() string {
	return "mock glue"
}

// UseOneShotSession implements glue.Glue.
func (m *MockGlue) UseOneShotSession(store kv.Storage, closeDomain bool, fn func(glue.Session) error) error {
	glueSession := &mockSession{
		se: m.se,
	}
	return fn(glueSession)
}
