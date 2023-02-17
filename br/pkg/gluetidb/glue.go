// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package gluetidb

import (
	"bytes"
	"context"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/gluetikv"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
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
	initStatsSe, err := session.CreateSession(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	se, err := session.CreateSession(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	dom, err := session.GetDomain(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = session.InitMDLVariable(store)
	if err != nil {
		return nil, err
	}
	// create stats handler for backup and restore.
	err = dom.UpdateTableStatsLoop(se, initStatsSe)
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
	if err = session.InitMDLVariable(store); err != nil {
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

// SplitBatchCreateTable provide a way to split batch into small batch when batch size is large than 6 MB.
// The raft entry has limit size of 6 MB, a batch of CreateTables may hit this limitation
// TODO: shall query string be set for each split batch create, it looks does not matter if we set once for all.
func (gs *tidbSession) SplitBatchCreateTable(schema model.CIStr, infos []*model.TableInfo, cs ...ddl.CreateTableWithInfoConfigurier) error {
	var err error
	d := domain.GetDomain(gs.se).DDL()

	if err = d.BatchCreateTableWithInfo(gs.se, schema, infos, append(cs, ddl.OnExistIgnore)...); kv.ErrEntryTooLarge.Equal(err) {
		log.Info("entry too large, split batch create table", zap.Int("num table", len(infos)))
		if len(infos) == 1 {
			return err
		}
		mid := len(infos) / 2
		err = gs.SplitBatchCreateTable(schema, infos[:mid], cs...)
		if err != nil {
			return err
		}
		err = gs.SplitBatchCreateTable(schema, infos[mid:], cs...)
		if err != nil {
			return err
		}
		return nil
	}
	return err
}

// CreateTables implements glue.BatchCreateTableSession.
func (gs *tidbSession) CreateTables(ctx context.Context, tables map[string][]*model.TableInfo, cs ...ddl.CreateTableWithInfoConfigurier) error {
	var dbName model.CIStr

	// Disable foreign key check when batch create tables.
	gs.se.GetSessionVars().ForeignKeyChecks = false
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
		if err := gs.SplitBatchCreateTable(dbName, cloneTables, cs...); err != nil {
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
	// Disable foreign key check when batch create tables.
	gs.se.GetSessionVars().ForeignKeyChecks = false
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
