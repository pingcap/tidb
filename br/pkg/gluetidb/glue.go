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
	})
	return Glue{}
}

// Glue is an implementation of glue.Glue using a new TiDB session.
type Glue struct {
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

// Execute implements glue.Session.
func (gs *tidbSession) Execute(ctx context.Context, sql string) error {
	_, err := gs.se.ExecuteInternal(ctx, sql)
	return errors.Trace(err)
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
	return d.CreateSchemaWithInfo(gs.se, schema, ddl.OnExistIgnore, true)
}

// CreateTable implements glue.Session.
func (gs *tidbSession) CreateTables(ctx context.Context, tables map[string][]*model.TableInfo, batchDdlSize uint) error {
	d := domain.GetDomain(gs.se).DDL()
	log.Info("tidb start create tables", zap.Uint("batchDdlSize", batchDdlSize))

	for db, tablesInDB := range tables {
		NewTables := make([]*model.TableInfo, 0, len(tablesInDB))
		dbName := model.NewCIStr(db)
		queryBuilder := strings.Builder{}
		lastSend := 0
		for _, table := range tablesInDB {

			query, err := gs.showCreateTable(table)
			if err != nil {
				log.Error("Tidbsession to show create tables failure.")
				return err
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
			NewTables = append(NewTables, table)
			var batchSize int = int(batchDdlSize)
			if batchDdlSize <= 1 {
				batchSize = 500
			}
			// 500 tables once, limitation from raft entry size
			LengthOfTables := len(NewTables)
			if LengthOfTables == 0 {
				log.Error("size of tables equals to 0.")
				return errors.New("empty tables to restore.")
			}
			if LengthOfTables%batchSize == 0 {
				ThePatchTables := NewTables[lastSend : LengthOfTables-1]
				gs.se.SetValue(sessionctx.QueryString, queryBuilder.String())
				err := d.BatchCreateTableWithInfo(gs.se, dbName, ThePatchTables, ddl.OnExistIgnore, true)
				if err != nil {
					log.Info("Bulk create table from tidb failure, it possible caused by version mismatch with BR.", zap.String("Error", err.Error()))
					return err
				}
				log.Info("BatchCreateTableWithInfo  DONE",
					zap.Stringer("DB", dbName),
					zap.Int("start", lastSend),
					zap.Int("end", LengthOfTables-1),
					zap.Int("query total size", len(queryBuilder.String())))
				lastSend = LengthOfTables - 1
				queryBuilder.Reset()
			}

		}
		// handle leftover tables from NewTables, e.g.
		// 1. if DB has 550 tables, batch size is 500, 500 will be send by internal loop, 50 leftover, here we handle 50
		// 2. if DB has 50 tables < 500
		LengthOfTables := len(NewTables)
		if lastSend < LengthOfTables {
			ThePatchTables := NewTables[lastSend:LengthOfTables]
			gs.se.SetValue(sessionctx.QueryString, queryBuilder.String())
			err := d.BatchCreateTableWithInfo(gs.se, dbName, ThePatchTables, ddl.OnExistIgnore, true)
			if err != nil {
				log.Info("Bulk create table from tidb failure, it possible caused by version mismatch with BR.", zap.String("Error", err.Error()))
				return err
			}
			log.Info("BatchCreateTableWithInfo  DONE",
				zap.Stringer("DB", dbName),
				zap.Int("start", lastSend),
				zap.Int("end", LengthOfTables),
				zap.Int("query total size", len(queryBuilder.String())))
			lastSend = LengthOfTables - 1
			queryBuilder.Reset()
		}
	}

	return nil
}

// CreateTable implements glue.Session.
func (gs *tidbSession) CreateTable(ctx context.Context, dbName model.CIStr, table *model.TableInfo) error {
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
	return d.CreateTableWithInfo(gs.se, dbName, table, ddl.OnExistIgnore, true)
}

// Close implements glue.Session.
func (gs *tidbSession) Close() {
	gs.se.Close()
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
