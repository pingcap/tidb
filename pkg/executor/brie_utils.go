// Copyright 2023 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"go.uber.org/zap"
)

const (
	defaultCapOfCreateTable    = 512
	defaultCapOfCreateDatabase = 64
)

// SplitBatchCreateTableForTest is only used for test.
var SplitBatchCreateTableForTest = splitBatchCreateTable

// showRestoredCreateDatabase shows the result of SHOW CREATE DATABASE from a dbInfo.
func showRestoredCreateDatabase(sctx sessionctx.Context, db *model.DBInfo, brComment string) (string, error) {
	result := bytes.NewBuffer(make([]byte, 0, defaultCapOfCreateDatabase))
	if len(brComment) > 0 {
		// this can never fail.
		_, _ = result.WriteString(brComment)
	}
	if err := ConstructResultOfShowCreateDatabase(sctx, db, true, result); err != nil {
		return "", errors.Trace(err)
	}
	return result.String(), nil
}

// BRIECreateDatabase creates the database with OnExistIgnore option
func BRIECreateDatabase(sctx sessionctx.Context, schema *model.DBInfo, brComment string) error {
	d := domain.GetDomain(sctx).DDL()
	query, err := showRestoredCreateDatabase(sctx, schema, brComment)
	if err != nil {
		return errors.Trace(err)
	}
	originQuery := sctx.Value(sessionctx.QueryString)
	sctx.SetValue(sessionctx.QueryString, query)
	defer func() {
		sctx.SetValue(sessionctx.QueryString, originQuery)
	}()

	schema = schema.Clone()
	if len(schema.Charset) == 0 {
		schema.Charset = mysql.DefaultCharset
	}
	return d.CreateSchemaWithInfo(sctx, schema, ddl.OnExistIgnore)
}

// showRestoredCreateTable shows the result of SHOW CREATE TABLE from a tableInfo.
func showRestoredCreateTable(sctx sessionctx.Context, tbl *model.TableInfo, brComment string) (string, error) {
	result := bytes.NewBuffer(make([]byte, 0, defaultCapOfCreateTable))
	if len(brComment) > 0 {
		// this can never fail.
		_, _ = result.WriteString(brComment)
	}
	if err := ConstructResultOfShowCreateTable(sctx, tbl, autoid.Allocators{}, result); err != nil {
		return "", err
	}
	return result.String(), nil
}

// BRIECreateTable creates the table with OnExistIgnore option
func BRIECreateTable(
	sctx sessionctx.Context,
	dbName model.CIStr,
	table *model.TableInfo,
	brComment string,
	cs ...ddl.CreateTableWithInfoConfigurier,
) error {
	d := domain.GetDomain(sctx).DDL()
	query, err := showRestoredCreateTable(sctx, table, brComment)
	if err != nil {
		return err
	}
	originQuery := sctx.Value(sessionctx.QueryString)
	sctx.SetValue(sessionctx.QueryString, query)
	// Disable foreign key check when batch create tables.
	originForeignKeyChecks := sctx.GetSessionVars().ForeignKeyChecks
	sctx.GetSessionVars().ForeignKeyChecks = false
	defer func() {
		sctx.SetValue(sessionctx.QueryString, originQuery)
		sctx.GetSessionVars().ForeignKeyChecks = originForeignKeyChecks
	}()

	table = table.Clone()

	return d.CreateTableWithInfo(sctx, dbName, table, append(cs, ddl.OnExistIgnore)...)
}

// BRIECreateTables creates the tables with OnExistIgnore option in batch
func BRIECreateTables(
	sctx sessionctx.Context,
	tables map[string][]*model.TableInfo,
	brComment string,
	cs ...ddl.CreateTableWithInfoConfigurier,
) error {
	// Disable foreign key check when batch create tables.
	originForeignKeyChecks := sctx.GetSessionVars().ForeignKeyChecks
	sctx.GetSessionVars().ForeignKeyChecks = false
	originQuery := sctx.Value(sessionctx.QueryString)
	defer func() {
		sctx.SetValue(sessionctx.QueryString, originQuery)
		sctx.GetSessionVars().ForeignKeyChecks = originForeignKeyChecks
	}()
	for db, tablesInDB := range tables {
		dbName := model.NewCIStr(db)
		queryBuilder := strings.Builder{}
		cloneTables := make([]*model.TableInfo, 0, len(tablesInDB))
		for _, table := range tablesInDB {
			query, err := showRestoredCreateTable(sctx, table, brComment)
			if err != nil {
				return errors.Trace(err)
			}

			queryBuilder.WriteString(query)
			queryBuilder.WriteString(";")

			cloneTables = append(cloneTables, table.Clone())
		}
		sctx.SetValue(sessionctx.QueryString, queryBuilder.String())
		if err := splitBatchCreateTable(sctx, dbName, cloneTables, cs...); err != nil {
			//It is possible to failure when TiDB does not support model.ActionCreateTables.
			//In this circumstance, BatchCreateTableWithInfo returns errno.ErrInvalidDDLJob,
			//we fall back to old way that creating table one by one
			log.Warn("batch create table from tidb failure", zap.Error(err))
			return err
		}
	}

	return nil
}

// splitBatchCreateTable provide a way to split batch into small batch when batch size is large than 6 MB.
// The raft entry has limit size of 6 MB, a batch of CreateTables may hit this limitation
// TODO: shall query string be set for each split batch create, it looks does not matter if we set once for all.
func splitBatchCreateTable(sctx sessionctx.Context, schema model.CIStr,
	infos []*model.TableInfo, cs ...ddl.CreateTableWithInfoConfigurier) error {
	var err error
	d := domain.GetDomain(sctx).DDL()
	err = d.BatchCreateTableWithInfo(sctx, schema, infos, append(cs, ddl.OnExistIgnore)...)
	if kv.ErrEntryTooLarge.Equal(err) {
		log.Info("entry too large, split batch create table", zap.Int("num table", len(infos)))
		if len(infos) == 1 {
			return err
		}
		mid := len(infos) / 2
		err = splitBatchCreateTable(sctx, schema, infos[:mid], cs...)
		if err != nil {
			return err
		}
		err = splitBatchCreateTable(sctx, schema, infos[mid:], cs...)
		if err != nil {
			return err
		}
		return nil
	}
	return err
}
