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
// See the License for the specific language governing permissions and
// limitations under the License.

package glue

import (
	"context"
	"database/sql"
	"errors"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/sqlexec"
)

type Glue interface {
	OwnsSQLExecutor() bool
	GetSQLExecutor() SQLExecutor
	GetDB() (*sql.DB, error)
	GetParser() *parser.Parser
	GetTables(context.Context, string) ([]*model.TableInfo, error)
	GetSession(context.Context) (checkpoints.Session, error)
	OpenCheckpointsDB(context.Context, *config.Config) (checkpoints.DB, error)
	// Record is used to report some information (key, value) to host TiDB, including progress, stage currently
	Record(string, uint64)
}

type SQLExecutor interface {
	// ExecuteWithLog and ObtainStringWithLog should support concurrently call and can't assure different calls goes to
	// same underlying connection
	ExecuteWithLog(ctx context.Context, query string, purpose string, logger log.Logger) error
	ObtainStringWithLog(ctx context.Context, query string, purpose string, logger log.Logger) (string, error)
	QueryStringsWithLog(ctx context.Context, query string, purpose string, logger log.Logger) ([][]string, error)
	Close()
}

// sqlConnSession implement checkpoints.Session used only for lighting itself
type sqlConnSession struct {
	checkpoints.Session
	conn *sql.Conn
}

func (session *sqlConnSession) Close() {
	session.conn.Close()
}

func (session *sqlConnSession) Execute(ctx context.Context, sql string) ([]sqlexec.RecordSet, error) {
	_, err := session.conn.ExecContext(ctx, sql)
	return nil, err
}

func (session *sqlConnSession) CommitTxn(context.Context) error {
	return errors.New("sqlConnSession doesn't have a valid CommitTxn implementation")
}

func (session *sqlConnSession) RollbackTxn(context.Context) {}

func (session *sqlConnSession) PrepareStmt(sql string) (stmtID uint32, paramCount int, fields []*ast.ResultField, err error) {
	return 0, 0, nil, errors.New("sqlConnSession doesn't have a valid PrepareStmt implementation")
}

func (session *sqlConnSession) ExecutePreparedStmt(ctx context.Context, stmtID uint32, param []types.Datum) (sqlexec.RecordSet, error) {
	return nil, errors.New("sqlConnSession doesn't have a valid ExecutePreparedStmt implementation")
}

func (session *sqlConnSession) DropPreparedStmt(stmtID uint32) error {
	return errors.New("sqlConnSession doesn't have a valid DropPreparedStmt implementation")
}

type ExternalTiDBGlue struct {
	db     *sql.DB
	parser *parser.Parser
}

func NewExternalTiDBGlue(db *sql.DB, sqlMode mysql.SQLMode) *ExternalTiDBGlue {
	p := parser.New()
	p.SetSQLMode(sqlMode)

	return &ExternalTiDBGlue{db: db, parser: p}
}

func (e *ExternalTiDBGlue) GetSQLExecutor() SQLExecutor {
	return e
}

func (e *ExternalTiDBGlue) ExecuteWithLog(ctx context.Context, query string, purpose string, logger log.Logger) error {
	sql := common.SQLWithRetry{
		DB:     e.db,
		Logger: logger,
	}
	return sql.Exec(ctx, purpose, query)
}

func (e *ExternalTiDBGlue) ObtainStringWithLog(ctx context.Context, query string, purpose string, logger log.Logger) (string, error) {
	var s string
	err := common.SQLWithRetry{
		DB:     e.db,
		Logger: logger,
	}.QueryRow(ctx, purpose, query, &s)
	return s, err
}

func (e *ExternalTiDBGlue) QueryStringsWithLog(ctx context.Context, query string, purpose string, logger log.Logger) (result [][]string, finalErr error) {
	finalErr = common.SQLWithRetry{
		DB:     e.db,
		Logger: logger,
	}.Transact(ctx, purpose, func(c context.Context, tx *sql.Tx) (txErr error) {
		rows, err := tx.QueryContext(c, query)
		if err != nil {
			return err
		}
		defer rows.Close()

		colNames, err := rows.Columns()
		if err != nil {
			return err
		}
		for rows.Next() {
			row := make([]string, len(colNames))
			refs := make([]interface{}, 0, len(row))
			for i := range row {
				refs = append(refs, &row[i])
			}
			if err := rows.Scan(refs...); err != nil {
				return err
			}
			result = append(result, row)
		}

		return rows.Err()
	})
	return
}

func (e *ExternalTiDBGlue) GetDB() (*sql.DB, error) {
	return e.db, nil
}

func (e *ExternalTiDBGlue) GetParser() *parser.Parser {
	return e.parser
}

func (e ExternalTiDBGlue) GetTables(context.Context, string) ([]*model.TableInfo, error) {
	return nil, errors.New("ExternalTiDBGlue doesn't have a valid GetTables function")
}

func (e *ExternalTiDBGlue) GetSession(ctx context.Context) (checkpoints.Session, error) {
	conn, err := e.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return &sqlConnSession{conn: conn}, nil
}

func (e *ExternalTiDBGlue) OpenCheckpointsDB(ctx context.Context, cfg *config.Config) (checkpoints.DB, error) {
	return checkpoints.OpenCheckpointsDB(ctx, cfg)
}

func (e *ExternalTiDBGlue) OwnsSQLExecutor() bool {
	return true
}

func (e *ExternalTiDBGlue) Close() {
	e.db.Close()
}

func (e *ExternalTiDBGlue) Record(string, uint64) {
}

const (
	RecordEstimatedChunk = "EstimatedChunk"
	RecordFinishedChunk  = "FinishedChunk"
)
