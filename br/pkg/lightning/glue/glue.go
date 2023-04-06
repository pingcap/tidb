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

package glue

import (
	"context"
	"database/sql"
	"errors"

	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/sqlexec"
)

// Glue is the glue that binds Lightning with the outside world.
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

// SQLExecutor is the interface for executing SQL statements.
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

// Close implements checkpoints.Session.Close
func (session *sqlConnSession) Close() {
	_ = session.conn.Close()
}

// Execute implements checkpoints.Session.Execute
func (session *sqlConnSession) Execute(ctx context.Context, sql string) ([]sqlexec.RecordSet, error) {
	_, err := session.conn.ExecContext(ctx, sql)
	return nil, err
}

// CommitTxn implements checkpoints.Session.CommitTxn
func (*sqlConnSession) CommitTxn(context.Context) error {
	return errors.New("sqlConnSession doesn't have a valid CommitTxn implementation")
}

// RollbackTxn implements checkpoints.Session.RollbackTxn
func (*sqlConnSession) RollbackTxn(context.Context) {}

// PrepareStmt implements checkpoints.Session.PrepareStmt
func (*sqlConnSession) PrepareStmt(_ string) (stmtID uint32, paramCount int, fields []*ast.ResultField, err error) {
	return 0, 0, nil, errors.New("sqlConnSession doesn't have a valid PrepareStmt implementation")
}

// ExecutePreparedStmt implements checkpoints.Session.ExecutePreparedStmt
func (*sqlConnSession) ExecutePreparedStmt(_ context.Context, _ uint32, _ []types.Datum) (sqlexec.RecordSet, error) {
	return nil, errors.New("sqlConnSession doesn't have a valid ExecutePreparedStmt implementation")
}

// DropPreparedStmt implements checkpoints.Session.DropPreparedStmt
func (*sqlConnSession) DropPreparedStmt(_ uint32) error {
	return errors.New("sqlConnSession doesn't have a valid DropPreparedStmt implementation")
}

// ExternalTiDBGlue is a Glue implementation which uses an external TiDB as storage.
type ExternalTiDBGlue struct {
	db     *sql.DB
	parser *parser.Parser
}

// NewExternalTiDBGlue creates a new ExternalTiDBGlue instance.
func NewExternalTiDBGlue(db *sql.DB, sqlMode mysql.SQLMode) *ExternalTiDBGlue {
	p := parser.New()
	p.SetSQLMode(sqlMode)

	return &ExternalTiDBGlue{db: db, parser: p}
}

// GetSQLExecutor implements Glue.GetSQLExecutor.
func (e *ExternalTiDBGlue) GetSQLExecutor() SQLExecutor {
	return e
}

// ExecuteWithLog implements SQLExecutor.ExecuteWithLog.
func (e *ExternalTiDBGlue) ExecuteWithLog(ctx context.Context, query string, purpose string, logger log.Logger) error {
	sql := common.SQLWithRetry{
		DB:     e.db,
		Logger: logger,
	}
	return sql.Exec(ctx, purpose, query)
}

// ObtainStringWithLog implements SQLExecutor.ObtainStringWithLog.
func (e *ExternalTiDBGlue) ObtainStringWithLog(ctx context.Context, query string, purpose string, logger log.Logger) (string, error) {
	var s string
	err := common.SQLWithRetry{
		DB:     e.db,
		Logger: logger,
	}.QueryRow(ctx, purpose, query, &s)
	return s, err
}

// QueryStringsWithLog implements SQLExecutor.QueryStringsWithLog.
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

// GetDB implements Glue.GetDB.
func (e *ExternalTiDBGlue) GetDB() (*sql.DB, error) {
	return e.db, nil
}

// GetParser implements Glue.GetParser.
func (e *ExternalTiDBGlue) GetParser() *parser.Parser {
	return e.parser
}

// GetTables implements Glue.GetTables.
func (ExternalTiDBGlue) GetTables(context.Context, string) ([]*model.TableInfo, error) {
	return nil, errors.New("ExternalTiDBGlue doesn't have a valid GetTables function")
}

// GetSession implements Glue.GetSession.
func (e *ExternalTiDBGlue) GetSession(ctx context.Context) (checkpoints.Session, error) {
	conn, err := e.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return &sqlConnSession{conn: conn}, nil
}

// OpenCheckpointsDB implements Glue.OpenCheckpointsDB.
func (*ExternalTiDBGlue) OpenCheckpointsDB(ctx context.Context, cfg *config.Config) (checkpoints.DB, error) {
	return checkpoints.OpenCheckpointsDB(ctx, cfg)
}

// OwnsSQLExecutor implements Glue.OwnsSQLExecutor.
func (*ExternalTiDBGlue) OwnsSQLExecutor() bool {
	return true
}

// Close implements Glue.Close.
func (e *ExternalTiDBGlue) Close() {
	e.db.Close()
}

// Record implements Glue.Record.
func (*ExternalTiDBGlue) Record(string, uint64) {
}

// record key names
const (
	RecordEstimatedChunk = "EstimatedChunk"
	RecordFinishedChunk  = "FinishedChunk"
)
