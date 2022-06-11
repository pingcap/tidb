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

package checker

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/atomic"
)

// ExecutableChecker is a part of TiDB to check the sql's executability
type ExecutableChecker struct {
	session  session.Session
	parser   *parser.Parser
	isClosed *atomic.Bool
}

// NewExecutableChecker creates a new ExecutableChecker
func NewExecutableChecker() (*ExecutableChecker, error) {
	err := logutil.InitLogger(&logutil.LogConfig{
		Config: log.Config{
			Level: "error",
		},
	})
	if err != nil {
		return nil, err
	}
	mockTikv, err := mockstore.NewMockStore()
	if err != nil {
		return nil, errors.Trace(err)
	}
	_, err = session.BootstrapSession(mockTikv)
	if err != nil {
		return nil, errors.Trace(err)
	}
	session, err := session.CreateSession4Test(mockTikv)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &ExecutableChecker{
		session:  session,
		parser:   parser.New(),
		isClosed: atomic.NewBool(false),
	}, nil
}

// Execute executes the sql to check it's executability
func (ec *ExecutableChecker) Execute(context context.Context, sql string) error {
	_, err := ec.session.Execute(context, sql)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// IsTableExist returns whether the table with the specified name exists
func (ec *ExecutableChecker) IsTableExist(context *context.Context, tableName string) bool {
	_, err := ec.session.Execute(*context,
		fmt.Sprintf("select 0 from `%s` limit 1", tableName))
	return err == nil
}

// CreateTable creates a new table with the specified sql
func (ec *ExecutableChecker) CreateTable(context context.Context, sql string) error {
	err := ec.Execute(context, sql)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// DropTable drops the the specified table
func (ec *ExecutableChecker) DropTable(context context.Context, tableName string) error {
	err := ec.Execute(context, fmt.Sprintf("drop table if exists `%s`", tableName))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Close closes the ExecutableChecker
func (ec *ExecutableChecker) Close() error {
	if !ec.isClosed.CAS(false, true) {
		return errors.New("ExecutableChecker is already closed")
	}
	ec.session.Close()
	return nil
}

// Parse parses a query and returns an ast.StmtNode.
func (ec *ExecutableChecker) Parse(sql string) (stmt ast.StmtNode, err error) {

	charset, collation := ec.session.GetSessionVars().GetCharsetInfo()
	stmt, err = ec.parser.ParseOneStmt(sql, charset, collation)
	return
}

// GetTablesNeededExist reports the table name needed to execute ast.StmtNode
// the specified ast.StmtNode must be a DDLNode
func GetTablesNeededExist(stmt ast.StmtNode) ([]string, error) {
	switch x := stmt.(type) {
	case *ast.TruncateTableStmt:
		return []string{x.Table.Name.String()}, nil
	case *ast.CreateIndexStmt:
		return []string{x.Table.Name.String()}, nil
	case *ast.DropTableStmt:
		tablesName := make([]string, len(x.Tables))
		for i, table := range x.Tables {
			tablesName[i] = table.Name.String()
		}
		return tablesName, nil
	case *ast.DropIndexStmt:
		return []string{x.Table.Name.String()}, nil
	case *ast.AlterTableStmt:
		return []string{x.Table.Name.String()}, nil
	case *ast.RenameTableStmt:
		return []string{x.TableToTables[0].OldTable.Name.String()}, nil
	case ast.DDLNode:
		return []string{}, nil
	default:
		return nil, errors.New("stmt is not a DDLNode")
	}
}

// GetTablesNeededNonExist reports the table name that conflicts with ast.StmtNode
// the specified ast.StmtNode must be a DDLNode
func GetTablesNeededNonExist(stmt ast.StmtNode) ([]string, error) {
	switch x := stmt.(type) {
	case *ast.CreateTableStmt:
		return []string{x.Table.Name.String()}, nil
	case *ast.RenameTableStmt:
		return []string{x.TableToTables[0].NewTable.Name.String()}, nil
	case ast.DDLNode:
		return []string{}, nil
	default:
		return nil, errors.New("stmt is not a DDLNode")
	}
}

// IsDDL reports weather the table DDLNode
func IsDDL(stmt ast.StmtNode) bool {
	_, isDDL := stmt.(ast.DDLNode)
	return isDDL
}
