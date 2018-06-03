// Copyright 2016 PingCAP, Inc.
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

package executor

import (
	"strings"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/schema_checker"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

// DDLExec represents a DDL executor.
// It grabs a DDL instance from Domain, calling the DDL methods to do the work.
type DDLExec struct {
	baseExecutor

	stmt ast.StmtNode
	is   infoschema.InfoSchema
	done bool
}

func (e *DDLExec) toErr(err error) error {
	dom := domain.GetDomain(e.ctx)
	checker := schema_checker.NewSchemaChecker(dom, e.is.SchemaMetaVersion(), nil)
	schemaInfoErr := checker.Check(e.ctx.Txn().StartTS())
	if schemaInfoErr != nil {
		return errors.Trace(schemaInfoErr)
	}
	return err
}

var connID uint64
var ParallelCnt int32

// Next implements the Executor Next interface.
func (e *DDLExec) Next(ctx context.Context, chk *chunk.Chunk) (err error) {
	if e.done {
		return nil
	}
	e.done = true

	if ParallelCnt != 0 {
		atomic.AddInt32(&ParallelCnt, 1)
		for {
			cnt := atomic.LoadInt32(&ParallelCnt)
			log.Warnf("                      cnt %v", cnt)
			if cnt == 3 {
				break
			}
			time.Sleep(time.Millisecond * 1)
		}
		atomic.CompareAndSwapUint64(&connID, 0, e.ctx.GetSessionVars().ConnectionID)
		for {
			id := atomic.LoadUint64(&connID)
			cnt := atomic.LoadInt32(&ParallelCnt)
			log.Warnf("------------------- id %v, cnt %v", id, cnt)
			if id == e.ctx.GetSessionVars().ConnectionID || cnt == 0 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	// gofail: var parallelCnt int
	// for parallelCnt == 0 {
	// 	time.Sleep(time.Millisecond * 5)
	// }
	// atomic.CompareAndSwapUint64(&connID, 0, e.ctx.GetSessionVars().ConnectionID)
	// for {
	//	id := atomic.LoadUint64(&connID)
	//	if id == e.ctx.GetSessionVars().ConnectionID || parallelCnt == 0 {
	//  log.Warnf("------------------- id %v, cnt %v", connID, parallelCnt)
	// 		break
	// 	}
	// 	time.Sleep(10 * time.Millisecond)
	// }

	switch x := e.stmt.(type) {
	case *ast.TruncateTableStmt:
		err = e.executeTruncateTable(x)
	case *ast.CreateDatabaseStmt:
		err = e.executeCreateDatabase(x)
	case *ast.CreateTableStmt:
		err = e.executeCreateTable(x)
	case *ast.CreateIndexStmt:
		err = e.executeCreateIndex(x)
	case *ast.DropDatabaseStmt:
		err = e.executeDropDatabase(x)
	case *ast.DropTableStmt:
		err = e.executeDropTable(x)
	case *ast.DropIndexStmt:
		err = e.executeDropIndex(x)
	case *ast.AlterTableStmt:
		err = e.executeAlterTable(x)
	case *ast.RenameTableStmt:
		err = e.executeRenameTable(x)
	}
	if err != nil {
		return errors.Trace(e.toErr(err))
	}

	dom := domain.GetDomain(e.ctx)
	// Update InfoSchema in TxnCtx, so it will pass schema check.
	is := dom.InfoSchema()
	txnCtx := e.ctx.GetSessionVars().TxnCtx
	txnCtx.InfoSchema = is
	txnCtx.SchemaVersion = is.SchemaMetaVersion()
	// DDL will force commit old transaction, after DDL, in transaction status should be false.
	e.ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, false)
	return nil
}

func (e *DDLExec) executeTruncateTable(s *ast.TruncateTableStmt) error {
	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	err := domain.GetDomain(e.ctx).DDL().TruncateTable(e.ctx, ident)
	return errors.Trace(err)
}

func (e *DDLExec) executeRenameTable(s *ast.RenameTableStmt) error {
	if len(s.TableToTables) != 1 {
		// Now we only allow one schema changing at the same time.
		return errors.Errorf("can't run multi schema change")
	}
	oldIdent := ast.Ident{Schema: s.OldTable.Schema, Name: s.OldTable.Name}
	newIdent := ast.Ident{Schema: s.NewTable.Schema, Name: s.NewTable.Name}
	err := domain.GetDomain(e.ctx).DDL().RenameTable(e.ctx, oldIdent, newIdent)
	return errors.Trace(err)
}

func (e *DDLExec) executeCreateDatabase(s *ast.CreateDatabaseStmt) error {
	var opt *ast.CharsetOpt
	if len(s.Options) != 0 {
		opt = &ast.CharsetOpt{}
		for _, val := range s.Options {
			switch val.Tp {
			case ast.DatabaseOptionCharset:
				opt.Chs = val.Value
			case ast.DatabaseOptionCollate:
				opt.Col = val.Value
			}
		}
	}
	err := domain.GetDomain(e.ctx).DDL().CreateSchema(e.ctx, model.NewCIStr(s.Name), opt)
	if err != nil {
		if infoschema.ErrDatabaseExists.Equal(err) && s.IfNotExists {
			err = nil
		}
	}
	return errors.Trace(err)
}

func (e *DDLExec) executeCreateTable(s *ast.CreateTableStmt) error {
	err := domain.GetDomain(e.ctx).DDL().CreateTable(e.ctx, s)
	return errors.Trace(err)
}

func (e *DDLExec) executeCreateIndex(s *ast.CreateIndexStmt) error {
	ident := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	err := domain.GetDomain(e.ctx).DDL().CreateIndex(e.ctx, ident, s.Unique, model.NewCIStr(s.IndexName), s.IndexColNames, s.IndexOption)
	return errors.Trace(err)
}

func (e *DDLExec) executeDropDatabase(s *ast.DropDatabaseStmt) error {
	dbName := model.NewCIStr(s.Name)
	err := domain.GetDomain(e.ctx).DDL().DropSchema(e.ctx, dbName)
	if infoschema.ErrDatabaseNotExists.Equal(err) {
		if s.IfExists {
			err = nil
		} else {
			err = infoschema.ErrDatabaseDropExists.GenByArgs(s.Name)
		}
	}
	sessionVars := e.ctx.GetSessionVars()
	if err == nil && strings.ToLower(sessionVars.CurrentDB) == dbName.L {
		sessionVars.CurrentDB = ""
		err = variable.SetSessionSystemVar(sessionVars, variable.CharsetDatabase, types.NewStringDatum("utf8"))
		if err != nil {
			return errors.Trace(err)
		}
		err = variable.SetSessionSystemVar(sessionVars, variable.CollationDatabase, types.NewStringDatum("utf8_unicode_ci"))
		if err != nil {
			return errors.Trace(err)
		}
	}
	return errors.Trace(err)
}

func (e *DDLExec) executeDropTable(s *ast.DropTableStmt) error {
	var notExistTables []string
	for _, tn := range s.Tables {
		fullti := ast.Ident{Schema: tn.Schema, Name: tn.Name}
		_, ok := e.is.SchemaByName(tn.Schema)
		if !ok {
			// TODO: we should return special error for table not exist, checking "not exist" is not enough,
			// because some other errors may contain this error string too.
			notExistTables = append(notExistTables, fullti.String())
			continue
		}
		_, err := e.is.TableByName(tn.Schema, tn.Name)
		if err != nil && infoschema.ErrTableNotExists.Equal(err) {
			notExistTables = append(notExistTables, fullti.String())
			continue
		} else if err != nil {
			return errors.Trace(err)
		}

		err = domain.GetDomain(e.ctx).DDL().DropTable(e.ctx, fullti)
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableNotExists.Equal(err) {
			notExistTables = append(notExistTables, fullti.String())
		} else if err != nil {
			return errors.Trace(err)
		}
	}
	if len(notExistTables) > 0 && !s.IfExists {
		return infoschema.ErrTableDropExists.GenByArgs(strings.Join(notExistTables, ","))
	}
	return nil
}

func (e *DDLExec) executeDropIndex(s *ast.DropIndexStmt) error {
	ti := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	err := domain.GetDomain(e.ctx).DDL().DropIndex(e.ctx, ti, model.NewCIStr(s.IndexName))
	if (infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableNotExists.Equal(err)) && s.IfExists {
		err = nil
	}
	return errors.Trace(err)
}

func (e *DDLExec) executeAlterTable(s *ast.AlterTableStmt) error {
	ti := ast.Ident{Schema: s.Table.Schema, Name: s.Table.Name}
	err := domain.GetDomain(e.ctx).DDL().AlterTable(e.ctx, ti, s.Specs)
	return errors.Trace(err)
}
