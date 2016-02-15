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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/db"
	"github.com/pingcap/tidb/sessionctx/variable"
)

// SimpleExec represents simple statement executor.
// For statements do simple execution, like `USE`, 'SET` ...
type SimpleExec struct {
	Statement ast.StmtNode
	ctx       context.Context
	done      bool
}

// Fields implements Executor Fields interface.
func (e *SimpleExec) Fields() []*ast.ResultField {
	return nil
}

// Next implements Execution Next interface.
func (e *SimpleExec) Next() (*Row, error) {
	if e.done {
		return nil, nil
	}
	var err error
	switch x := e.Statement.(type) {
	case *ast.UseStmt:
		err = e.executeUse(x)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	e.done = true
	return nil, nil
}

// Close implements Executor Close interface.
func (e *SimpleExec) Close() error {
	return nil
}

func (e *SimpleExec) executeUse(s *ast.UseStmt) error {
	dbname := model.NewCIStr(s.DBName)
	dbinfo, exists := sessionctx.GetDomain(e.ctx).InfoSchema().SchemaByName(dbname)
	if !exists {
		return infoschema.DatabaseNotExists.Gen("database %s not exists", dbname)
	}
	db.BindCurrentSchema(e.ctx, dbname.O)
	// character_set_database is the character set used by the default database.
	// The server sets this variable whenever the default database changes.
	// See: http://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_character_set_database
	sessionVars := variable.GetSessionVars(e.ctx)
	sessionVars.Systems[variable.CharsetDatabase] = dbinfo.Charset
	sessionVars.Systems[variable.CollationDatabase] = dbinfo.Collate
	return nil
}
