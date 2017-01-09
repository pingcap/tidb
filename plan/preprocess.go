// Copyright 2015 PingCAP, Inc.
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

package plan

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/privilege"
)

// Preprocess does preprocess work for optimizer.
func Preprocess(node ast.Node, info infoschema.InfoSchema, ctx context.Context) error {
	if err := ResolveName(node, info, ctx); err != nil {
		return errors.Trace(err)
	}

	if checker := privilege.GetPrivilegeChecker(ctx); checker != nil {
		if err := CheckPrivilege(node, ctx, checker); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// CheckPrivilege is exposed for testing.
func CheckPrivilege(node ast.Node, ctx context.Context, checker privilege.Checker) error {
	privChecker := privilegeChecker{
		Checker: checker,
		ctx:     ctx,
		pass:    true,
	}
	node.Accept(&privChecker)
	if !privChecker.pass {
		return errors.New("check privilege failed")
	}

	return nil
}

// privilegeChecker wraps privilege.Checker and implements ast.Visitor interface.
type privilegeChecker struct {
	ctx context.Context
	privilege.Checker
	pass bool
}

func (pc *privilegeChecker) check(t *ast.TableName, priv mysql.PrivilegeType) {
	// TODO: Call Checker's check function
}

func (pc *privilegeChecker) Enter(in ast.Node) (ast.Node, bool) {
	if !pc.pass {
		return in, true
	}
	switch v := in.(type) {
	case *ast.AdminStmt:
	case *ast.AlterTableStmt:
		pc.check(v.Table, mysql.AlterPriv)
	case *ast.AnalyzeTableStmt:
	case *ast.CreateIndexStmt:
		pc.check(v.Table, mysql.IndexPriv)
	case *ast.CreateTableStmt:
		pc.check(v.Table, mysql.CreatePriv)
	case *ast.DeleteStmt:
	case *ast.DeleteTableList:
	case *ast.DropTableStmt:
	case *ast.DropIndexStmt:
		pc.check(v.Table, mysql.IndexPriv)
	case *ast.FieldList:
	case *ast.InsertStmt:
	case *ast.LoadDataStmt:
	case *ast.SelectStmt:
	case *ast.SetStmt:
	case *ast.ShowStmt:
		pc.check(v.Table, mysql.ShowDBPriv)
	case *ast.TruncateTableStmt:
		pc.check(v.Table, mysql.DeletePriv)
	case *ast.UnionStmt:
	case *ast.UpdateStmt:
	}
	return in, false
}

func (pc *privilegeChecker) Leave(in ast.Node) (ast.Node, bool) {
	return in, pc.pass
}
