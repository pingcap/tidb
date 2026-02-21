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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)
func composeGlobalPrivUpdate(sql *strings.Builder, priv mysql.PrivilegeType, value string) error {
	if priv != mysql.AllPriv {
		if priv != mysql.GrantPriv && !mysql.AllGlobalPrivs.Has(priv) {
			return exeerrors.ErrWrongUsage.GenWithStackByArgs("GLOBAL GRANT", "NON-GLOBAL PRIVILEGES")
		}
		sqlescape.MustFormatSQL(sql, "%n=%?", priv.ColumnString(), value)
		return nil
	}

	for i, v := range mysql.AllGlobalPrivs {
		if i > 0 {
			sqlescape.MustFormatSQL(sql, ",")
		}
		sqlescape.MustFormatSQL(sql, "%n=%?", v.ColumnString(), value)
	}
	return nil
}

// composeDBPrivUpdate composes update stmt assignment list for db scope privilege update.
func composeDBPrivUpdate(sql *strings.Builder, priv mysql.PrivilegeType, value string) error {
	if priv != mysql.AllPriv {
		if priv != mysql.GrantPriv && !mysql.AllDBPrivs.Has(priv) {
			return exeerrors.ErrWrongUsage.GenWithStackByArgs("DB GRANT", "NON-DB PRIVILEGES")
		}
		sqlescape.MustFormatSQL(sql, "%n=%?", priv.ColumnString(), value)
		return nil
	}

	for i, p := range mysql.AllDBPrivs {
		if i > 0 {
			sqlescape.MustFormatSQL(sql, ",")
		}
		sqlescape.MustFormatSQL(sql, "%n=%?", p.ColumnString(), value)
	}
	return nil
}

// composeTablePrivUpdateForGrant composes update stmt assignment list for table scope privilege update.
func composeTablePrivUpdateForGrant(ctx sessionctx.Context, sql *strings.Builder, priv mysql.PrivilegeType, name string, host string, db string, tbl string) error {
	var newTablePriv, newColumnPriv []string
	if priv != mysql.AllPriv {
		currTablePriv, currColumnPriv, err := getTablePriv(ctx, name, host, db, tbl)
		if err != nil {
			return err
		}
		newTablePriv = SetFromString(currTablePriv)
		newTablePriv = addToSet(newTablePriv, priv.SetString())

		newColumnPriv = SetFromString(currColumnPriv)
		if mysql.AllColumnPrivs.Has(priv) {
			newColumnPriv = addToSet(newColumnPriv, priv.SetString())
		}
	} else {
		for _, p := range mysql.AllTablePrivs {
			newTablePriv = addToSet(newTablePriv, p.SetString())
		}

		for _, p := range mysql.AllColumnPrivs {
			newColumnPriv = addToSet(newColumnPriv, p.SetString())
		}
	}

	sqlescape.MustFormatSQL(sql, `Table_priv=%?, Column_priv=%?, Grantor=%?`, setToString(newTablePriv), setToString(newColumnPriv), ctx.GetSessionVars().User.String())
	return nil
}

// composeColumnPrivUpdateForGrant composes update stmt assignment list for column scope privilege update.
func composeColumnPrivUpdateForGrant(ctx sessionctx.Context, sql *strings.Builder, priv mysql.PrivilegeType, name string, host string, db string, tbl string, col string) error {
	var newColumnPriv []string
	if priv != mysql.AllPriv {
		currColumnPriv, err := getColumnPriv(ctx, name, host, db, tbl, col)
		if err != nil {
			return err
		}
		newColumnPriv = SetFromString(currColumnPriv)
		newColumnPriv = addToSet(newColumnPriv, priv.SetString())
	} else {
		for _, p := range mysql.AllColumnPrivs {
			newColumnPriv = addToSet(newColumnPriv, p.SetString())
		}
	}

	sqlescape.MustFormatSQL(sql, `Column_priv=%?`, setToString(newColumnPriv))
	return nil
}

// recordExists is a helper function to check if the sql returns any row.
func recordExists(sctx sessionctx.Context, sql string, args ...any) (bool, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	rs, err := sctx.GetSQLExecutor().ExecuteInternal(ctx, sql, args...)
	if err != nil {
		return false, err
	}
	rows, _, err := getRowsAndFields(sctx, rs)
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}

// globalPrivEntryExists checks if there is an entry with key user-host in mysql.global_priv.
func globalPrivEntryExists(ctx sessionctx.Context, name string, host string) (bool, error) {
	return recordExists(ctx, `SELECT * FROM %n.%n WHERE User=%? AND Host=%?;`, mysql.SystemDB, mysql.GlobalPrivTable, name, host)
}

// dbUserExists checks if there is an entry with key user-host-db in mysql.DB.
func dbUserExists(ctx sessionctx.Context, name string, host string, db string) (bool, error) {
	return recordExists(ctx, `SELECT * FROM %n.%n WHERE User=%? AND Host=%? AND DB=%?;`, mysql.SystemDB, mysql.DBTable, name, host, db)
}

// tableUserExists checks if there is an entry with key user-host-db-tbl in mysql.Tables_priv.
func tableUserExists(ctx sessionctx.Context, name string, host string, db string, tbl string) (bool, error) {
	return recordExists(ctx, `SELECT * FROM %n.%n WHERE User=%? AND Host=%? AND DB=%? AND Table_name=%?;`, mysql.SystemDB, mysql.TablePrivTable, name, host, db, tbl)
}

// columnPrivEntryExists checks if there is an entry with key user-host-db-tbl-col in mysql.Columns_priv.
func columnPrivEntryExists(ctx sessionctx.Context, name string, host string, db string, tbl string, col string) (bool, error) {
	return recordExists(ctx, `SELECT * FROM %n.%n WHERE User=%? AND Host=%? AND DB=%? AND Table_name=%? AND Column_name=%?;`, mysql.SystemDB, mysql.ColumnPrivTable, name, host, db, tbl, col)
}

// getTablePriv gets current table scope privilege set from mysql.Tables_priv.
// Return Table_priv and Column_priv.
func getTablePriv(sctx sessionctx.Context, name string, host string, db string, tbl string) (tPriv, cPriv string, err error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	rs, err := sctx.GetSQLExecutor().ExecuteInternal(ctx, `SELECT Table_priv, Column_priv FROM %n.%n WHERE User=%? AND Host=%? AND DB=%? AND Table_name=%?`, mysql.SystemDB, mysql.TablePrivTable, name, host, db, tbl)
	if err != nil {
		return "", "", err
	}
	rows, fields, err := getRowsAndFields(sctx, rs)
	if err != nil {
		return "", "", errors.Errorf("get table privilege fail for %s %s %s %s: %v", name, host, db, tbl, err)
	}
	if len(rows) < 1 {
		return "", "", errors.Errorf("get table privilege fail for %s %s %s %s", name, host, db, tbl)
	}
	row := rows[0]
	if fields[0].Column.GetType() == mysql.TypeSet {
		tablePriv := row.GetSet(0)
		tPriv = tablePriv.Name
	}
	if fields[1].Column.GetType() == mysql.TypeSet {
		columnPriv := row.GetSet(1)
		cPriv = columnPriv.Name
	}
	return tPriv, cPriv, nil
}

// getColumnPriv gets current column scope privilege set from mysql.Columns_priv.
// Return Column_priv.
func getColumnPriv(sctx sessionctx.Context, name string, host string, db string, tbl string, col string) (string, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	rs, err := sctx.GetSQLExecutor().ExecuteInternal(ctx, `SELECT Column_priv FROM %n.%n WHERE User=%? AND Host=%? AND DB=%? AND Table_name=%? AND Column_name=%?;`, mysql.SystemDB, mysql.ColumnPrivTable, name, host, db, tbl, col)
	if err != nil {
		return "", err
	}
	rows, fields, err := getRowsAndFields(sctx, rs)
	if err != nil {
		return "", errors.Errorf("get column privilege fail for %s %s %s %s: %s", name, host, db, tbl, err)
	}
	if len(rows) < 1 {
		return "", errors.Errorf("get column privilege fail for %s %s %s %s %s", name, host, db, tbl, col)
	}
	cPriv := ""
	if fields[0].Column.GetType() == mysql.TypeSet {
		setVal := rows[0].GetSet(0)
		cPriv = setVal.Name
	}
	return cPriv, nil
}

// getTargetSchemaAndTable finds the schema and table by dbName and tableName.
func getTargetSchemaAndTable(ctx context.Context, sctx sessionctx.Context, dbName, tableName string, is infoschema.InfoSchema) (string, table.Table, error) {
	if len(dbName) == 0 {
		dbName = sctx.GetSessionVars().CurrentDB
		if len(dbName) == 0 {
			return "", nil, errors.New("miss DB name for grant privilege")
		}
	}
	name := ast.NewCIStr(tableName)
	tbl, err := is.TableByName(ctx, ast.NewCIStr(dbName), name)
	if terror.ErrorEqual(err, infoschema.ErrTableNotExists) {
		return dbName, nil, err
	}
	if err != nil {
		return "", nil, err
	}
	return dbName, tbl, nil
}

// getRowsAndFields is used to extract rows from record sets.
func getRowsAndFields(sctx sessionctx.Context, rs sqlexec.RecordSet) ([]chunk.Row, []*resolve.ResultField, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	if rs == nil {
		return nil, nil, errors.Errorf("nil recordset")
	}
	rows, err := getRowFromRecordSet(ctx, sctx, rs)
	if err != nil {
		return nil, nil, err
	}
	if err = rs.Close(); err != nil {
		return nil, nil, err
	}
	return rows, rs.Fields(), nil
}

func getRowFromRecordSet(ctx context.Context, se sessionctx.Context, rs sqlexec.RecordSet) ([]chunk.Row, error) {
	var rows []chunk.Row
	req := rs.NewChunk(nil)
	for {
		err := rs.Next(ctx, req)
		if err != nil || req.NumRows() == 0 {
			return rows, err
		}
		iter := chunk.NewIterator4Chunk(req)
		for r := iter.Begin(); r != iter.End(); r = iter.Next() {
			rows = append(rows, r)
		}
		req = chunk.Renew(req, se.GetSessionVars().MaxChunkSize)
	}
}
