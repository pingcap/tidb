// Copyright 2026 PingCAP, Inc.
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

package ddl

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

func (e *executor) CreateModel(ctx sessionctx.Context, stmt *ast.CreateModelStmt) error {
	schema := stmt.Name.Schema
	if schema.L == "" {
		schema = ast.NewCIStr(ctx.GetExprCtx().GetEvalCtx().CurrentDB())
	}
	if schema.L == "" {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema.O)
	}
	if _, ok := e.infoCache.GetLatest().SchemaByName(schema); !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema.O)
	}

	modelName := stmt.Name.Name
	exec := ctx.GetRestrictedSQLExecutor()
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	_, exists, err := getModelID(internalCtx, exec, schema.L, modelName.L, true)
	if err != nil {
		return err
	}
	if exists {
		err = infoschema.ErrModelExists.GenWithStackByArgs(schema.O, modelName.O)
		if stmt.IfNotExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}

	createdBy := currentUserString(ctx)
	_, _, err = exec.ExecRestrictedSQL(
		internalCtx,
		nil,
		"INSERT INTO mysql.tidb_model (db_name, model_name, owner, created_by, updated_by, status) VALUES (%?, %?, %?, %?, %?, 'public')",
		schema.L, modelName.L, createdBy, createdBy, createdBy,
	)
	if err != nil {
		return errors.Trace(err)
	}

	modelID, _, err := getModelID(internalCtx, exec, schema.L, modelName.L, true)
	if err != nil {
		return err
	}
	if modelID == 0 {
		return errors.New("model id not found after insert")
	}

	inputSchema, err := modelColumnDefsToJSON(stmt.InputCols)
	if err != nil {
		return err
	}
	outputSchema, err := modelColumnDefsToJSON(stmt.OutputCols)
	if err != nil {
		return err
	}
	engine := strings.ToUpper(stmt.Engine)

	_, _, err = exec.ExecRestrictedSQL(
		internalCtx,
		nil,
		"INSERT INTO mysql.tidb_model_version (model_id, version, engine, location, checksum, input_schema, output_schema, options, created_by, status) VALUES (%?, %?, %?, %?, %?, %?, %?, %?, %?, 'public')",
		modelID, int64(1), engine, stmt.Location, stmt.Checksum, inputSchema, outputSchema, nil, createdBy,
	)
	return errors.Trace(err)
}

func (e *executor) AlterModel(ctx sessionctx.Context, stmt *ast.AlterModelStmt) error {
	schema := stmt.Name.Schema
	if schema.L == "" {
		schema = ast.NewCIStr(ctx.GetExprCtx().GetEvalCtx().CurrentDB())
	}
	if schema.L == "" {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema.O)
	}
	if _, ok := e.infoCache.GetLatest().SchemaByName(schema); !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema.O)
	}

	modelName := stmt.Name.Name
	exec := ctx.GetRestrictedSQLExecutor()
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	modelID, exists, err := getModelID(internalCtx, exec, schema.L, modelName.L, false)
	if err != nil {
		return err
	}
	if !exists {
		return infoschema.ErrModelNotExists.GenWithStackByArgs(schema.O, modelName.O)
	}

	rows, _, err := exec.ExecRestrictedSQL(
		internalCtx,
		nil,
		"SELECT version, engine, input_schema, output_schema FROM mysql.tidb_model_version WHERE model_id = %? AND deleted_at IS NULL ORDER BY version DESC LIMIT 1",
		modelID,
	)
	if err != nil {
		return errors.Trace(err)
	}
	if len(rows) == 0 {
		return infoschema.ErrModelNotExists.GenWithStackByArgs(schema.O, modelName.O)
	}

	lastVersion := rows[0].GetInt64(0)
	engine := rows[0].GetString(1)
	inputJSON := rows[0].GetJSON(2)
	outputJSON := rows[0].GetJSON(3)
	inputSchemaBytes, err := inputJSON.MarshalJSON()
	if err != nil {
		return errors.Trace(err)
	}
	outputSchemaBytes, err := outputJSON.MarshalJSON()
	if err != nil {
		return errors.Trace(err)
	}
	inputSchema := string(inputSchemaBytes)
	outputSchema := string(outputSchemaBytes)
	createdBy := currentUserString(ctx)

	_, _, err = exec.ExecRestrictedSQL(
		internalCtx,
		nil,
		"INSERT INTO mysql.tidb_model_version (model_id, version, engine, location, checksum, input_schema, output_schema, options, created_by, status) VALUES (%?, %?, %?, %?, %?, %?, %?, %?, %?, 'public')",
		modelID, lastVersion+1, engine, stmt.Location, stmt.Checksum, inputSchema, outputSchema, nil, createdBy,
	)
	if err != nil {
		return errors.Trace(err)
	}

	_, _, err = exec.ExecRestrictedSQL(
		internalCtx,
		nil,
		"UPDATE mysql.tidb_model SET updated_by = %? WHERE id = %?",
		createdBy, modelID,
	)
	return errors.Trace(err)
}

func (e *executor) DropModel(ctx sessionctx.Context, stmt *ast.DropModelStmt) error {
	schema := stmt.Name.Schema
	if schema.L == "" {
		schema = ast.NewCIStr(ctx.GetExprCtx().GetEvalCtx().CurrentDB())
	}
	if schema.L == "" {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema.O)
	}
	if _, ok := e.infoCache.GetLatest().SchemaByName(schema); !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(schema.O)
	}

	modelName := stmt.Name.Name
	exec := ctx.GetRestrictedSQLExecutor()
	internalCtx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	modelID, exists, err := getModelID(internalCtx, exec, schema.L, modelName.L, false)
	if err != nil {
		return err
	}
	if !exists {
		err = infoschema.ErrModelNotExists.GenWithStackByArgs(schema.O, modelName.O)
		if stmt.IfExists {
			ctx.GetSessionVars().StmtCtx.AppendNote(err)
			return nil
		}
		return err
	}

	updatedBy := currentUserString(ctx)
	_, _, err = exec.ExecRestrictedSQL(
		internalCtx,
		nil,
		"UPDATE mysql.tidb_model SET status = 'deleted', updated_by = %?, deleted_at = CURRENT_TIMESTAMP WHERE id = %?",
		updatedBy, modelID,
	)
	return errors.Trace(err)
}

func getModelID(ctx context.Context, exec sqlexec.RestrictedSQLExecutor, schema, name string, includeDeleted bool) (int64, bool, error) {
	query := "SELECT id FROM mysql.tidb_model WHERE db_name = %? AND model_name = %?"
	if !includeDeleted {
		query += " AND deleted_at IS NULL"
	}
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, query, schema, name)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	if len(rows) == 0 {
		return 0, false, nil
	}
	return rows[0].GetInt64(0), true, nil
}

func modelColumnDefsToJSON(cols []*ast.ColumnDef) (string, error) {
	colStrs := make([]string, 0, len(cols))
	for _, col := range cols {
		var buf bytes.Buffer
		restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &buf)
		if err := col.Restore(restoreCtx); err != nil {
			return "", errors.Trace(err)
		}
		colStrs = append(colStrs, buf.String())
	}
	data, err := json.Marshal(colStrs)
	if err != nil {
		return "", errors.Trace(err)
	}
	return string(data), nil
}

func currentUserString(ctx sessionctx.Context) string {
	if ctx == nil {
		return ""
	}
	//nolint:forbidigo // User is stored in session vars; no alternate accessor on sessionctx.Context.
	user := ctx.GetSessionVars().User
	if user == nil {
		return ""
	}
	return user.String()
}
