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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// recoverSchemaMaterializedViewInfos restores the persisted maintenance baselines
// in the same transaction that makes the recovered database public.
func (w *worker) recoverSchemaMaterializedViewInfos(jobCtx *jobContext, tables []*model.RecoverTableInfo, snapshotTS uint64) error {
	var mviewIDs, mlogIDs []int64
	for _, info := range tables {
		if info.TableInfo.MaterializedView != nil {
			mviewIDs = append(mviewIDs, info.TableInfo.ID)
		}
		if info.TableInfo.MaterializedViewLog != nil {
			mlogIDs = append(mlogIDs, info.TableInfo.ID)
		}
	}
	if len(mviewIDs) == 0 && len(mlogIDs) == 0 {
		return nil
	}
	jobCtx.mustRollbackTxnOnError = true
	readCtx, err := w.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.Put(readCtx)
	ctx := kv.WithInternalSourceType(jobCtx.stepCtx, kv.InternalTxnDDL)
	for _, info := range []struct {
		tableName string
		idColumn  string
		columns   []string
		ids       []int64
	}{
		{"tidb_mview_refresh_info", "MVIEW_ID", []string{"MVIEW_ID", "LAST_SUCCESS_READ_TSO", "LAST_SUCCESS_REFRESH_END_UNIX_SECONDS", "NEXT_REFRESH_UNIX_SECONDS"}, mviewIDs},
		{"tidb_mlog_purge_info", "MLOG_ID", []string{"MLOG_ID", "NEXT_PURGE_UNIX_SECONDS", "LAST_PURGED_TSO"}, mlogIDs},
		{"tidb_mview_fast_refresh_source_info", "MVIEW_ID", []string{"MVIEW_ID", "MLOG_ID"}, mviewIDs},
	} {
		for start := 0; start < len(info.ids); start += materializedViewInfoDeleteBatchSize {
			batch := info.ids[start:min(start+materializedViewInfoDeleteBatchSize, len(info.ids))]
			readArgs := make([]any, 0, len(batch)+3)
			readArgs = append(readArgs, info.tableName, info.idColumn)
			for _, id := range batch {
				readArgs = append(readArgs, id)
			}
			readArgs = append(readArgs, info.idColumn)
			/* #nosec G202: columns are fixed above; identifiers and values are escaped via placeholders. */
			query := "SELECT " + strings.Join(info.columns, ", ") + " FROM mysql.%n WHERE %n IN (" + strings.Repeat("%?,", len(batch)-1) + "%?) ORDER BY %n"
			rows, fields, err := readCtx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx,
				[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionWithSnapshot(snapshotTS)}, query, readArgs...)
			if err != nil {
				return errors.Annotatef(err, "flashback database: read mysql.%s at TSO %d", info.tableName, snapshotTS)
			}
			seen := make(map[int64]struct{}, len(batch))
			writeArgs := make([]any, 0, len(rows)*len(info.columns))
			for _, row := range rows {
				seen[row.GetInt64(0)] = struct{}{}
				for i, field := range fields {
					value := row.GetDatum(i, &field.Column.FieldType)
					writeArgs = append(writeArgs, value.GetValue())
				}
			}
			if len(seen) != len(batch) {
				return dbterror.ErrInvalidDDLJob.GenWithStack("flashback database: maintenance records missing from mysql.%s at TSO %d", info.tableName, snapshotTS)
			}
			values := "(" + strings.Repeat("%?,", len(info.columns)-1) + "%?)"
			/* #nosec G202: columns are fixed above; identifiers and values are escaped by sqlescape. */
			insertSQL := "INSERT INTO mysql.%n (" + strings.Join(info.columns, ", ") + ") VALUES " + strings.Repeat(values+",", len(rows)-1) + values
			_, err = w.sess.Execute(ctx, sqlescape.MustEscapeSQL(insertSQL, append([]any{info.tableName}, writeArgs...)...), "recover-schema-mview-info")
			failpoint.Inject("mockRecoverSchemaMaterializedViewInfoError", func(val failpoint.Value) {
				err = errors.New(val.(string))
			})
			if err != nil {
				return errors.Annotatef(err, "flashback database: restore mysql.%s", info.tableName)
			}
		}
	}
	return nil
}

func rewriteRecoveredViewSchema(info *model.TableInfo, oldName, newName pmodel.CIStr) error {
	if info.View == nil && info.MaterializedView == nil {
		return nil
	}
	p := parser.New()
	var definition *string
	restoreFlags := format.DefaultRestoreFlags
	if info.MaterializedView != nil {
		definition = &info.MaterializedView.SQLContent
		// Canonical SQL escapes backslashes regardless of the creation SQL mode.
		p.SetSQLMode(info.MaterializedView.DefinitionSQLMode &^ mysql.ModeNoBackslashEscapes)
		// Match the canonical form used when materialized views are created.
		restoreFlags |= format.RestoreStringWithoutCharset
	} else {
		// Ordinary views preserve charset introducers, as BuildViewInfo does.
		definition = &info.View.SelectStmt
	}
	stmt, err := p.ParseOneStmt(*definition, info.Charset, info.Collate)
	if err != nil {
		return errors.Annotatef(err, "flashback database: parse definition of view %s", info.Name.O)
	}
	stmt.Accept(&recoveredViewSchemaRewriter{oldName: oldName, newName: newName})
	var sb strings.Builder
	if err := stmt.Restore(format.NewRestoreCtx(restoreFlags, &sb)); err != nil {
		return errors.Annotatef(err, "flashback database: rewrite definition of view %s", info.Name.O)
	}
	*definition = sb.String()
	return nil
}

type recoveredViewSchemaRewriter struct {
	oldName pmodel.CIStr
	newName pmodel.CIStr
}

func (v *recoveredViewSchemaRewriter) Enter(node ast.Node) (ast.Node, bool) {
	switch n := node.(type) {
	case *ast.TableName:
		if n.Schema.L == v.oldName.L {
			n.Schema = v.newName
		}
	case *ast.ColumnName:
		if n.Schema.L == v.oldName.L {
			n.Schema = v.newName
		}
	case *ast.SelectField:
		// SelectField.Accept does not visit its wildcard.
		if n.WildCard != nil && n.WildCard.Schema.L == v.oldName.L {
			n.WildCard.Schema = v.newName
		}
	case *ast.TableOptimizerHint:
		for i := range n.Tables {
			if n.Tables[i].DBName.L == v.oldName.L {
				n.Tables[i].DBName = v.newName
			}
		}
	}
	return node, false
}

func (*recoveredViewSchemaRewriter) Leave(node ast.Node) (ast.Node, bool) {
	return node, true
}
