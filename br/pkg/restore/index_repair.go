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

package restore

import (
	"context"
	"fmt"
	"strings"

	"github.com/fatih/color"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/restore/ingestrec"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/ast"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// session count for repairing ingest indexes. Currently only one TiDB node executes adding index jobs
// at the same time and the add-index job concurrency is about min(10, `TiDB CPUs / 4`).
const defaultRepairIndexSessionCount uint = 10

func colsToStr(cols []ast.CIStr) string {
	var str strings.Builder
	for i, col := range cols {
		if i != 0 {
			str.WriteString(",")
		}
		str.WriteString(col.O)
	}
	return str.String()
}

const (
	alterTableDropIndexSQL         = "ALTER TABLE %n.%n DROP INDEX %n"
	alterTableAddIndexFormat       = "ALTER TABLE %%n.%%n ADD INDEX %%n(%s)"
	alterTableAddUniqueIndexFormat = "ALTER TABLE %%n.%%n ADD UNIQUE KEY %%n(%s)"
	alterTableAddPrimaryFormat     = "ALTER TABLE %%n.%%n ADD PRIMARY KEY (%s) NONCLUSTERED"
	alterTableAddForeignKeyFormat  = "ALTER TABLE %%n.%%n ADD CONSTRAINT %%n FOREIGN KEY (%s) REFERENCES %%n.%%n (%s)"
	alterTableDropForeignKeyFormat = "ALTER TABLE %n.%n DROP FOREIGN KEY %n"
	repairIngestIndexSQLFile       = "add-index.sql"
)

func generateRepairIngestIndexSQLs(
	ctx context.Context,
	dom *domain.Domain,
	ingestRecorder *ingestrec.IngestRecorder,
	checkpointMetaManager checkpoint.IndexRepairMetaManager,
) ([]checkpoint.CheckpointIngestIndexRepairSQL, []checkpoint.CheckpointForeignKeyUpdateSQL, bool, error) {
	var sqls []checkpoint.CheckpointIngestIndexRepairSQL
	var fkSqls []checkpoint.CheckpointForeignKeyUpdateSQL
	if checkpointMetaManager != nil {
		exists, err := checkpointMetaManager.ExistsCheckpointIngestIndexRepairSQLs(ctx)
		if err != nil {
			return sqls, fkSqls, false, errors.Trace(err)
		}
		if exists {
			checkpointSQLs, err := checkpointMetaManager.LoadCheckpointIngestIndexRepairSQLs(ctx)
			if err != nil {
				return sqls, fkSqls, false, errors.Trace(err)
			}
			sqls = checkpointSQLs.SQLs
			fkSqls = checkpointSQLs.FKSQLs
			log.Info("load ingest index repair sqls from checkpoint",
				zap.String("category", "ingest"),
				zap.Int("index-sql-count", len(sqls)),
				zap.Int("foreign-key-sql-count", len(fkSqls)))
			return sqls, fkSqls, true, nil
		}
	}

	if err := ingestRecorder.UpdateIndexInfo(ctx, dom.InfoSchema()); err != nil {
		return sqls, fkSqls, false, errors.Trace(err)
	}
	if err := ingestRecorder.IterateForeignKeys(func(fkRecord *ingestrec.ForeignKeyRecord) error {
		var (
			addSQL  strings.Builder
			addArgs []any = make([]any, 0, 7+len(fkRecord.Cols)+len(fkRecord.RefCols))
		)
		childCols := colsToStr(fkRecord.Cols)
		parentCols := colsToStr(fkRecord.RefCols)
		fmt.Fprintf(&addSQL, alterTableAddForeignKeyFormat, childCols, parentCols)
		addArgs = append(addArgs,
			fkRecord.ChildSchemaNameO, fkRecord.ChildTableNameO, fkRecord.Name.O, fkRecord.RefSchema.O, fkRecord.RefTable.O,
		)
		if onDelete := ast.ReferOptionType(fkRecord.OnDelete); onDelete != ast.ReferOptionNoOption {
			fmt.Fprintf(&addSQL, " ON DELETE %s", onDelete.String())
		}
		if onUpdate := ast.ReferOptionType(fkRecord.OnUpdate); onUpdate != ast.ReferOptionNoOption {
			fmt.Fprintf(&addSQL, " ON UPDATE %s", onUpdate.String())
		}
		fkSqls = append(fkSqls, checkpoint.CheckpointForeignKeyUpdateSQL{
			FKID:       fkRecord.ID,
			SchemaName: fkRecord.ChildSchemaNameO,
			TableName:  fkRecord.ChildTableNameO,
			FKName:     fkRecord.Name.O,
			AddSQL:     addSQL.String(),
			AddArgs:    addArgs,
		})
		return nil
	}); err != nil {
		return sqls, fkSqls, false, errors.Trace(err)
	}
	if err := ingestRecorder.Iterate(func(_, indexID int64, info *ingestrec.IngestIndexInfo) error {
		var (
			addSQL  strings.Builder
			addArgs []any = make([]any, 0, 5+len(info.ColumnArgs))
		)
		if info.IsPrimary {
			fmt.Fprintf(&addSQL, alterTableAddPrimaryFormat, info.ColumnList)
			addArgs = append(addArgs, info.SchemaName.O, info.TableName.O)
			addArgs = append(addArgs, info.ColumnArgs...)
		} else if info.IndexInfo.Unique {
			fmt.Fprintf(&addSQL, alterTableAddUniqueIndexFormat, info.ColumnList)
			addArgs = append(addArgs, info.SchemaName.O, info.TableName.O, info.IndexInfo.Name.O)
			addArgs = append(addArgs, info.ColumnArgs...)
		} else {
			fmt.Fprintf(&addSQL, alterTableAddIndexFormat, info.ColumnList)
			addArgs = append(addArgs, info.SchemaName.O, info.TableName.O, info.IndexInfo.Name.O)
			addArgs = append(addArgs, info.ColumnArgs...)
		}
		// WHERE CONDITION
		if len(info.IndexInfo.ConditionExprString) > 0 {
			addSQL.WriteString(" WHERE ")
			addSQL.WriteString(info.IndexInfo.ConditionExprString)
		}
		// USING BTREE/HASH/RTREE
		indexTypeStr := info.IndexInfo.Tp.String()
		if len(indexTypeStr) > 0 {
			addSQL.WriteString(" USING ")
			addSQL.WriteString(indexTypeStr)
		}
		// WITH PARSER [...]
		if info.IndexInfo.FullTextInfo != nil {
			addSQL.WriteString(" WITH PARSER ")
			addSQL.WriteString(info.IndexInfo.FullTextInfo.ParserType.SQLName())
		}
		// COMMENT [...]
		if len(info.IndexInfo.Comment) > 0 {
			addSQL.WriteString(" COMMENT %?")
			addArgs = append(addArgs, info.IndexInfo.Comment)
		}

		if info.IndexInfo.Invisible {
			addSQL.WriteString(" INVISIBLE")
		} else {
			addSQL.WriteString(" VISIBLE")
		}

		if info.IndexInfo.Global {
			addSQL.WriteString(" GLOBAL")
		} else {
			addSQL.WriteString(" LOCAL")
		}

		sqls = append(sqls, checkpoint.CheckpointIngestIndexRepairSQL{
			IndexID:    indexID,
			SchemaName: info.SchemaName,
			TableName:  info.TableName,
			IndexName:  info.IndexInfo.Name.O,
			AddSQL:     addSQL.String(),
			AddArgs:    addArgs,
		})

		return nil
	}); err != nil {
		return sqls, fkSqls, false, errors.Trace(err)
	}
	log.Info("generated ingest index repair sqls",
		zap.String("category", "ingest"),
		zap.Int("index-sql-count", len(sqls)),
		zap.Int("foreign-key-sql-count", len(fkSqls)))

	if checkpointMetaManager != nil && len(sqls) > 0 {
		if err := checkpointMetaManager.SaveCheckpointIngestIndexRepairSQLs(ctx, &checkpoint.CheckpointIngestIndexRepairSQLs{
			SQLs:   sqls,
			FKSQLs: fkSqls,
		}); err != nil {
			return sqls, fkSqls, false, errors.Trace(err)
		}
	}
	return sqls, fkSqls, false, nil
}

func writeRepairIngestIndexSQLFile(
	ctx context.Context,
	storage storeapi.Storage,
	sqls []checkpoint.CheckpointIngestIndexRepairSQL,
	fkSqls []checkpoint.CheckpointForeignKeyUpdateSQL,
) error {
	if storage == nil {
		return nil
	}

	var builder strings.Builder
	sqlCount := 0
	builder.WriteString("-- SQLs for rebuilding indexes skipped by PITR restore.\n")
	builder.WriteString("-- Execute them manually after restore if you need these indexes and foreign keys.\n")
	for _, sql := range sqls {
		if sql.IndexRepaired {
			continue
		}
		if err := sqlescape.FormatSQL(&builder, sql.AddSQL, sql.AddArgs...); err != nil {
			return errors.Trace(err)
		}
		builder.WriteString(";\n")
		sqlCount++
	}
	for _, fk := range fkSqls {
		if fk.ForeignKeyUpdated {
			continue
		}
		if err := sqlescape.FormatSQL(&builder, fk.AddSQL, fk.AddArgs...); err != nil {
			return errors.Trace(err)
		}
		builder.WriteString(";\n")
		sqlCount++
	}
	if sqlCount == 0 {
		log.Info("no repair ingest index sqls need to be written to external storage")
		return nil
	}
	if err := storage.WriteFile(ctx, repairIngestIndexSQLFile, []byte(builder.String())); err != nil {
		return errors.Trace(err)
	}
	log.Info("write repair ingest index sqls to external storage",
		zap.String("file", repairIngestIndexSQLFile),
		zap.Int("sql-count", sqlCount))
	return nil
}

// RepairIndexes rebuilds recorded indexes and their foreign keys using checkpointed DDL.
// A nil checkpoint manager disables checkpoints; a nil recorder resumes persisted SQLs.
// When addIndexSQLStorage is set, it drops the indexes and exports SQL instead of rebuilding them.
func RepairIndexes(
	ctx context.Context,
	dom *domain.Domain,
	ingestRecorder *ingestrec.IngestRecorder,
	checkpointMetaManager checkpoint.IndexRepairMetaManager,
	g glue.Glue,
	addIndexSQLStorage storeapi.Storage,
) error {
	if err := dom.Reload(); err != nil {
		return errors.Trace(err)
	}
	sqls, fkSqls, fromCheckpoint, err := generateRepairIngestIndexSQLs(ctx, dom, ingestRecorder, checkpointMetaManager)
	if err != nil {
		return errors.Trace(err)
	}

	if len(sqls) == 0 && len(fkSqls) == 0 {
		return nil
	}

	info := dom.InfoSchema()
	console := glue.GetConsole(g)
	for i, sql := range sqls {
		tableInfo, err := info.TableByName(ctx, sql.SchemaName, sql.TableName)
		if err != nil {
			return errors.Trace(err)
		}
		sqls[i].OldIndexIDFound = false
		sqls[i].IndexRepaired = false
		if fromCheckpoint {
			for _, idx := range tableInfo.Indices() {
				indexInfo := idx.Meta()
				if indexInfo.ID == sql.IndexID {
					// the original index id is not dropped
					sqls[i].OldIndexIDFound = true
					break
				}
				if indexInfo.Name.O == sql.IndexName {
					// A previous BR process may have crashed while the replacement index DDL was still running.
					if indexInfo.State != model.StatePublic {
						return errors.Errorf("index %s on %s.%s is still being rebuilt; retry restore after its DDL finishes", sql.IndexName, sql.SchemaName, sql.TableName)
					}
					progressTitle := fmt.Sprintf("repair ingest index %s for table %s.%s", sql.IndexName, sql.SchemaName, sql.TableName)
					// find the same name index, but not the same index id,
					// which means the repaired index id is created
					if _, err := fmt.Fprintf(console.Out(), "%s ... %s\n", progressTitle, color.HiGreenString("SKIPPED DUE TO CHECKPOINT MODE")); err != nil {
						return errors.Trace(err)
					}
					sqls[i].IndexRepaired = true
					break
				}
			}
		}
	}
	for i, sql := range fkSqls {
		tableInfo, err := info.TableByName(ctx, ast.NewCIStr(sql.SchemaName), ast.NewCIStr(sql.TableName))
		if err != nil {
			return errors.Trace(err)
		}
		fkSqls[i].OldForeignKeyFound = false
		fkSqls[i].ForeignKeyUpdated = false
		if fromCheckpoint {
			for _, fk := range tableInfo.Meta().ForeignKeys {
				if fk.ID == sql.FKID {
					// the original foreign key id is not dropped
					fkSqls[i].OldForeignKeyFound = true
					break
				}
				if fk.Name.O == sql.FKName {
					// find the same name foreign key, but not the same foreign key id,
					// which means the foreign key is updated and all the indexes are recreated
					fkSqls[i].ForeignKeyUpdated = true
					break
				}
			}
		}
	}

	sessionCount := defaultRepairIndexSessionCount
	indexSessions := make([]glue.Session, 0, sessionCount)
	defer func() {
		for _, session := range indexSessions {
			session.Close()
		}
	}()
	for range sessionCount {
		session, err := g.CreateSession(dom.Store())
		if err != nil {
			return errors.Trace(err)
		}
		indexSessions = append(indexSessions, session)
		// Restore index definitions independently of the target's SQL mode.
		if err := session.Execute(ctx, "set @@sql_mode=''"); err != nil {
			return errors.Trace(err)
		}
	}
	workerpool := tidbutil.NewWorkerPool(sessionCount, "repair ingest index")
	eg, ectx := errgroup.WithContext(ctx)
	for _, fk := range fkSqls {
		if fk.ForeignKeyUpdated {
			continue
		}
		if fromCheckpoint && !fk.OldForeignKeyFound {
			continue
		}
		workerpool.ApplyWithIDInErrorGroup(eg, func(id uint64) error {
			indexSession := indexSessions[id%uint64(len(indexSessions))]
			if err := indexSession.ExecuteInternal(ectx, alterTableDropForeignKeyFormat, fk.SchemaName, fk.TableName, fk.FKName); err != nil {
				return errors.Trace(err)
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return errors.Trace(err)
	}
	failpoint.Inject("failed-before-repair-ingest-index", func(v failpoint.Value) {
		if v != nil && v.(bool) {
			failpoint.Return(errors.New("failed before repair ingest index"))
		}
	})
	eg, ectx = errgroup.WithContext(ctx)
	mp := console.StartMultiProgress()
	for _, sql := range sqls {
		if sql.IndexRepaired {
			continue
		}
		if ectx.Err() != nil {
			break
		}
		progressTitle := fmt.Sprintf("repair ingest index %s for table %s.%s", sql.IndexName, sql.SchemaName, sql.TableName)
		w := mp.AddTextBar(progressTitle, 1)
		workerpool.ApplyWithIDInErrorGroup(eg, func(id uint64) error {
			defer w.Done()

			indexSession := indexSessions[id%uint64(len(indexSessions))]
			// TODO: When the TiDB supports the DROP and CREATE the same name index in one SQL,
			//   the checkpoint for ingest recorder can be removed and directly use the SQL:
			//      ALTER TABLE db.tbl DROP INDEX `i_1`, ADD IDNEX `i_1` ...
			//
			// This SQL is compatible with checkpoint: If one ingest index has been recreated by
			// the SQL, the index's id would be another one. In the next retry execution, BR can
			// not find the ingest index's dropped id so that BR regards it as a dropped index by
			// restored metakv and then skips repairing it.

			// only when first execution or old index id is not dropped
			if !fromCheckpoint || sql.OldIndexIDFound {
				if err := indexSession.ExecuteInternal(ectx, alterTableDropIndexSQL, sql.SchemaName.O, sql.TableName.O, sql.IndexName); err != nil {
					return errors.Trace(err)
				}
			}
			if addIndexSQLStorage != nil {
				w.Increment()
				return nil
			}
			failpoint.Inject("failed-before-create-ingest-index", func(v failpoint.Value) {
				if v != nil && v.(bool) {
					failpoint.Return(errors.New("failed before create ingest index"))
				}
			})
			// create the repaired index when first execution or not found it
			if err := indexSession.ExecuteInternal(ectx, sql.AddSQL, sql.AddArgs...); err != nil {
				return errors.Trace(err)
			}
			w.Increment()
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return errors.Trace(err)
	}
	if addIndexSQLStorage != nil {
		if err := writeRepairIngestIndexSQLFile(ctx, addIndexSQLStorage, sqls, fkSqls); err != nil {
			return errors.Trace(err)
		}
		return nil
	}
	eg, ectx = errgroup.WithContext(ctx)
	for _, fk := range fkSqls {
		if fk.ForeignKeyUpdated {
			continue
		}
		workerpool.ApplyWithIDInErrorGroup(eg, func(id uint64) error {
			indexSession := indexSessions[id%uint64(len(indexSessions))]
			if err := indexSession.ExecuteInternal(ectx, fk.AddSQL, fk.AddArgs...); err != nil {
				return errors.Trace(err)
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return errors.Trace(err)
	}
	failpoint.Inject("failed-after-repair-ingest-index", func(v failpoint.Value) {
		if v != nil && v.(bool) {
			failpoint.Return(errors.New("failed after repair ingest index"))
		}
	})

	return nil
}
