// Copyright 2024 PingCAP, Inc.
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

package workloadrepo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util/slice"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
)

func buildCreateQuery(ctx context.Context, sess sessionctx.Context, rt *repositoryTable) (string, error) {
	is := sessiontxn.GetTxnManager(sess).GetTxnInfoSchema()
	tbl, err := is.TableByName(ctx, ast.NewCIStr(rt.schema), ast.NewCIStr(rt.table))
	if err != nil {
		return "", err
	}
	if rt.tableType == metadataTable {
		return "", errors.New("buildCreateQuery invoked on metadataTable")
	}

	sb := &strings.Builder{}
	sqlescape.MustFormatSQL(sb, "CREATE TABLE IF NOT EXISTS %n.%n (", WorkloadSchema, rt.destTable)
	if rt.tableType == snapshotTable {
		fmt.Fprintf(sb, "`SNAP_ID` INT UNSIGNED NOT NULL, ")
	}
	fmt.Fprintf(sb, "`TS` DATETIME NOT NULL, ")
	fmt.Fprintf(sb, "`INSTANCE_ID` VARCHAR(64) DEFAULT NULL")

	for _, v := range tbl.Cols() {
		sqlescape.MustFormatSQL(sb, ", %n ", v.Name.O)
		fmt.Fprintf(sb, "%s COMMENT ", v.GetTypeDesc())
		sqlescape.MustFormatSQL(sb, "%? ", v.Comment)
	}
	fmt.Fprintf(sb, ") DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin ")

	return sb.String(), nil
}

func buildInsertQuery(ctx context.Context, sess sessionctx.Context, rt *repositoryTable) error {
	is := sessiontxn.GetTxnManager(sess).GetTxnInfoSchema()
	tbl, err := is.TableByName(ctx, ast.NewCIStr(rt.schema), ast.NewCIStr(rt.table))
	if err != nil {
		return err
	}
	if rt.tableType == metadataTable {
		return errors.New("buildInsertQuery invoked on metadataTable")
	}

	sb := &strings.Builder{}
	sqlescape.MustFormatSQL(sb, "INSERT %n.%n (", WorkloadSchema, rt.destTable)

	if rt.tableType == snapshotTable {
		fmt.Fprint(sb, "`SNAP_ID`, ")
	}
	fmt.Fprint(sb, "`TS`, ")
	fmt.Fprint(sb, "`INSTANCE_ID`")

	for _, v := range tbl.Cols() {
		sqlescape.MustFormatSQL(sb, ", %n", v.Name.O)
	}
	fmt.Fprint(sb, ") SELECT ")

	if rt.tableType == snapshotTable {
		fmt.Fprint(sb, "%?, now(), %?")
	} else if rt.tableType == samplingTable {
		fmt.Fprint(sb, "now(), %?")
	}

	for _, v := range tbl.Cols() {
		sqlescape.MustFormatSQL(sb, ", %n", v.Name.O)
	}
	sqlescape.MustFormatSQL(sb, " FROM %n.%n", rt.schema, rt.table)
	if rt.where != "" {
		fmt.Fprint(sb, "WHERE ", rt.where)
	}

	rt.insertStmt = sb.String()
	return nil
}

func (w *worker) createAllTables(ctx context.Context, now time.Time) error {
	_sessctx := w.getSessionWithRetry()
	sess := _sessctx.(sessionctx.Context)
	defer w.sesspool.Put(_sessctx)
	is := sess.GetDomainInfoSchema().(infoschema.InfoSchema)
	if !is.SchemaExists(workloadSchemaCIStr) {
		_, err := execRetry(ctx, sess, "create database if not exists "+WorkloadSchema)
		if err != nil {
			return err
		}
	}

	for _, tbl := range w.workloadTables {
		if checkTableExistsByIS(ctx, is, tbl.destTable, zeroTime) {
			continue
		}

		createStmt := tbl.createStmt
		if createStmt == "" {
			cs, err := buildCreateQuery(ctx, sess, &tbl)
			if err != nil {
				return err
			}
			createStmt = cs
		}

		if tbl.tableType == metadataTable {
			sb := &strings.Builder{}
			fmt.Fprint(sb, createStmt)
			generatePartitionDef(sb, "BEGIN_TIME", now)
			createStmt = sb.String()
		} else {
			sb := &strings.Builder{}
			fmt.Fprint(sb, createStmt)
			generatePartitionDef(sb, "TS", now)
			createStmt = sb.String()
		}

		if _, err := execRetry(ctx, sess, createStmt); err != nil {
			return err
		}
	}

	is = sess.GetDomainInfoSchema().(infoschema.InfoSchema)
	return w.createAllPartitions(ctx, sess, is, now)
}

func (w *worker) checkTablesExists(ctx context.Context, now time.Time) bool {
	_sessctx := w.getSessionWithRetry()
	sess := _sessctx.(sessionctx.Context)
	defer w.sesspool.Put(_sessctx)
	is := sess.GetDomainInfoSchema().(infoschema.InfoSchema)
	return slice.AllOf(w.workloadTables, func(i int) bool {
		return checkTableExistsByIS(ctx, is, w.workloadTables[i].destTable, now)
	})
}

func checkTableExistsByIS(ctx context.Context, is infoschema.InfoSchema, tblName string, now time.Time) bool {
	if now == zeroTime {
		return is.TableExists(workloadSchemaCIStr, ast.NewCIStr(tblName))
	}

	// check for partitions, too
	tbSchema, err := is.TableByName(ctx, workloadSchemaCIStr, ast.NewCIStr(tblName))
	if err != nil {
		return false
	}

	// Insure that the table has a partition for tomorrow.
	tbInfo := tbSchema.Meta()
	if tbInfo == nil {
		return false
	}
	pi := tbInfo.GetPartitionInfo()
	if pi == nil || pi.Definitions == nil || len(pi.Definitions) == 0 {
		return false
	}
	ptInfos := pi.Definitions
	ot, err := parsePartitionName(ptInfos[len(ptInfos)-1].Name.L)
	if err != nil {
		return false
	}

	// It doesn't matter if now has a timestamp.
	return ot.After(now.AddDate(0, 0, 1))
}
