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

package repository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util/slice"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

const (
	// WorkloadSchema is the name of database for repository worker.
	WorkloadSchema = "workload_schema"
)

var (
	workloadSchemaCIStr = model.NewCIStr(WorkloadSchema)
	zeroTime            = time.Time{}
)

func (w *Worker) buildCreateQuery(ctx context.Context, sess sessionctx.Context, rt *repositoryTable) (string, error) {
	is := sessiontxn.GetTxnManager(sess).GetTxnInfoSchema()
	tbl, err := is.TableByName(ctx, model.NewCIStr(rt.schema), model.NewCIStr(rt.table))
	if err != nil {
		return "", err
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

func (w *Worker) createAllTables(ctx context.Context) error {
	_sessctx := w.getSessionWithRetry()
	exec := _sessctx.(sqlexec.SQLExecutor)
	sess := _sessctx.(sessionctx.Context)
	defer w.sesspool.Put(_sessctx)
	is := sess.GetDomainInfoSchema().(infoschema.InfoSchema)
	if !is.SchemaExists(workloadSchemaCIStr) {
		_, err := exec.ExecuteInternal(ctx, "create database if not exists "+WorkloadSchema)
		if err != nil {
			return err
		}
	}

	if _, err := exec.ExecuteInternal(ctx, "use "+WorkloadSchema); err != nil {
		return err
	}

	for _, tbl := range workloadTables {
		if w.checkTableExistsByIS(ctx, is, tbl.destTable, zeroTime) {
			continue
		}

		createStmt := tbl.createStmt
		if createStmt == "" {
			cs, err := w.buildCreateQuery(ctx, sess, &tbl)
			if err != nil {
				return err
			}
			createStmt = cs
		}

		if tbl.tableType == metadataTable {
			sb := &strings.Builder{}
			fmt.Fprint(sb, createStmt)
			generatePartitionDef(sb, "BEGIN_TIME")
			createStmt = sb.String()
		} else {
			sb := &strings.Builder{}
			fmt.Fprint(sb, createStmt)
			generatePartitionDef(sb, "TS")
			createStmt = sb.String()
		}

		if err := execRetry(ctx, exec, createStmt); err != nil {
			return err
		}
	}

	return w.createAllPartitions(ctx, exec, is)
}

func (w *Worker) checkTablesExists(ctx context.Context) bool {
	_sessctx := w.getSessionWithRetry()
	sess := _sessctx.(sessionctx.Context)
	defer w.sesspool.Put(_sessctx)
	is := sess.GetDomainInfoSchema().(infoschema.InfoSchema)
	now := time.Now()
	return slice.AllOf(workloadTables, func(i int) bool {
		return w.checkTableExistsByIS(ctx, is, workloadTables[i].destTable, now)
	})
}

func (w *Worker) checkTableExistsByIS(ctx context.Context, is infoschema.InfoSchema, tblName string, now time.Time) bool {
	if now == zeroTime {
		return is.TableExists(workloadSchemaCIStr, model.NewCIStr(tblName))
	}

	// check for partitions, too
	tbSchema, err := is.TableByName(ctx, workloadSchemaCIStr, model.NewCIStr(tblName))
	if err != nil {
		return false
	}

	tbInfo := tbSchema.Meta()
	for i := 0; i < 2; i++ {
		newPtTime := now.AddDate(0, 0, i+1)
		newPtName := "p" + newPtTime.Format("20060102")
		ptInfos := tbInfo.GetPartitionInfo().Definitions
		if slice.NoneOf(ptInfos, func(i int) bool {
			return ptInfos[i].Name.L == newPtName
		}) {
			return false
		}
	}
	return true
}
