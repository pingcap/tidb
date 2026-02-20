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

package domain

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// extractTableNames extracts table names from the given stmts.
func extractTableNames(ctx context.Context, sctx sessionctx.Context,
	execStmts []ast.StmtNode, curDB ast.CIStr,
) (map[tableNamePair]struct{}, error) {
	tableExtractor := &tableNameExtractor{
		ctx:      ctx,
		executor: sctx.GetRestrictedSQLExecutor(),
		is:       GetDomain(sctx).InfoSchema(),
		curDB:    curDB,
		names:    make(map[tableNamePair]struct{}),
		cteNames: make(map[string]struct{}),
	}
	for _, execStmt := range execStmts {
		execStmt.Accept(tableExtractor)
	}
	if tableExtractor.err != nil {
		return nil, tableExtractor.err
	}
	return tableExtractor.getTablesAndViews()
}

func getStatsForTable(do *Domain, pair tableNamePair, historyStatsTS uint64) (*util.JSONTable, []string, error) {
	is := do.InfoSchema()
	h := do.StatsHandle()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr(pair.DBName), ast.NewCIStr(pair.TableName))
	if err != nil {
		return nil, nil, err
	}
	if historyStatsTS > 0 {
		return h.DumpHistoricalStatsBySnapshot(pair.DBName, tbl.Meta(), historyStatsTS)
	}
	jt, err := h.DumpStatsToJSON(pair.DBName, tbl.Meta(), nil, true)
	return jt, nil, err
}

func getShowCreateTable(pair tableNamePair, zw *zip.Writer, ctx sessionctx.Context) error {
	recordSets, err := ctx.GetSQLExecutor().Execute(context.Background(), fmt.Sprintf("show create table `%v`.`%v`", pair.DBName, pair.TableName))
	if err != nil {
		return err
	}
	sRows, err := resultSetToStringSlice(context.Background(), recordSets[0], false)
	if err != nil {
		return err
	}
	var fw io.Writer
	if pair.IsView {
		fw, err = zw.Create(fmt.Sprintf("view/%v.%v.view.txt", pair.DBName, pair.TableName))
		if err != nil {
			return errors.AddStack(err)
		}
		if len(sRows) == 0 || len(sRows[0]) != 4 {
			return fmt.Errorf("plan replayer: get create view %v.%v failed", pair.DBName, pair.TableName)
		}
	} else {
		fw, err = zw.Create(fmt.Sprintf("schema/%v.%v.schema.txt", pair.DBName, pair.TableName))
		if err != nil {
			return errors.AddStack(err)
		}
		if len(sRows) == 0 || len(sRows[0]) != 2 {
			return fmt.Errorf("plan replayer: get create table %v.%v failed", pair.DBName, pair.TableName)
		}
	}
	fmt.Fprintf(fw, "create database if not exists `%v`; use `%v`;", pair.DBName, pair.DBName)
	fmt.Fprintf(fw, "%s", sRows[0][1])
	if len(recordSets) > 0 {
		if err := recordSets[0].Close(); err != nil {
			return err
		}
	}
	return nil
}

func resultSetToStringSlice(ctx context.Context, rs sqlexec.RecordSet, emptyAsNil bool) ([][]string, error) {
	rows, err := getRows(ctx, rs)
	if err != nil {
		return nil, err
	}
	err = rs.Close()
	if err != nil {
		return nil, err
	}
	sRows := make([][]string, len(rows))
	for i, row := range rows {
		iRow := make([]string, row.Len())
		for j := range row.Len() {
			if row.IsNull(j) {
				iRow[j] = "<nil>"
			} else {
				d := row.GetDatum(j, &rs.Fields()[j].Column.FieldType)
				iRow[j], err = d.ToString()
				if err != nil {
					return nil, err
				}
				if len(iRow[j]) < 1 && emptyAsNil {
					iRow[j] = "<nil>"
				}
			}
		}
		sRows[i] = iRow
	}
	return sRows, nil
}

func getRows(ctx context.Context, rs sqlexec.RecordSet) ([]chunk.Row, error) {
	if rs == nil {
		return nil, nil
	}
	var rows []chunk.Row
	req := rs.NewChunk(nil)
	// Must reuse `req` for imitating server.(*clientConn).writeChunks
	for {
		err := rs.Next(ctx, req)
		if err != nil {
			return nil, err
		}
		if req.NumRows() == 0 {
			break
		}

		iter := chunk.NewIterator4Chunk(req.CopyConstruct())
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func dumpDebugTrace(zw *zip.Writer, debugTraces []any) error {
	if debugTraces == nil { // if no debug trace collected, we still create one empty file for compatibility
		debugTraces = append(debugTraces, nil)
	}
	for i, trace := range debugTraces {
		fw, err := zw.Create(fmt.Sprintf("debug_trace/debug_trace%d.json", i))
		if err != nil {
			return errors.AddStack(err)
		}
		err = dumpOneDebugTrace(fw, trace)
		if err != nil {
			return errors.AddStack(err)
		}
	}
	return nil
}

func dumpOneDebugTrace(w io.Writer, debugTrace any) error {
	if debugTrace == nil {
		return nil
	}
	jsonEncoder := json.NewEncoder(w)
	// If we do not set this to false, ">", "<", "&"... will be escaped to "\u003c","\u003e", "\u0026"...
	jsonEncoder.SetEscapeHTML(false)
	return jsonEncoder.Encode(debugTrace)
}

func dumpErrorMsgs(zw *zip.Writer, msgs []string) error {
	mt, err := zw.Create(PlanReplayerErrorMessageFile)
	if err != nil {
		return errors.AddStack(err)
	}
	for _, msg := range msgs {
		_, err = mt.Write([]byte(msg))
		if err != nil {
			return errors.AddStack(err)
		}
		_, err = mt.Write([]byte{'\n'})
		if err != nil {
			return errors.AddStack(err)
		}
	}
	return nil
}
