// Copyright 2020 PingCAP, Inc.
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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func adjustColumns(input [][]types.Datum, outColumns []*model.ColumnInfo, table *model.TableInfo) [][]types.Datum {
	if len(outColumns) == len(table.Columns) {
		return input
	}
	rows := make([][]types.Datum, len(input))
	for i, fullRow := range input {
		row := make([]types.Datum, len(outColumns))
		for j, col := range outColumns {
			row[j] = fullRow[col.Offset]
		}
		rows[i] = row
	}
	return rows
}

// TiFlashSystemTableRetriever is used to read system table from tiflash.
type TiFlashSystemTableRetriever struct {
	dummyCloser
	table         *model.TableInfo
	outputCols    []*model.ColumnInfo
	instanceCount int
	instanceIdx   int
	instanceIDs   []string
	rowIdx        int
	retrieved     bool
	initialized   bool
	extractor     *plannercore.TiFlashSystemTableExtractor
}

func (e *TiFlashSystemTableRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.extractor.SkipRequest || e.retrieved {
		return nil, nil
	}
	if !e.initialized {
		err := e.initialize(sctx, e.extractor.TiFlashInstances)
		if err != nil {
			return nil, err
		}
	}
	if e.instanceCount == 0 || e.instanceIdx >= e.instanceCount {
		e.retrieved = true
		return nil, nil
	}

	for {
		rows, err := e.dataForTiFlashSystemTables(ctx, sctx, e.extractor.TiDBDatabases, e.extractor.TiDBTables)
		if err != nil {
			return nil, err
		}
		if len(rows) > 0 || e.instanceIdx >= e.instanceCount {
			return rows, nil
		}
	}
}

func (e *TiFlashSystemTableRetriever) initialize(sctx sessionctx.Context, tiflashInstances set.StringSet) error {
	storeInfo, err := infoschema.GetStoreServerInfo(sctx.GetStore())
	if err != nil {
		return err
	}

	for _, info := range storeInfo {
		if info.ServerType != kv.TiFlash.Name() {
			continue
		}
		info.ResolveLoopBackAddr()
		if len(tiflashInstances) > 0 && !tiflashInstances.Exist(info.Address) {
			continue
		}
		hostAndStatusPort := strings.Split(info.StatusAddr, ":")
		if len(hostAndStatusPort) != 2 {
			return errors.Errorf("node status addr: %s format illegal", info.StatusAddr)
		}
		e.instanceIDs = append(e.instanceIDs, info.Address)
		e.instanceCount++
	}
	e.initialized = true
	return nil
}

type tiFlashSQLExecuteResponseMetaColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type tiFlashSQLExecuteResponse struct {
	Meta []tiFlashSQLExecuteResponseMetaColumn `json:"meta"`
	Data [][]any                               `json:"data"`
}

var tiflashTargetTableName = map[string]string{
	"tiflash_tables":   "dt_tables",
	"tiflash_segments": "dt_segments",
	"tiflash_indexes":  "dt_local_indexes",
}

func (e *TiFlashSystemTableRetriever) dataForTiFlashSystemTables(ctx context.Context, sctx sessionctx.Context, tidbDatabases string, tidbTables string) ([][]types.Datum, error) {
	maxCount := 1024
	targetTable := tiflashTargetTableName[e.table.Name.L]

	var filters []string
	// Add filter for keyspace_id if tidb is running in the keyspace mode.
	if keyspace.GetKeyspaceNameBySettings() != "" {
		keyspaceID := uint32(sctx.GetStore().GetCodec().GetKeyspaceID())
		filters = append(filters, fmt.Sprintf("keyspace_id=%d", keyspaceID))
	}
	if len(tidbDatabases) > 0 {
		filters = append(filters, fmt.Sprintf("tidb_database IN (%s)", strings.ReplaceAll(tidbDatabases, "\"", "'")))
	}
	if len(tidbTables) > 0 {
		filters = append(filters, fmt.Sprintf("tidb_table IN (%s)", strings.ReplaceAll(tidbTables, "\"", "'")))
	}
	sql := fmt.Sprintf("SELECT * FROM system.%s", targetTable)
	if len(filters) > 0 {
		sql = fmt.Sprintf("%s WHERE %s", sql, strings.Join(filters, " AND "))
	}
	sql = fmt.Sprintf("%s LIMIT %d, %d", sql, e.rowIdx, maxCount)
	request := tikvrpc.Request{
		Type:    tikvrpc.CmdGetTiFlashSystemTable,
		StoreTp: tikvrpc.TiFlash,
		Req: &kvrpcpb.TiFlashSystemTableRequest{
			Sql: sql,
		},
	}

	store := sctx.GetStore()
	tikvStore, ok := store.(tikv.Storage)
	if !ok {
		return nil, errors.New("Get tiflash system tables can only run with tikv compatible storage")
	}
	// send request to tiflash, use 5 minutes as per-request timeout
	instanceID := e.instanceIDs[e.instanceIdx]
	timeout := time.Duration(5*60) * time.Second
	resp, err := tikvStore.GetTiKVClient().SendRequest(ctx, instanceID, &request, timeout)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var result tiFlashSQLExecuteResponse
	tiflashResp, ok := resp.Resp.(*kvrpcpb.TiFlashSystemTableResponse)
	if !ok {
		return nil, errors.Errorf("Unexpected response type: %T", resp.Resp)
	}
	err = json.Unmarshal(tiflashResp.Data, &result)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to decode JSON from TiFlash")
	}

	// Map result columns back to our columns. It is possible that some columns cannot be
	// recognized and some other columns are missing. This may happen during upgrading.
	outputColIndexMap := map[string]int{} // Map from TiDB Column name to Output Column Index
	for idx, c := range e.outputCols {
		outputColIndexMap[c.Name.L] = idx
	}
	tiflashColIndexMap := map[int]int{} // Map from TiFlash Column index to Output Column Index
	for tiFlashColIdx, col := range result.Meta {
		if outputIdx, ok := outputColIndexMap[strings.ToLower(col.Name)]; ok {
			tiflashColIndexMap[tiFlashColIdx] = outputIdx
		}
	}
	is := sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema()
	outputRows := make([][]types.Datum, 0, len(result.Data))
	for _, rowFields := range result.Data {
		if len(rowFields) == 0 {
			continue
		}
		outputRow := make([]types.Datum, len(e.outputCols))
		for tiFlashColIdx, fieldValue := range rowFields {
			outputIdx, ok := tiflashColIndexMap[tiFlashColIdx]
			if !ok {
				// Discard this field, we don't know which output column is the destination
				continue
			}
			if fieldValue == nil {
				continue
			}
			valStr := fmt.Sprint(fieldValue)
			column := e.outputCols[outputIdx]
			if column.GetType() == mysql.TypeVarchar {
				outputRow[outputIdx].SetString(valStr, mysql.DefaultCollationName)
			} else if column.GetType() == mysql.TypeLonglong {
				value, err := strconv.ParseInt(valStr, 10, 64)
				if err != nil {
					return nil, errors.Trace(err)
				}
				outputRow[outputIdx].SetInt64(value)
			} else if column.GetType() == mysql.TypeDouble {
				value, err := strconv.ParseFloat(valStr, 64)
				if err != nil {
					return nil, errors.Trace(err)
				}
				outputRow[outputIdx].SetFloat64(value)
			} else {
				return nil, errors.Errorf("Meet column of unknown type %v", column)
			}
		}
		outputRow[len(e.outputCols)-1].SetString(instanceID, mysql.DefaultCollationName)

		// for "tiflash_indexes", set the column_name and index_name according to the TableInfo
		if e.table.Name.L == "tiflash_indexes" {
			logicalTableID := outputRow[outputColIndexMap["table_id"]].GetInt64()
			if !outputRow[outputColIndexMap["belonging_table_id"]].IsNull() {
				// Old TiFlash versions may not have this column. In this case we will try to get by the "table_id"
				belongingTableID := outputRow[outputColIndexMap["belonging_table_id"]].GetInt64()
				if belongingTableID != -1 && belongingTableID != 0 {
					logicalTableID = belongingTableID
				}
			}
			if table, ok := is.TableByID(ctx, logicalTableID); ok {
				tableInfo := table.Meta()
				getInt64DatumVal := func(datum_name string, default_val int64) int64 {
					datum := outputRow[outputColIndexMap[datum_name]]
					if !datum.IsNull() {
						return datum.GetInt64()
					}
					return default_val
				}
				// set column_name
				columnID := getInt64DatumVal("column_id", 0)
				columnName := tableInfo.FindColumnNameByID(columnID)
				outputRow[outputColIndexMap["column_name"]].SetString(columnName, mysql.DefaultCollationName)
				// set index_name
				indexID := getInt64DatumVal("index_id", 0)
				indexName := tableInfo.FindIndexNameByID(indexID)
				outputRow[outputColIndexMap["index_name"]].SetString(indexName, mysql.DefaultCollationName)
			}
		}

		outputRows = append(outputRows, outputRow)
	}
	e.rowIdx += len(outputRows)
	if len(outputRows) < maxCount {
		e.instanceIdx++
		e.rowIdx = 0
	}
	return outputRows, nil
}

