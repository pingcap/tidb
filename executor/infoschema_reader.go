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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

type memtableRetriever struct {
	dummyCloser
	table       *model.TableInfo
	columns     []*model.ColumnInfo
	rows        [][]types.Datum
	rowIdx      int
	retrieved   bool
	initialized bool
}

// retrieve implements the infoschemaRetriever interface
func (e *memtableRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.retrieved {
		return nil, nil
	}

	//Cache the ret full rows in schemataRetriever
	if !e.initialized {
		is := infoschema.GetInfoSchema(sctx)
		dbs := is.AllSchemas()
		sort.Sort(infoschema.SchemasSorter(dbs))
		var err error
		switch e.table.Name.O {
		case infoschema.TableSchemata:
			e.rows = dataForSchemata(sctx, dbs)
		case infoschema.TableViews:
			e.rows, err = dataForViews(sctx, dbs)
		}
		if err != nil {
			return nil, err
		}
		e.initialized = true
	}

	//Adjust the amount of each return
	maxCount := 1024
	retCount := maxCount
	if e.rowIdx+maxCount > len(e.rows) {
		retCount = len(e.rows) - e.rowIdx
		e.retrieved = true
	}
	ret := make([][]types.Datum, retCount)
	for i := e.rowIdx; i < e.rowIdx+retCount; i++ {
		ret[i-e.rowIdx] = e.rows[i]
	}
	e.rowIdx += retCount
	if len(e.columns) == len(e.table.Columns) {
		return ret, nil
	}
	rows := make([][]types.Datum, len(ret))
	for i, fullRow := range ret {
		row := make([]types.Datum, len(e.columns))
		for j, col := range e.columns {
			row[j] = fullRow[col.Offset]
		}
		rows[i] = row
	}
	return rows, nil
}

func dataForSchemata(ctx sessionctx.Context, schemas []*model.DBInfo) [][]types.Datum {
	checker := privilege.GetPrivilegeManager(ctx)
	rows := make([][]types.Datum, 0, len(schemas))

	for _, schema := range schemas {

		charset := mysql.DefaultCharset
		collation := mysql.DefaultCollationName

		if len(schema.Charset) > 0 {
			charset = schema.Charset // Overwrite default
		}

		if len(schema.Collate) > 0 {
			collation = schema.Collate // Overwrite default
		}

		if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, "", "", mysql.AllPrivMask) {
			continue
		}
		record := types.MakeDatums(
			infoschema.CatalogVal, // CATALOG_NAME
			schema.Name.O,         // SCHEMA_NAME
			charset,               // DEFAULT_CHARACTER_SET_NAME
			collation,             // DEFAULT_COLLATION_NAME
			nil,
		)
		rows = append(rows, record)
	}
	return rows
}

func dataForViews(ctx sessionctx.Context, schemas []*model.DBInfo) ([][]types.Datum, error) {
	checker := privilege.GetPrivilegeManager(ctx)
	var rows [][]types.Datum
	for _, schema := range schemas {
		for _, table := range schema.Tables {
			if !table.IsView() {
				continue
			}
			collation := table.Collate
			charset := table.Charset
			if collation == "" {
				collation = mysql.DefaultCollationName
			}
			if charset == "" {
				charset = mysql.DefaultCharset
			}
			if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, table.Name.L, "", mysql.AllPrivMask) {
				continue
			}
			record := types.MakeDatums(
				infoschema.CatalogVal,           // TABLE_CATALOG
				schema.Name.O,                   // TABLE_SCHEMA
				table.Name.O,                    // TABLE_NAME
				table.View.SelectStmt,           // VIEW_DEFINITION
				table.View.CheckOption.String(), // CHECK_OPTION
				"NO",                            // IS_UPDATABLE
				table.View.Definer.String(),     // DEFINER
				table.View.Security.String(),    // SECURITY_TYPE
				charset,                         // CHARACTER_SET_CLIENT
				collation,                       // COLLATION_CONNECTION
			)
			rows = append(rows, record)
		}
	}
	return rows, nil
}

//slowQueryRetriever is used to read slow log data.
type diskUsageRetriever struct {
	dummyCloser
	table         *model.TableInfo
	outputCols    []*model.ColumnInfo
	retrieved     bool
	initialized   bool
	extractor     *plannercore.DiskUsageExtractor
	initialTables []*initialTable
	curTable      int
}

func (e *diskUsageRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.retrieved {
		return nil, nil
	}
	if !e.initialized {
		err := e.initialize(sctx)
		if err != nil {
			return nil, err
		}
	}
	if len(e.initialTables) == 0 || e.curTable >= len(e.initialTables) {
		e.retrieved = true
		return nil, nil
	}

	rows, err := e.dataForDiskUsage(sctx)
	if err != nil {
		return nil, err
	}
	if len(e.outputCols) == len(e.table.Columns) {
		return rows, nil
	}
	retRows := make([][]types.Datum, len(rows))
	for i, fullRow := range rows {
		row := make([]types.Datum, len(e.outputCols))
		for j, col := range e.outputCols {
			row[j] = fullRow[col.Offset]
		}
		retRows[i] = row
	}
	return retRows, nil
}

type initialTable struct {
	db string
	tb *model.TableInfo
}

func (e *diskUsageRetriever) initialize(sctx sessionctx.Context) error {
	is := infoschema.GetInfoSchema(sctx)
	dbs := is.AllSchemas()
	sort.Sort(infoschema.SchemasSorter(dbs))
	schemas := e.extractor.TableSchema
	tables := e.extractor.TableName
	//var initialTables []initialTable
	if len(schemas) != 0 {
		for _, schema := range dbs {
			if schemas.Exist(schema.Name.L) {
				if len(tables) != 0 {
					for _, table := range schema.Tables {
						if tables.Exist(table.Name.L) {
							e.initialTables = append(e.initialTables, &initialTable{db: schema.Name.L, tb: table})
						}
					}
				} else {
					for _, table := range schema.Tables {
						e.initialTables = append(e.initialTables, &initialTable{db: schema.Name.L, tb: table})
					}
				}
			}

		}
		if len(e.initialTables) == 0 && tables == nil {
			return errors.Errorf("schema or table not exist, please check the schema and table")
		}
	} else {
		if len(tables) != 0 {
			for _, schema := range dbs {
				for _, table := range schema.Tables {
					if tables.Exist(table.Name.L) {
						e.initialTables = append(e.initialTables, &initialTable{db: schema.Name.L, tb: table})
					}
				}
			}
		} else {
			for _, schema := range dbs {
				for _, table := range schema.Tables {
					e.initialTables = append(e.initialTables, &initialTable{schema.Name.O, table})
				}
			}
		}
	}
	e.initialized = true
	return nil
}

// pdRegionStats is the json response from PD.
type pdRegionStats struct {
	Count            int              `json:"count"`
	EmptyCount       int              `json:"empty_count"`
	StorageSize      int64            `json:"storage_size"`
	StoreLeaderCount map[uint64]int   `json:"store_leader_count"`
	StorePeerCount   map[uint64]int   `json:"store_peer_count"`
	StoreLeaderSize  map[uint64]int64 `json:"store_leader_size"`
	StorePeerSize    map[uint64]int64 `json:"store_peer_size"`
}

func (e *diskUsageRetriever) dataForDiskUsage(ctx sessionctx.Context) ([][]types.Datum, error) {
	rows := make([][]types.Datum, 0, 1024)
	tikvStore, ok := ctx.GetStore().(tikv.Storage)
	if !ok {
		return nil, errors.New("Information about TiKV region status can be gotten only when the storage is TiKV")
	}
	var pdAddrs []string
	etcd, ok := tikvStore.(tikv.EtcdBackend)
	if !ok {
		return nil, errors.New("not implemented")
	}
	pdAddrs = etcd.EtcdAddrs()
	if len(pdAddrs) < 0 {
		return nil, errors.New("pd unavailable")
	}
	count := 0
	for i := e.curTable; e.curTable < len(e.initialTables) && count < 1024; i++ {
		table := (e.initialTables)[e.curTable]
		tableID := table.tb.ID
		// Include table and index data, because their range located in tableID_i tableID_r
		startKey := tablecodec.EncodeTablePrefix(tableID)
		endKey := tablecodec.EncodeTablePrefix(tableID + 1)
		startKey = codec.EncodeBytes([]byte{}, startKey)
		endKey = codec.EncodeBytes([]byte{}, endKey)

		statURL := fmt.Sprintf("http://%s/pd/api/v1/stats/region?start_key=%s&end_key=%s",
			pdAddrs[0],
			url.QueryEscape(string(startKey)),
			url.QueryEscape(string(endKey)))

		resp, err := http.Get(statURL)
		if err != nil {
			return nil, err
		}
		var stats pdRegionStats
		dec := json.NewDecoder(resp.Body)
		if err := dec.Decode(&stats); err != nil {
			return nil, err
		}
		record := types.MakeDatums(
			table.db,          // TABLE_SCHEMA
			table.tb.Name.O,   // TABLE_NAME
			tableID,           // TABLE_ID
			stats.StorageSize, //DISK_USAGE
		)
		rows = append(rows, record)
		count++
		e.curTable++
	}
	return rows, nil
}
