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
	"fmt"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/pdapi"
	"github.com/pingcap/tidb/util/set"
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
		case infoschema.TableStatistics:
			e.rows = dataForStatistics(sctx, dbs)
		case infoschema.TableTiDBIndexes:
			e.rows, err = dataForIndexes(sctx, dbs)
		case infoschema.TableViews:
			e.rows, err = dataForViews(sctx, dbs)
		case infoschema.TableEngines:
			e.rows = dataForEngines()
		case infoschema.TableCharacterSets:
			e.rows = dataForCharacterSets()
		case infoschema.TableCollations:
			e.rows = dataForCollations()
		case infoschema.TableKeyColumn:
			e.rows = dataForKeyColumnUsage(sctx, dbs)
		case infoschema.TableCollationCharacterSetApplicability:
			e.rows = dataForCollationCharacterSetApplicability()
		case infoschema.TableUserPrivileges:
			e.setDataFromUserPrivileges(sctx)
		case infoschema.TableTiKVRegionPeers:
			e.rows, err = dataForTikVRegionPeers(sctx)
		case infoschema.TableTiDBHotRegions:
			e.rows, err = dataForTiDBHotRegions(sctx)
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

func dataForStatistics(ctx sessionctx.Context, schemas []*model.DBInfo) [][]types.Datum {
	checker := privilege.GetPrivilegeManager(ctx)
	var rows [][]types.Datum
	for _, schema := range schemas {
		for _, table := range schema.Tables {
			if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, table.Name.L, "", mysql.AllPrivMask) {
				continue
			}

			rs := dataForStatisticsInTable(schema, table)
			rows = append(rows, rs...)
		}
	}
	return rows
}

func dataForStatisticsInTable(schema *model.DBInfo, table *model.TableInfo) [][]types.Datum {
	var rows [][]types.Datum
	if table.PKIsHandle {
		for _, col := range table.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				record := types.MakeDatums(
					infoschema.CatalogVal, // TABLE_CATALOG
					schema.Name.O,         // TABLE_SCHEMA
					table.Name.O,          // TABLE_NAME
					"0",                   // NON_UNIQUE
					schema.Name.O,         // INDEX_SCHEMA
					"PRIMARY",             // INDEX_NAME
					1,                     // SEQ_IN_INDEX
					col.Name.O,            // COLUMN_NAME
					"A",                   // COLLATION
					0,                     // CARDINALITY
					nil,                   // SUB_PART
					nil,                   // PACKED
					"",                    // NULLABLE
					"BTREE",               // INDEX_TYPE
					"",                    // COMMENT
					"NULL",                // Expression
					"",                    // INDEX_COMMENT
				)
				rows = append(rows, record)
			}
		}
	}
	nameToCol := make(map[string]*model.ColumnInfo, len(table.Columns))
	for _, c := range table.Columns {
		nameToCol[c.Name.L] = c
	}
	for _, index := range table.Indices {
		nonUnique := "1"
		if index.Unique {
			nonUnique = "0"
		}
		for i, key := range index.Columns {
			col := nameToCol[key.Name.L]
			nullable := "YES"
			if mysql.HasNotNullFlag(col.Flag) {
				nullable = ""
			}
			colName := col.Name.O
			expression := "NULL"
			tblCol := table.Columns[col.Offset]
			if tblCol.Hidden {
				colName = "NULL"
				expression = fmt.Sprintf("(%s)", tblCol.GeneratedExprString)
			}
			record := types.MakeDatums(
				infoschema.CatalogVal, // TABLE_CATALOG
				schema.Name.O,         // TABLE_SCHEMA
				table.Name.O,          // TABLE_NAME
				nonUnique,             // NON_UNIQUE
				schema.Name.O,         // INDEX_SCHEMA
				index.Name.O,          // INDEX_NAME
				i+1,                   // SEQ_IN_INDEX
				colName,               // COLUMN_NAME
				"A",                   // COLLATION
				0,                     // CARDINALITY
				nil,                   // SUB_PART
				nil,                   // PACKED
				nullable,              // NULLABLE
				"BTREE",               // INDEX_TYPE
				"",                    // COMMENT
				expression,            // Expression
				"",                    // INDEX_COMMENT
			)
			rows = append(rows, record)
		}
	}
	return rows
}

func dataForIndexes(ctx sessionctx.Context, schemas []*model.DBInfo) ([][]types.Datum, error) {
	checker := privilege.GetPrivilegeManager(ctx)
	var rows [][]types.Datum
	for _, schema := range schemas {
		for _, tb := range schema.Tables {
			if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, tb.Name.L, "", mysql.AllPrivMask) {
				continue
			}

			if tb.PKIsHandle {
				var pkCol *model.ColumnInfo
				for _, col := range tb.Cols() {
					if mysql.HasPriKeyFlag(col.Flag) {
						pkCol = col
						break
					}
				}
				record := types.MakeDatums(
					schema.Name.O, // TABLE_SCHEMA
					tb.Name.O,     // TABLE_NAME
					0,             // NON_UNIQUE
					"PRIMARY",     // KEY_NAME
					1,             // SEQ_IN_INDEX
					pkCol.Name.O,  // COLUMN_NAME
					nil,           // SUB_PART
					"",            // INDEX_COMMENT
					"NULL",        // Expression
					0,             // INDEX_ID
				)
				rows = append(rows, record)
			}
			for _, idxInfo := range tb.Indices {
				if idxInfo.State != model.StatePublic {
					continue
				}
				for i, col := range idxInfo.Columns {
					nonUniq := 1
					if idxInfo.Unique {
						nonUniq = 0
					}
					var subPart interface{}
					if col.Length != types.UnspecifiedLength {
						subPart = col.Length
					}
					colName := col.Name.O
					expression := "NULL"
					tblCol := tb.Columns[col.Offset]
					if tblCol.Hidden {
						colName = "NULL"
						expression = fmt.Sprintf("(%s)", tblCol.GeneratedExprString)
					}
					record := types.MakeDatums(
						schema.Name.O,   // TABLE_SCHEMA
						tb.Name.O,       // TABLE_NAME
						nonUniq,         // NON_UNIQUE
						idxInfo.Name.O,  // KEY_NAME
						i+1,             // SEQ_IN_INDEX
						colName,         // COLUMN_NAME
						subPart,         // SUB_PART
						idxInfo.Comment, // INDEX_COMMENT
						expression,      // Expression
						idxInfo.ID,      // INDEX_ID
					)
					rows = append(rows, record)
				}
			}
		}
	}
	return rows, nil
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

func dataForEngines() (rows [][]types.Datum) {
	rows = append(rows,
		types.MakeDatums(
			"InnoDB",  // Engine
			"DEFAULT", // Support
			"Supports transactions, row-level locking, and foreign keys", // Comment
			"YES", // Transactions
			"YES", // XA
			"YES", // Savepoints
		),
	)
	return rows
}

func dataForCharacterSets() (records [][]types.Datum) {
	charsets := charset.GetSupportedCharsets()
	for _, charset := range charsets {
		records = append(records,
			types.MakeDatums(charset.Name, charset.DefaultCollation, charset.Desc, charset.Maxlen),
		)
	}
	return records
}

func dataForCollations() (records [][]types.Datum) {
	collations := charset.GetSupportedCollations()
	for _, collation := range collations {
		isDefault := ""
		if collation.IsDefault {
			isDefault = "Yes"
		}
		records = append(records,
			types.MakeDatums(collation.Name, collation.CharsetName, collation.ID, isDefault, "Yes", 1),
		)
	}
	return records
}

func dataForCollationCharacterSetApplicability() (records [][]types.Datum) {
	collations := charset.GetSupportedCollations()
	for _, collation := range collations {
		records = append(records,
			types.MakeDatums(collation.Name, collation.CharsetName),
		)
	}
	return records
}

func dataForKeyColumnUsage(ctx sessionctx.Context, schemas []*model.DBInfo) [][]types.Datum {
	checker := privilege.GetPrivilegeManager(ctx)
	rows := make([][]types.Datum, 0, len(schemas)) // The capacity is not accurate, but it is not a big problem.
	for _, schema := range schemas {
		for _, table := range schema.Tables {
			if checker != nil && !checker.RequestVerification(ctx.GetSessionVars().ActiveRoles, schema.Name.L, table.Name.L, "", mysql.AllPrivMask) {
				continue
			}
			rs := keyColumnUsageInTable(schema, table)
			rows = append(rows, rs...)
		}
	}
	return rows
}

func keyColumnUsageInTable(schema *model.DBInfo, table *model.TableInfo) [][]types.Datum {
	var rows [][]types.Datum
	if table.PKIsHandle {
		for _, col := range table.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				record := types.MakeDatums(
					infoschema.CatalogVal,        // CONSTRAINT_CATALOG
					schema.Name.O,                // CONSTRAINT_SCHEMA
					infoschema.PrimaryConstraint, // CONSTRAINT_NAME
					infoschema.CatalogVal,        // TABLE_CATALOG
					schema.Name.O,                // TABLE_SCHEMA
					table.Name.O,                 // TABLE_NAME
					col.Name.O,                   // COLUMN_NAME
					1,                            // ORDINAL_POSITION
					1,                            // POSITION_IN_UNIQUE_CONSTRAINT
					nil,                          // REFERENCED_TABLE_SCHEMA
					nil,                          // REFERENCED_TABLE_NAME
					nil,                          // REFERENCED_COLUMN_NAME
				)
				rows = append(rows, record)
				break
			}
		}
	}
	nameToCol := make(map[string]*model.ColumnInfo, len(table.Columns))
	for _, c := range table.Columns {
		nameToCol[c.Name.L] = c
	}
	for _, index := range table.Indices {
		var idxName string
		if index.Primary {
			idxName = infoschema.PrimaryConstraint
		} else if index.Unique {
			idxName = index.Name.O
		} else {
			// Only handle unique/primary key
			continue
		}
		for i, key := range index.Columns {
			col := nameToCol[key.Name.L]
			record := types.MakeDatums(
				infoschema.CatalogVal, // CONSTRAINT_CATALOG
				schema.Name.O,         // CONSTRAINT_SCHEMA
				idxName,               // CONSTRAINT_NAME
				infoschema.CatalogVal, // TABLE_CATALOG
				schema.Name.O,         // TABLE_SCHEMA
				table.Name.O,          // TABLE_NAME
				col.Name.O,            // COLUMN_NAME
				i+1,                   // ORDINAL_POSITION,
				nil,                   // POSITION_IN_UNIQUE_CONSTRAINT
				nil,                   // REFERENCED_TABLE_SCHEMA
				nil,                   // REFERENCED_TABLE_NAME
				nil,                   // REFERENCED_COLUMN_NAME
			)
			rows = append(rows, record)
		}
	}
	for _, fk := range table.ForeignKeys {
		fkRefCol := ""
		if len(fk.RefCols) > 0 {
			fkRefCol = fk.RefCols[0].O
		}
		for i, key := range fk.Cols {
			col := nameToCol[key.L]
			record := types.MakeDatums(
				infoschema.CatalogVal, // CONSTRAINT_CATALOG
				schema.Name.O,         // CONSTRAINT_SCHEMA
				fk.Name.O,             // CONSTRAINT_NAME
				infoschema.CatalogVal, // TABLE_CATALOG
				schema.Name.O,         // TABLE_SCHEMA
				table.Name.O,          // TABLE_NAME
				col.Name.O,            // COLUMN_NAME
				i+1,                   // ORDINAL_POSITION,
				1,                     // POSITION_IN_UNIQUE_CONSTRAINT
				schema.Name.O,         // REFERENCED_TABLE_SCHEMA
				fk.RefTable.O,         // REFERENCED_TABLE_NAME
				fkRefCol,              // REFERENCED_COLUMN_NAME
			)
			rows = append(rows, record)
		}
	}
	return rows
}

func dataForTikVRegionPeers(ctx sessionctx.Context) (records [][]types.Datum, err error) {
	tikvStore, ok := ctx.GetStore().(tikv.Storage)
	if !ok {
		return nil, errors.New("Information about TiKV region status can be gotten only when the storage is TiKV")
	}
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}
	regionsInfo, err := tikvHelper.GetRegionsInfo()
	if err != nil {
		return nil, err
	}
	for _, region := range regionsInfo.Regions {
		rows := newTiKVRegionPeersCols(&region)
		records = append(records, rows...)
	}
	return records, nil
}

func newTiKVRegionPeersCols(region *helper.RegionInfo) [][]types.Datum {
	records := make([][]types.Datum, 0, len(region.Peers))
	pendingPeerIDSet := set.NewInt64Set()
	for _, peer := range region.PendingPeers {
		pendingPeerIDSet.Insert(peer.ID)
	}
	downPeerMap := make(map[int64]int64, len(region.DownPeers))
	for _, peerStat := range region.DownPeers {
		downPeerMap[peerStat.ID] = peerStat.DownSec
	}
	for _, peer := range region.Peers {
		row := make([]types.Datum, len(infoschema.TableTiKVRegionPeersCols))
		row[0].SetInt64(region.ID)
		row[1].SetInt64(peer.ID)
		row[2].SetInt64(peer.StoreID)
		if peer.IsLearner {
			row[3].SetInt64(1)
		} else {
			row[3].SetInt64(0)
		}
		if peer.ID == region.Leader.ID {
			row[4].SetInt64(1)
		} else {
			row[4].SetInt64(0)
		}
		if pendingPeerIDSet.Exist(peer.ID) {
			row[5].SetString(pendingPeer, mysql.DefaultCollationName)
		} else if downSec, ok := downPeerMap[peer.ID]; ok {
			row[5].SetString(downPeer, mysql.DefaultCollationName)
			row[6].SetInt64(downSec)
		} else {
			row[5].SetString(normalPeer, mysql.DefaultCollationName)
		}
		records = append(records, row)
	}
	return records
}

const (
	normalPeer  = "NORMAL"
	pendingPeer = "PENDING"
	downPeer    = "DOWN"
)

func dataForTiDBHotRegions(ctx sessionctx.Context) (records [][]types.Datum, err error) {
	tikvStore, ok := ctx.GetStore().(tikv.Storage)
	if !ok {
		return nil, errors.New("Information about hot region can be gotten only when the storage is TiKV")
	}
	allSchemas := ctx.GetSessionVars().TxnCtx.InfoSchema.(infoschema.InfoSchema).AllSchemas()
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}
	metrics, err := tikvHelper.ScrapeHotInfo(pdapi.HotRead, allSchemas)
	if err != nil {
		return nil, err
	}
	records = append(records, dataForHotRegionByMetrics(metrics, "read")...)
	metrics, err = tikvHelper.ScrapeHotInfo(pdapi.HotWrite, allSchemas)
	if err != nil {
		return nil, err
	}
	records = append(records, dataForHotRegionByMetrics(metrics, "write")...)
	return records, nil
}

func dataForHotRegionByMetrics(metrics []helper.HotTableIndex, tp string) [][]types.Datum {
	rows := make([][]types.Datum, 0, len(metrics))
	for _, tblIndex := range metrics {
		row := make([]types.Datum, len(infoschema.TableTiDBHotRegionsCols))
		if tblIndex.IndexName != "" {
			row[1].SetInt64(tblIndex.IndexID)
			row[4].SetString(tblIndex.IndexName, mysql.DefaultCollationName)
		} else {
			row[1].SetNull()
			row[4].SetNull()
		}
		row[0].SetInt64(tblIndex.TableID)
		row[2].SetString(tblIndex.DbName, mysql.DefaultCollationName)
		row[3].SetString(tblIndex.TableName, mysql.DefaultCollationName)
		row[5].SetUint64(tblIndex.RegionID)
		row[6].SetString(tp, mysql.DefaultCollationName)
		if tblIndex.RegionMetric == nil {
			row[7].SetNull()
			row[8].SetNull()
		} else {
			row[7].SetInt64(int64(tblIndex.RegionMetric.MaxHotDegree))
			row[8].SetInt64(int64(tblIndex.RegionMetric.Count))
		}
		row[9].SetUint64(tblIndex.RegionMetric.FlowBytes)
		rows = append(rows, row)
	}
	return rows
}

func (e *memtableRetriever) setDataFromUserPrivileges(ctx sessionctx.Context) {
	pm := privilege.GetPrivilegeManager(ctx)
	e.rows = pm.UserPrivilegesTable()
}
