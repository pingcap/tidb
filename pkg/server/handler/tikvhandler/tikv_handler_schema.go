// Copyright 2018 PingCAP, Inc.
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

package tikvhandler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/tikv/pd/client/clients/router"
	pd "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/client/opt"
	"go.uber.org/zap"
)

// SchemaTableStorage is the schema table storage info.
type SchemaTableStorage struct {
	TableSchema   string `json:"table_schema"`
	TableName     string `json:"table_name"`
	TableRows     int64  `json:"table_rows"`
	AvgRowLength  int64  `json:"avg_row_length"`
	DataLength    int64  `json:"data_length"`
	MaxDataLength int64  `json:"max_data_length"`
	IndexLength   int64  `json:"index_length"`
	DataFree      int64  `json:"data_free"`
}

func getSchemaTablesStorageInfo(h *SchemaStorageHandler, schema *ast.CIStr, table *ast.CIStr) (messages []*SchemaTableStorage, err error) {
	var s sessionapi.Session
	if s, err = session.CreateSession(h.Store); err != nil {
		return
	}
	defer s.Close()

	sctx := s.(sessionctx.Context)
	condition := make([]string, 0)
	params := make([]any, 0)

	if schema != nil {
		condition = append(condition, `TABLE_SCHEMA = %?`)
		params = append(params, schema.O)
	}
	if table != nil {
		condition = append(condition, `TABLE_NAME = %?`)
		params = append(params, table.O)
	}

	sql := `select TABLE_SCHEMA,TABLE_NAME,TABLE_ROWS,AVG_ROW_LENGTH,DATA_LENGTH,MAX_DATA_LENGTH,INDEX_LENGTH,DATA_FREE from INFORMATION_SCHEMA.TABLES`
	if len(condition) > 0 {
		//nolint: gosec
		sql += ` WHERE ` + strings.Join(condition, ` AND `)
	}
	var results sqlexec.RecordSet
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	if results, err = sctx.GetSQLExecutor().ExecuteInternal(ctx, sql, params...); err != nil {
		logutil.BgLogger().Error(`ExecuteInternal`, zap.Error(err))
	} else if results != nil {
		messages = make([]*SchemaTableStorage, 0)
		defer terror.Call(results.Close)
		for {
			req := results.NewChunk(nil)
			if err = results.Next(ctx, req); err != nil {
				break
			}

			if req.NumRows() == 0 {
				break
			}

			for i := range req.NumRows() {
				messages = append(messages, &SchemaTableStorage{
					TableSchema:   req.GetRow(i).GetString(0),
					TableName:     req.GetRow(i).GetString(1),
					TableRows:     req.GetRow(i).GetInt64(2),
					AvgRowLength:  req.GetRow(i).GetInt64(3),
					DataLength:    req.GetRow(i).GetInt64(4),
					MaxDataLength: req.GetRow(i).GetInt64(5),
					IndexLength:   req.GetRow(i).GetInt64(6),
					DataFree:      req.GetRow(i).GetInt64(7),
				})
			}
		}
	}
	return
}

// ServeHTTP handles request of list a database or table's schemas.
func (h SchemaStorageHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	schema, err := h.Schema()
	if err != nil {
		handler.WriteError(w, err)
		return
	}

	// parse params
	params := mux.Vars(req)

	var (
		dbName    *ast.CIStr
		tableName *ast.CIStr
		isSingle  bool
	)

	if reqDbName, ok := params[handler.DBName]; ok {
		cDBName := ast.NewCIStr(reqDbName)
		// all table schemas in a specified database
		schemaInfo, exists := schema.SchemaByName(cDBName)
		if !exists {
			handler.WriteError(w, infoschema.ErrDatabaseNotExists.GenWithStackByArgs(reqDbName))
			return
		}
		dbName = &schemaInfo.Name

		if reqTableName, ok := params[handler.TableName]; ok {
			// table schema of a specified table name
			cTableName := ast.NewCIStr(reqTableName)
			data, e := schema.TableByName(context.Background(), cDBName, cTableName)
			if e != nil {
				handler.WriteError(w, e)
				return
			}
			tableName = &data.Meta().Name
			isSingle = true
		}
	}

	if results, e := getSchemaTablesStorageInfo(&h, dbName, tableName); e != nil {
		handler.WriteError(w, e)
	} else {
		if isSingle {
			handler.WriteData(w, results[0])
		} else {
			handler.WriteData(w, results)
		}
	}
}

// WriteDBTablesData writes all the table data in a database. The format is the
// marshal result of []*model.TableInfo, you can unmarshal it to
// []*model.TableInfo.
//
// Note: It would return StatusOK even if errors occur. But if errors occur,
// there must be some bugs.
func WriteDBTablesData(w http.ResponseWriter, tbs []*model.TableInfo) {
	a := make([]any, 0, len(tbs))
	for _, tb := range tbs {
		a = append(a, tb)
	}
	manualWriteJSONArray(w, a)
}

// manualWriteJSONArray manually construct the marshal result so that the memory
// can be deallocated quickly. For every item in the input, we marshal them. The
// result such as {tb1} {tb2} {tb3}. Then we add some bytes to make it become
// [{tb1}, {tb2}, {tb3}] to build a valid JSON array.
func manualWriteJSONArray(w http.ResponseWriter, array []any) {
	if len(array) == 0 {
		handler.WriteData(w, []*model.TableInfo{})
		return
	}
	w.Header().Set(handler.HeaderContentType, handler.ContentTypeJSON)
	// We assume that marshal is always OK.
	w.WriteHeader(http.StatusOK)
	_, err := w.Write(hack.Slice("[\n"))
	if err != nil {
		terror.Log(errors.Trace(err))
		return
	}
	init := false
	for _, item := range array {
		if init {
			_, err = w.Write(hack.Slice(",\n"))
			if err != nil {
				terror.Log(errors.Trace(err))
				return
			}
		} else {
			init = true
		}
		js, err := json.MarshalIndent(item, "", " ")
		if err != nil {
			terror.Log(errors.Trace(err))
			return
		}
		_, err = w.Write(js)
		if err != nil {
			terror.Log(errors.Trace(err))
			return
		}
	}
	_, err = w.Write(hack.Slice("\n]"))
	terror.Log(errors.Trace(err))
}

func writeDBSimpleTablesData(w http.ResponseWriter, tbs []*model.TableNameInfo) {
	a := make([]any, 0, len(tbs))
	for _, tb := range tbs {
		a = append(a, tb)
	}
	manualWriteJSONArray(w, a)
}

// ServeHTTP handles request of list a database or table's schemas.
func (h SchemaHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	schema, err := h.Schema()
	if err != nil {
		handler.WriteError(w, err)
		return
	}

	// parse params
	params := mux.Vars(req)

	if dbName, ok := params[handler.DBName]; ok {
		cDBName := ast.NewCIStr(dbName)
		if tableName, ok := params[handler.TableName]; ok {
			// table schema of a specified table name
			cTableName := ast.NewCIStr(tableName)
			data, err := schema.TableByName(context.Background(), cDBName, cTableName)
			if err != nil {
				handler.WriteError(w, err)
				return
			}
			handler.WriteData(w, data.Meta())
			return
		}
		// all table schemas in a specified database
		if schema.SchemaExists(cDBName) {
			if a := req.FormValue(handler.IDNameOnly); a == "true" {
				tbs, err := schema.SchemaSimpleTableInfos(context.Background(), cDBName)
				if err != nil {
					handler.WriteError(w, err)
					return
				}
				writeDBSimpleTablesData(w, tbs)
				return
			}
			tbs, err := schema.SchemaTableInfos(context.Background(), cDBName)
			if err != nil {
				handler.WriteError(w, err)
				return
			}
			WriteDBTablesData(w, tbs)
			return
		}
		handler.WriteError(w, infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName))
		return
	}

	if tableID := req.FormValue(handler.TableIDQuery); len(tableID) > 0 {
		// table schema of a specified tableID
		data, err := getTableByIDStr(schema, tableID)
		if err != nil {
			handler.WriteError(w, err)
			return
		}
		handler.WriteData(w, data)
		return
	}

	if tableIDsStr := req.FormValue(handler.TableIDsQuery); len(tableIDsStr) > 0 {
		tableIDs := strings.Split(tableIDsStr, ",")
		data := make(map[int64]*model.TableInfo, len(tableIDs))
		for _, tableID := range tableIDs {
			tbl, err := getTableByIDStr(schema, tableID)
			if err == nil {
				data[tbl.ID] = tbl
			}
		}
		if len(data) > 0 {
			handler.WriteData(w, data)
		} else {
			handler.WriteError(w, errors.New("All tables are not found"))
		}
		return
	}

	// all databases' schemas
	handler.WriteData(w, schema.AllSchemas())
}

func getTableByIDStr(schema infoschema.InfoSchema, tableID string) (*model.TableInfo, error) {
	tid, err := strconv.Atoi(tableID)
	if err != nil {
		return nil, err
	}
	if tid < 0 {
		return nil, infoschema.ErrTableNotExists.GenWithStack("Table which ID = %s does not exist.", tableID)
	}
	if data, ok := schema.TableByID(context.Background(), int64(tid)); ok {
		return data.Meta(), nil
	}
	// The tid maybe a partition ID of the partition-table.
	tbl, _, _ := schema.FindTableByPartitionID(int64(tid))
	if tbl == nil {
		return nil, infoschema.ErrTableNotExists.GenWithStack("Table which ID = %s does not exist.", tableID)
	}
	return tbl.Meta(), nil
}

// ServeHTTP handles table related requests, such as table's region information, disk usage.
func (h *TableHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// parse params
	params := mux.Vars(req)
	dbName := params[handler.DBName]
	tableName := params[handler.TableName]
	schema, err := h.Schema()
	if err != nil {
		handler.WriteError(w, err)
		return
	}

	tableName, partitionName := handler.ExtractTableAndPartitionName(tableName)
	tableVal, err := schema.TableByName(context.Background(), ast.NewCIStr(dbName), ast.NewCIStr(tableName))
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	switch h.op {
	case OpTableRegions:
		h.handleRegionRequest(tableVal, w)
	case OpTableRanges:
		h.handleRangeRequest(tableVal, w)
	case OpTableDiskUsage:
		h.handleDiskUsageRequest(tableVal, w)
	case OpTableScatter:
		// supports partition table, only get one physical table, prevent too many scatter schedulers.
		ptbl, err := h.GetPartition(tableVal, partitionName)
		if err != nil {
			handler.WriteError(w, err)
			return
		}
		h.handleScatterTableRequest(ptbl, w)
	case OpStopTableScatter:
		ptbl, err := h.GetPartition(tableVal, partitionName)
		if err != nil {
			handler.WriteError(w, err)
			return
		}
		h.handleStopScatterTableRequest(ptbl, w)
	default:
		handler.WriteError(w, errors.New("method not found"))
	}
}

// ServeHTTP handles request of ddl jobs history.
func (h DDLHistoryJobHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var (
		jobID   = 0
		limitID = 0
		err     error
	)
	if jobValue := req.FormValue(handler.JobID); len(jobValue) > 0 {
		jobID, err = strconv.Atoi(jobValue)
		if err != nil {
			handler.WriteError(w, err)
			return
		}
		if jobID < 1 {
			handler.WriteError(w, errors.New("ddl history start_job_id must be greater than 0"))
			return
		}
	}
	if limitValue := req.FormValue(handler.Limit); len(limitValue) > 0 {
		limitID, err = strconv.Atoi(limitValue)
		if err != nil {
			handler.WriteError(w, err)
			return
		}
		if limitID < 1 || limitID > ddl.DefNumGetDDLHistoryJobs {
			handler.WriteError(w,
				errors.Errorf("ddl history limit must be greater than 0 and less than or equal to %v", ddl.DefNumGetDDLHistoryJobs))
			return
		}
	}

	jobs, err := h.getHistoryDDL(jobID, limitID)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	handler.WriteData(w, jobs)
}

func (h DDLHistoryJobHandler) getHistoryDDL(jobID, limit int) (jobs []*model.Job, err error) {
	txn, err := h.Store.Begin()
	if err != nil {
		return nil, errors.Trace(err)
	}
	txnMeta := meta.NewMutator(txn)

	jobs, err = ddl.ScanHistoryDDLJobs(txnMeta, int64(jobID), limit)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return jobs, nil
}

func (h DDLResignOwnerHandler) resignDDLOwner() error {
	dom, err := session.GetDomain(h.store)
	if err != nil {
		return errors.Trace(err)
	}

	ownerMgr := dom.DDL().OwnerManager()
	err = ownerMgr.ResignOwner(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// ServeHTTP handles request of resigning ddl owner.
func (h DDLResignOwnerHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		handler.WriteError(w, errors.Errorf("This api only support POST method"))
		return
	}

	err := h.resignDDLOwner()
	if err != nil {
		log.Error("failed to resign DDL owner", zap.Error(err))
		handler.WriteError(w, err)
		return
	}

	handler.WriteData(w, "success!")
}

func (h *TableHandler) getPDAddr() ([]string, error) {
	etcd, ok := h.Store.(kv.EtcdBackend)
	if !ok {
		return nil, errors.New("not implemented")
	}
	pdAddrs, err := etcd.EtcdAddrs()
	if err != nil {
		return nil, err
	}
	if len(pdAddrs) == 0 {
		return nil, errors.New("pd unavailable")
	}
	return pdAddrs, nil
}

func (h *TableHandler) addScatterSchedule(startKey, endKey []byte, name string) error {
	pdAddrs, err := h.getPDAddr()
	if err != nil {
		return err
	}
	input := map[string]string{
		"name":       "scatter-range",
		"start_key":  url.QueryEscape(string(startKey)),
		"end_key":    url.QueryEscape(string(endKey)),
		"range_name": name,
	}
	v, err := json.Marshal(input)
	if err != nil {
		return err
	}
	scheduleURL := fmt.Sprintf("%s://%s%s", util.InternalHTTPSchema(), pdAddrs[0], pd.Schedulers)
	resp, err := util.InternalHTTPClient().Post(scheduleURL, "application/json", bytes.NewBuffer(v))
	if err != nil {
		return err
	}
	if err := resp.Body.Close(); err != nil {
		log.Error("failed to close response body", zap.Error(err))
	}
	return nil
}

func (h *TableHandler) deleteScatterSchedule(name string) error {
	pdAddrs, err := h.getPDAddr()
	if err != nil {
		return err
	}
	scheduleURL := fmt.Sprintf("%s://%s%s", util.InternalHTTPSchema(), pdAddrs[0], pd.ScatterRangeSchedulerWithName(name))
	req, err := http.NewRequest(http.MethodDelete, scheduleURL, nil)
	if err != nil {
		return err
	}
	resp, err := util.InternalHTTPClient().Do(req)
	if err != nil {
		return err
	}
	if err := resp.Body.Close(); err != nil {
		log.Error("failed to close response body", zap.Error(err))
	}
	return nil
}

func (h *TableHandler) handleScatterTableRequest(tbl table.PhysicalTable, w http.ResponseWriter) {
	// for record
	tableID := tbl.GetPhysicalID()
	startKey, endKey := tablecodec.GetTableHandleKeyRange(tableID)
	startKey = codec.EncodeBytes([]byte{}, startKey)
	endKey = codec.EncodeBytes([]byte{}, endKey)
	tableName := fmt.Sprintf("%s-%d", tbl.Meta().Name.String(), tableID)
	err := h.addScatterSchedule(startKey, endKey, tableName)
	if err != nil {
		handler.WriteError(w, errors.Annotate(err, "scatter record error"))
		return
	}
	// for indices
	for _, index := range tbl.Indices() {
		indexID := index.Meta().ID
		indexName := index.Meta().Name.String()
		startKey, endKey := tablecodec.GetTableIndexKeyRange(tableID, indexID)
		startKey = codec.EncodeBytes([]byte{}, startKey)
		endKey = codec.EncodeBytes([]byte{}, endKey)
		name := tableName + "-" + indexName
		err := h.addScatterSchedule(startKey, endKey, name)
		if err != nil {
			handler.WriteError(w, errors.Annotatef(err, "scatter index(%s) error", name))
			return
		}
	}
	handler.WriteData(w, "success!")
}

func (h *TableHandler) handleStopScatterTableRequest(tbl table.PhysicalTable, w http.ResponseWriter) {
	// for record
	tableName := fmt.Sprintf("%s-%d", tbl.Meta().Name.String(), tbl.GetPhysicalID())
	err := h.deleteScatterSchedule(tableName)
	if err != nil {
		handler.WriteError(w, errors.Annotate(err, "stop scatter record error"))
		return
	}
	// for indices
	for _, index := range tbl.Indices() {
		indexName := index.Meta().Name.String()
		name := tableName + "-" + indexName
		err := h.deleteScatterSchedule(name)
		if err != nil {
			handler.WriteError(w, errors.Annotatef(err, "delete scatter index(%s) error", name))
			return
		}
	}
	handler.WriteData(w, "success!")
}

func (h *TableHandler) handleRegionRequest(tbl table.Table, w http.ResponseWriter) {
	pi := tbl.Meta().GetPartitionInfo()
	if pi != nil {
		// Partitioned table.
		var data []*TableRegions
		for _, def := range pi.Definitions {
			tableRegions, err := h.getRegionsByID(tbl, def.ID, def.Name.O)
			if err != nil {
				handler.WriteError(w, err)
				return
			}

			data = append(data, tableRegions)
		}
		handler.WriteData(w, data)
		return
	}

	meta := tbl.Meta()
	tableRegions, err := h.getRegionsByID(tbl, meta.ID, meta.Name.O)
	if err != nil {
		handler.WriteError(w, err)
		return
	}

	handler.WriteData(w, tableRegions)
}

func createTableRanges(tblID int64, tblName string, indices []*model.IndexInfo) *TableRanges {
	indexPrefix := tablecodec.GenTableIndexPrefix(tblID)
	recordPrefix := tablecodec.GenTableRecordPrefix(tblID)
	tableEnd := tablecodec.EncodeTablePrefix(tblID + 1)
	ranges := &TableRanges{
		TableName: tblName,
		TableID:   tblID,
		Range:     createRangeDetail(tablecodec.EncodeTablePrefix(tblID), tableEnd),
		Record:    createRangeDetail(recordPrefix, tableEnd),
		Index:     createRangeDetail(indexPrefix, recordPrefix),
	}
	if len(indices) != 0 {
		indexRanges := make(map[string]RangeDetail)
		for _, index := range indices {
			start := tablecodec.EncodeTableIndexPrefix(tblID, index.ID)
			end := tablecodec.EncodeTableIndexPrefix(tblID, index.ID+1)
			indexRanges[index.Name.String()] = createRangeDetail(start, end)
		}
		ranges.Indices = indexRanges
	}
	return ranges
}

func (*TableHandler) handleRangeRequest(tbl table.Table, w http.ResponseWriter) {
	meta := tbl.Meta()
	pi := meta.GetPartitionInfo()
	if pi != nil {
		// Partitioned table.
		var data []*TableRanges
		for _, def := range pi.Definitions {
			data = append(data, createTableRanges(def.ID, def.Name.String(), meta.Indices))
		}
		handler.WriteData(w, data)
		return
	}

	handler.WriteData(w, createTableRanges(meta.ID, meta.Name.String(), meta.Indices))
}

func (h *TableHandler) getRegionsByID(tbl table.Table, id int64, name string) (*TableRegions, error) {
	// for record
	startKey, endKey := tablecodec.GetTableHandleKeyRange(id)
	ctx := context.Background()
	pdCli := h.RegionCache.PDClient()
	regions, err := pdCli.BatchScanRegions(ctx, []router.KeyRange{{StartKey: startKey, EndKey: endKey}}, -1, opt.WithAllowFollowerHandle())
	if err != nil {
		return nil, err
	}

	recordRegions := make([]handler.RegionMeta, 0, len(regions))
	for _, region := range regions {
		meta := handler.RegionMeta{
			ID:          region.Meta.Id,
			Leader:      region.Leader,
			Peers:       region.Meta.Peers,
			RegionEpoch: region.Meta.RegionEpoch,
		}
		recordRegions = append(recordRegions, meta)
	}

	// for indices
	indices := make([]IndexRegions, len(tbl.Indices()))
	for i, index := range tbl.Indices() {
		indexID := index.Meta().ID
		indices[i].Name = index.Meta().Name.String()
		indices[i].ID = indexID
		startKey, endKey := tablecodec.GetTableIndexKeyRange(id, indexID)
		regions, err := pdCli.BatchScanRegions(ctx, []router.KeyRange{{StartKey: startKey, EndKey: endKey}}, -1, opt.WithAllowFollowerHandle())
		if err != nil {
			return nil, err
		}
		indexRegions := make([]handler.RegionMeta, 0, len(regions))
		for _, region := range regions {
			meta := handler.RegionMeta{
				ID:          region.Meta.Id,
				Leader:      region.Leader,
				Peers:       region.Meta.Peers,
				RegionEpoch: region.Meta.RegionEpoch,
			}
			indexRegions = append(indexRegions, meta)
		}
		indices[i].Regions = indexRegions
	}

	return &TableRegions{
		TableName:     name,
		TableID:       id,
		Indices:       indices,
		RecordRegions: recordRegions,
	}, nil
}

func (h *TableHandler) handleDiskUsageRequest(tbl table.Table, w http.ResponseWriter) {
	stats, err := h.GetPDRegionStats(context.Background(), tbl.Meta().ID, false)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	handler.WriteData(w, stats.StorageSize)
}
