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
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

const (
	pDBName     = "db"
	pHexKey     = "hexKey"
	pIndexName  = "index"
	pHandle     = "handle"
	pRegionID   = "regionID"
	pStartTS    = "startTS"
	pTableName  = "table"
	pColumnID   = "colID"
	pColumnTp   = "colTp"
	pColumnFlag = "colFlag"
	pColumnLen  = "colLen"
	pRowBin     = "rowBin"
)

// For query string
const qTableID = "table_id"

const (
	headerContentType = "Content-Type"
	contentTypeJSON   = "application/json"
)

type kvStore interface {
	GetRegionCache() *tikv.RegionCache
	SendReq(bo *tikv.Backoffer, req *tikvrpc.Request, regionID tikv.RegionVerID, timeout time.Duration) (*tikvrpc.Response, error)
}

func writeError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	_, err = w.Write([]byte(err.Error()))
	terror.Log(errors.Trace(err))
}

func writeData(w http.ResponseWriter, data interface{}) {
	js, err := json.Marshal(data)
	if err != nil {
		writeError(w, err)
		return
	}
	log.Info(string(js))
	// write response
	w.Header().Set(headerContentType, contentTypeJSON)
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(js)
	terror.Log(errors.Trace(err))
}

type tikvHandlerTool struct {
	regionCache *tikv.RegionCache
	store       kvStore
}

// newTikvHandlerTool checks and prepares for tikv handler.
// It would panic when any error happens.
func (s *Server) newTikvHandlerTool() *tikvHandlerTool {
	var tikvStore kvStore
	store, ok := s.driver.(*TiDBDriver)
	if !ok {
		panic("Invalid KvStore with illegal driver")
	}

	if tikvStore, ok = store.store.(kvStore); !ok {
		panic("Invalid KvStore with illegal store")
	}

	regionCache := tikvStore.GetRegionCache()

	return &tikvHandlerTool{
		regionCache: regionCache,
		store:       tikvStore,
	}
}

func (t *tikvHandlerTool) getMvccByEncodedKey(encodedKey kv.Key) (*kvrpcpb.MvccGetByKeyResponse, error) {
	keyLocation, err := t.regionCache.LocateKey(tikv.NewBackoffer(context.Background(), 500), encodedKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tikvReq := &tikvrpc.Request{
		Type: tikvrpc.CmdMvccGetByKey,
		MvccGetByKey: &kvrpcpb.MvccGetByKeyRequest{
			Key: encodedKey,
		},
	}
	kvResp, err := t.store.SendReq(tikv.NewBackoffer(context.Background(), 500), tikvReq, keyLocation.Region, time.Minute)
	log.Info(string(encodedKey), keyLocation.Region, string(keyLocation.StartKey), string(keyLocation.EndKey), kvResp, err)

	if err != nil {
		return nil, errors.Trace(err)
	}
	return kvResp.MvccGetByKey, nil
}

func (t *tikvHandlerTool) getMvccByHandle(tableID, handle int64) (*kvrpcpb.MvccGetByKeyResponse, error) {
	encodedKey := tablecodec.EncodeRowKeyWithHandle(tableID, handle)
	return t.getMvccByEncodedKey(encodedKey)
}

func (t *tikvHandlerTool) getMvccByStartTs(startTS uint64, startKey, endKey []byte) (*kvrpcpb.MvccGetByStartTsResponse, error) {
	bo := tikv.NewBackoffer(context.Background(), 5000)
	for {
		curRegion, err := t.regionCache.LocateKey(bo, startKey)
		if err != nil {
			log.Error(startTS, startKey, err)
			return nil, errors.Trace(err)
		}

		tikvReq := &tikvrpc.Request{
			Type: tikvrpc.CmdMvccGetByStartTs,
			MvccGetByStartTs: &kvrpcpb.MvccGetByStartTsRequest{
				StartTs: startTS,
			},
		}
		tikvReq.Context.Priority = kvrpcpb.CommandPri_Low
		kvResp, err := t.store.SendReq(bo, tikvReq, curRegion.Region, time.Hour)
		log.Info(startTS, string(startKey), curRegion.Region, string(curRegion.StartKey), string(curRegion.EndKey), kvResp)
		if err != nil {
			log.Error(startTS, string(startKey), curRegion.Region, string(curRegion.StartKey), string(curRegion.EndKey), err)
			return nil, errors.Trace(err)
		}
		data := kvResp.MvccGetByStartTS
		if err := data.GetRegionError(); err != nil {
			log.Warn(startTS, string(startKey), curRegion.Region, string(curRegion.StartKey), string(curRegion.EndKey), err)
			continue
		}

		if len(data.GetError()) > 0 {
			log.Error(startTS, string(startKey), curRegion.Region, string(curRegion.StartKey), string(curRegion.EndKey), data.GetError())
			return nil, errors.New(data.GetError())
		}

		key := data.GetKey()
		if len(key) > 0 {
			return data, nil
		}

		if len(endKey) > 0 && curRegion.Contains(endKey) {
			return nil, nil
		}
		if len(curRegion.EndKey) == 0 {
			return nil, nil
		}
		startKey = curRegion.EndKey
	}
}

func (t *tikvHandlerTool) getMvccByIdxValue(idx table.Index, values url.Values, idxCols []*model.ColumnInfo, handleStr string) (*kvrpcpb.MvccGetByKeyResponse, error) {
	sc := new(stmtctx.StatementContext)
	// HTTP request is not a database session, set timezone to UTC directly here.
	// See https://github.com/pingcap/tidb/blob/master/docs/tidb_http_api.md for more details.
	sc.TimeZone = time.UTC
	idxRow, err := t.formValue2DatumRow(sc, values, idxCols)
	if err != nil {
		return nil, errors.Trace(err)
	}
	handle, err := strconv.ParseInt(handleStr, 10, 64)
	if err != nil {
		return nil, errors.Trace(err)
	}
	encodedKey, _, err := idx.GenIndexKey(sc, idxRow, handle, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return t.getMvccByEncodedKey(encodedKey)
}

// formValue2DatumRow converts URL query string to a Datum Row.
func (t *tikvHandlerTool) formValue2DatumRow(sc *stmtctx.StatementContext, values url.Values, idxCols []*model.ColumnInfo) ([]types.Datum, error) {
	data := make([]types.Datum, len(idxCols))
	for i, col := range idxCols {
		colName := col.Name.String()
		vals, ok := values[colName]
		if !ok {
			return nil, errors.BadRequestf("Missing value for index column %s.", colName)
		}

		switch len(vals) {
		case 0:
			data[i].SetNull()
		case 1:
			bDatum := types.NewStringDatum(vals[0])
			cDatum, err := bDatum.ConvertTo(sc, &col.FieldType)
			if err != nil {
				return nil, errors.Trace(err)
			}
			data[i] = cDatum
		default:
			return nil, errors.BadRequestf("Invalid query form for column '%s', it's values are %v."+
				" Column value should be unique for one index record.", colName, vals)
		}
	}
	return data, nil
}

func (t *tikvHandlerTool) getTableID(dbName, tableName string) (int64, error) {
	schema, err := t.schema()
	if err != nil {
		return 0, errors.Trace(err)
	}
	tableVal, err := schema.TableByName(model.NewCIStr(dbName), model.NewCIStr(tableName))
	if err != nil {
		return 0, errors.Trace(err)
	}
	return tableVal.Meta().ID, nil
}

func (t *tikvHandlerTool) schema() (infoschema.InfoSchema, error) {
	session, err := tidb.CreateSession(t.store.(kv.Storage))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return domain.GetDomain(session.(sessionctx.Context)).InfoSchema(), nil
}

func (t *tikvHandlerTool) handleMvccGetByHex(params map[string]string) (interface{}, error) {
	encodedKey, err := hex.DecodeString(params[pHexKey])
	if err != nil {
		return nil, errors.Trace(err)
	}
	return t.getMvccByEncodedKey(encodedKey)
}

// settingsHandler is the handler for list tidb server settings.
type settingsHandler struct {
}

// schemaHandler is the handler for list database or table schemas.
type schemaHandler struct {
	*tikvHandlerTool
}

// regionHandler is the common field for http handler. It contains
// some common functions for all handlers.
type regionHandler struct {
	*tikvHandlerTool
}

// tableHandler is the handler for list table's regions.
type tableHandler struct {
	*tikvHandlerTool
	op string
}

// valueHandle is the handler for get value.
type valueHandler struct {
}

const (
	opTableRegions   = "regions"
	opTableDiskUsage = "disk-usage"
)

// mvccTxnHandler is the handler for txn debugger
type mvccTxnHandler struct {
	*tikvHandlerTool
	op string
}

const (
	opMvccGetByHex = "hex"
	opMvccGetByKey = "key"
	opMvccGetByIdx = "idx"
	opMvccGetByTxn = "txn"
)

// ServeHTTP handles request of list a database or table's schemas.
func (vh valueHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// parse params
	params := mux.Vars(req)

	colID, err := strconv.ParseInt(params[pColumnID], 0, 64)
	if err != nil {
		writeError(w, err)
		return
	}
	colTp, err := strconv.ParseInt(params[pColumnTp], 0, 64)
	if err != nil {
		writeError(w, err)
		return
	}
	colFlag, err := strconv.ParseUint(params[pColumnFlag], 0, 64)
	if err != nil {
		writeError(w, err)
		return
	}
	colLen, err := strconv.ParseInt(params[pColumnLen], 0, 64)
	if err != nil {
		writeError(w, err)
		return
	}

	// Get the unchanged binary.
	if req.URL == nil {
		err = errors.BadRequestf("Invalid URL")
		writeError(w, err)
		return
	}
	values := make(url.Values)
	shouldUnescape := false
	err = parseQuery(req.URL.RawQuery, values, shouldUnescape)
	if err != nil {
		writeError(w, err)
		return
	}
	if len(values[pRowBin]) != 1 {
		err = errors.BadRequestf("Invalid Query:%v", values[pRowBin])
		writeError(w, err)
		return
	}
	bin := values[pRowBin][0]
	valData, err := base64.StdEncoding.DecodeString(bin)
	if err != nil {
		writeError(w, err)
		return
	}
	// Construct field type.
	defaultDecimal := 6
	ft := &types.FieldType{
		Tp:      byte(colTp),
		Flag:    uint(colFlag),
		Flen:    int(colLen),
		Decimal: defaultDecimal,
	}
	// Decode a column.
	m := make(map[int64]*types.FieldType, 1)
	m[int64(colID)] = ft
	loc := time.UTC
	vals, err := tablecodec.DecodeRow(valData, m, loc)
	if err != nil {
		writeError(w, err)
		return
	}

	v := vals[int64(colID)]
	val, err := v.ToString()
	if err != nil {
		writeError(w, err)
		return
	}
	writeData(w, val)
	return
}

// TableRegions is the response data for list table's regions.
// It contains regions list for record and indices.
type TableRegions struct {
	TableName     string         `json:"name"`
	TableID       int64          `json:"id"`
	RecordRegions []RegionMeta   `json:"record_regions"`
	Indices       []IndexRegions `json:"indices"`
}

// RegionMeta contains a region's peer detail
type RegionMeta struct {
	ID          uint64              `json:"region_id"`
	Leader      *metapb.Peer        `json:"leader"`
	Peers       []*metapb.Peer      `json:"peers"`
	RegionEpoch *metapb.RegionEpoch `json:"region_epoch"`
}

// IndexRegions is the region info for one index.
type IndexRegions struct {
	Name    string       `json:"name"`
	ID      int64        `json:"id"`
	Regions []RegionMeta `json:"regions"`
}

// RegionDetail is the response data for get region by ID
// it includes indices and records detail in current region.
type RegionDetail struct {
	RegionID uint64       `json:"region_id"`
	StartKey []byte       `json:"start_key"`
	EndKey   []byte       `json:"end_key"`
	Frames   []*FrameItem `json:"frames"`
}

// addTableInRange insert a table into RegionDetail
// with index's id or record in the range if r.
func (rt *RegionDetail) addTableInRange(dbName string, curTable *model.TableInfo, r *RegionFrameRange) {
	tName := curTable.Name.String()
	tID := curTable.ID

	for _, index := range curTable.Indices {
		if f := r.getIndexFrame(tID, index.ID, dbName, tName, index.Name.String()); f != nil {
			rt.Frames = append(rt.Frames, f)
		}
	}
	if f := r.getRecordFrame(tID, dbName, tName); f != nil {
		rt.Frames = append(rt.Frames, f)
	}
}

// FrameItem includes a index's or record's meta data with table's info.
type FrameItem struct {
	DBName    string `json:"db_name"`
	TableName string `json:"table_name"`
	TableID   int64  `json:"table_id"`
	IsRecord  bool   `json:"is_record"`
	IndexName string `json:"index_name,omitempty"`
	IndexID   int64  `json:"index_id,omitempty"`
}

// RegionFrameRange contains a frame range info which the region covered.
type RegionFrameRange struct {
	first  *FrameItem        // start frame of the region
	last   *FrameItem        // end frame of the region
	region *tikv.KeyLocation // the region
}

func (t *tikvHandlerTool) getRegionsMeta(regionIDs []uint64) ([]RegionMeta, error) {
	regions := make([]RegionMeta, len(regionIDs))
	for i, regionID := range regionIDs {
		meta, leader, err := t.regionCache.PDClient().GetRegionByID(context.TODO(), regionID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		regions[i] = RegionMeta{
			ID:          regionID,
			Leader:      leader,
			Peers:       meta.Peers,
			RegionEpoch: meta.RegionEpoch,
		}

	}
	return regions, nil
}

// ServeHTTP handles request of list tidb server settings.
func (h settingsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	writeData(w, config.GetGlobalConfig())
	return
}

// ServeHTTP handles request of list a database or table's schemas.
func (h schemaHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	schema, err := h.schema()
	if err != nil {
		writeError(w, err)
		return
	}

	// parse params
	params := mux.Vars(req)

	if dbName, ok := params[pDBName]; ok {
		cDBName := model.NewCIStr(dbName)
		if tableName, ok := params[pTableName]; ok {
			// table schema of a specified table name
			cTableName := model.NewCIStr(tableName)
			data, err := schema.TableByName(cDBName, cTableName)
			if err != nil {
				writeError(w, err)
				return
			}
			writeData(w, data.Meta())
			return
		}
		// all table schemas in a specified database
		if schema.SchemaExists(cDBName) {
			tbs := schema.SchemaTables(cDBName)
			tbsInfo := make([]*model.TableInfo, len(tbs))
			for i := range tbsInfo {
				tbsInfo[i] = tbs[i].Meta()
			}
			writeData(w, tbsInfo)
			return
		}
		writeError(w, infoschema.ErrDatabaseNotExists.GenByArgs(dbName))
		return
	}

	if tableID := req.FormValue(qTableID); len(tableID) > 0 {
		// table schema of a specified tableID
		tid, err := strconv.Atoi(tableID)
		if err != nil {
			writeError(w, err)
			return
		}
		if tid < 0 {
			writeError(w, infoschema.ErrTableNotExists.Gen("Table which ID = %s does not exist.", tableID))
			return
		}
		if data, ok := schema.TableByID(int64(tid)); ok {
			writeData(w, data.Meta())
			return
		}
		writeError(w, infoschema.ErrTableNotExists.Gen("Table which ID = %s does not exist.", tableID))
		return
	}

	// all databases' schemas
	writeData(w, schema.AllSchemas())
	return
}

// ServeHTTP handles table related requests, such as table's region information, disk usage.
func (h tableHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// parse params
	params := mux.Vars(req)
	dbName := params[pDBName]
	tableName := params[pTableName]
	schema, err := h.schema()
	if err != nil {
		writeError(w, err)
		return
	}
	// get table's schema.
	tableVal, err := schema.TableByName(model.NewCIStr(dbName), model.NewCIStr(tableName))
	if err != nil {
		writeError(w, err)
		return
	}

	switch h.op {
	case opTableRegions:
		h.handleRegionRequest(schema, tableVal, w, req)
	case opTableDiskUsage:
		h.handleDiskUsageRequest(schema, tableVal, w, req)
	default:
		writeError(w, errors.New("method not found"))
	}
}

func (h tableHandler) handleRegionRequest(schema infoschema.InfoSchema, tbl table.Table, w http.ResponseWriter, req *http.Request) {
	tableID := tbl.Meta().ID
	// for record
	startKey, endKey := tablecodec.GetTableHandleKeyRange(tableID)
	recordRegionIDs, err := h.regionCache.ListRegionIDsInKeyRange(tikv.NewBackoffer(context.Background(), 500), startKey, endKey)
	if err != nil {
		writeError(w, err)
		return
	}
	recordRegions, err := h.getRegionsMeta(recordRegionIDs)
	if err != nil {
		writeError(w, err)
		return
	}

	// for indices
	indices := make([]IndexRegions, len(tbl.Indices()))
	for i, index := range tbl.Indices() {
		indexID := index.Meta().ID
		indices[i].Name = index.Meta().Name.String()
		indices[i].ID = indexID
		startKey, endKey := tablecodec.GetTableIndexKeyRange(tableID, indexID)
		rIDs, err := h.regionCache.ListRegionIDsInKeyRange(tikv.NewBackoffer(context.Background(), 500), startKey, endKey)
		if err != nil {
			writeError(w, err)
			return
		}
		indices[i].Regions, err = h.getRegionsMeta(rIDs)
		if err != nil {
			writeError(w, err)
			return
		}
	}

	tableRegions := &TableRegions{
		TableName:     tbl.Meta().Name.O,
		TableID:       tableID,
		Indices:       indices,
		RecordRegions: recordRegions,
	}

	writeData(w, tableRegions)
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

func (h tableHandler) handleDiskUsageRequest(schema infoschema.InfoSchema, tbl table.Table, w http.ResponseWriter, req *http.Request) {
	tableID := tbl.Meta().ID
	var pdAddrs []string
	etcd, ok := h.store.(domain.EtcdBackend)
	if !ok {
		writeError(w, errors.New("not implemented"))
	}
	pdAddrs = etcd.EtcdAddrs()
	if len(pdAddrs) < 0 {
		writeError(w, errors.New("pd unavailable"))
	}

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
		writeError(w, err)
		return
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Error(err)
		}
	}()

	var stats pdRegionStats
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&stats); err != nil {
		writeError(w, err)
		return
	}
	writeData(w, stats.StorageSize)
}

// ServeHTTP handles request of get region by ID.
func (h regionHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// parse and check params
	params := mux.Vars(req)
	if _, ok := params[pRegionID]; !ok {
		startKey := []byte{'m'}
		endKey := []byte{'n'}

		recordRegionIDs, err := h.regionCache.ListRegionIDsInKeyRange(tikv.NewBackoffer(context.Background(), 500), startKey, endKey)
		if err != nil {
			writeError(w, err)
			return
		}

		recordRegions, err := h.getRegionsMeta(recordRegionIDs)
		if err != nil {
			writeError(w, err)
			return
		}
		writeData(w, recordRegions)
		return
	}

	regionIDInt, err := strconv.ParseInt(params[pRegionID], 0, 64)
	if err != nil {
		writeError(w, err)
		return
	}
	regionID := uint64(regionIDInt)

	// locate region
	region, err := h.regionCache.LocateRegionByID(tikv.NewBackoffer(context.Background(), 500), regionID)
	if err != nil {
		writeError(w, err)
		return
	}

	frameRange, err := NewRegionFrameRange(region)
	if err != nil {
		writeError(w, err)
		return
	}

	// create RegionDetail from RegionFrameRange
	regionDetail := &RegionDetail{
		RegionID: regionID,
		StartKey: region.StartKey,
		EndKey:   region.EndKey,
	}
	schema, err := h.schema()
	if err != nil {
		writeError(w, err)
		return
	}
	// Since we need a database's name for each frame, and a table's database name can not
	// get from table's ID directly. Above all, here do dot process like
	// 		`for id in [frameRange.firstTableID,frameRange.endTableID]`
	// on [frameRange.firstTableID,frameRange.endTableID] is small enough.
	for _, db := range schema.AllSchemas() {
		for _, tableVal := range db.Tables {
			regionDetail.addTableInRange(db.Name.String(), tableVal, frameRange)
		}
	}
	writeData(w, regionDetail)
}

// NewFrameItemFromRegionKey creates a FrameItem with region's startKey or endKey,
// returns err when key is illegal.
func NewFrameItemFromRegionKey(key []byte) (frame *FrameItem, err error) {
	frame = &FrameItem{}
	frame.TableID, frame.IndexID, frame.IsRecord, err = tablecodec.DecodeKeyHead(key)
	if err == nil {
		return
	}
	if bytes.HasPrefix(key, tablecodec.TablePrefix()) {
		// If SplitTable is enabled, the key may be `t{id}`.
		if len(key) == tablecodec.TableSplitKeyLen {
			frame.TableID = tablecodec.DecodeTableID(key)
			return frame, nil
		}
		return nil, errors.Trace(err)
	}

	// key start with tablePrefix must be either record key or index key
	// That's means table's record key and index key are always together
	// in the continuous interval. And for key with prefix smaller than
	// tablePrefix, is smaller than all tables. While for key with prefix
	// bigger than tablePrefix, means is bigger than all tables.
	err = nil
	if bytes.Compare(key, tablecodec.TablePrefix()) < 0 {
		frame.TableID = math.MinInt64
		frame.IndexID = math.MinInt64
		frame.IsRecord = false
		return
	}
	// bigger than tablePrefix, means is bigger than all tables.
	frame.TableID = math.MaxInt64
	frame.TableID = math.MaxInt64
	frame.IsRecord = true
	return
}

// NewRegionFrameRange init a NewRegionFrameRange with region info.
func NewRegionFrameRange(region *tikv.KeyLocation) (idxRange *RegionFrameRange, err error) {
	var first, last *FrameItem
	// check and init first frame
	if len(region.StartKey) > 0 {
		first, err = NewFrameItemFromRegionKey(region.StartKey)
		if err != nil {
			return
		}
	} else { // empty startKey means start with -infinite
		first = &FrameItem{
			IndexID:  int64(math.MinInt64),
			IsRecord: false,
			TableID:  int64(math.MinInt64),
		}
	}

	// check and init last frame
	if len(region.EndKey) > 0 {
		last, err = NewFrameItemFromRegionKey(region.EndKey)
		if err != nil {
			return
		}
	} else { // empty endKey means end with +infinite
		last = &FrameItem{
			TableID:  int64(math.MaxInt64),
			IndexID:  int64(math.MaxInt64),
			IsRecord: true,
		}
	}

	idxRange = &RegionFrameRange{
		region: region,
		first:  first,
		last:   last,
	}
	return idxRange, nil
}

// getRecordFrame returns the record frame of a table. If the table's records
// are not covered by this frame range, it returns nil.
func (r *RegionFrameRange) getRecordFrame(tableID int64, dbName, tableName string) *FrameItem {
	if tableID == r.first.TableID && r.first.IsRecord {
		return r.first
	}
	if tableID == r.last.TableID && r.last.IsRecord {
		return r.last
	}

	if tableID >= r.first.TableID && tableID < r.last.TableID {
		return &FrameItem{
			DBName:    dbName,
			TableName: tableName,
			TableID:   tableID,
			IsRecord:  true,
		}
	}
	return nil
}

// getIndexFrame returns the indnex frame of a table. If the table's indices are
// not covered by this frame range, it returns nil.
func (r *RegionFrameRange) getIndexFrame(tableID, indexID int64, dbName, tableName, indexname string) *FrameItem {
	if tableID == r.first.TableID && !r.first.IsRecord && indexID == r.first.IndexID {
		return r.first
	}
	if tableID == r.last.TableID && indexID == r.last.IndexID {
		return r.last
	}

	greaterThanFirst := tableID > r.first.TableID || (tableID == r.first.TableID && !r.first.IsRecord && indexID > r.first.IndexID)
	lessThanLast := tableID < r.last.TableID || (tableID == r.last.TableID && (r.last.IsRecord || indexID < r.last.IndexID))
	if greaterThanFirst && lessThanLast {
		return &FrameItem{
			DBName:    dbName,
			TableName: tableName,
			TableID:   tableID,
			IsRecord:  false,
			IndexName: indexname,
			IndexID:   indexID,
		}
	}
	return nil
}

// parseQuery is used to parse query string in URL with shouldUnescape, due to golang http package can not distinguish
// query like "?a=" and "?a". We rewrite it to separate these two queries. e.g.
// "?a=" which means that a is an empty string "";
// "?a"  which means that a is null.
// If shouldUnescape is true, we use QueryUnescape to handle keys and values that will be put in m.
// If shouldUnescape is false, we don't use QueryUnescap to handle.
func parseQuery(query string, m url.Values, shouldUnescape bool) error {
	var err error
	for query != "" {
		key := query
		if i := strings.IndexAny(key, "&;"); i >= 0 {
			key, query = key[:i], key[i+1:]
		} else {
			query = ""
		}
		if key == "" {
			continue
		}
		if i := strings.Index(key, "="); i >= 0 {
			value := ""
			key, value = key[:i], key[i+1:]
			if shouldUnescape {
				key, err = url.QueryUnescape(key)
				if err != nil {
					return errors.Trace(err)
				}
				value, err = url.QueryUnescape(value)
				if err != nil {
					return errors.Trace(err)
				}
			}
			m[key] = append(m[key], value)
		} else {
			if shouldUnescape {
				key, err = url.QueryUnescape(key)
				if err != nil {
					return errors.Trace(err)
				}
			}
			if _, ok := m[key]; !ok {
				m[key] = nil
			}
		}
	}
	return errors.Trace(err)
}

// ServeHTTP handles request of list a table's regions.
func (h mvccTxnHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var data interface{}
	params := mux.Vars(req)
	var err error
	switch h.op {
	case opMvccGetByHex:
		data, err = h.handleMvccGetByHex(params)
	case opMvccGetByIdx:
		if req.URL == nil {
			err = errors.BadRequestf("Invalid URL")
			break
		}
		values := make(url.Values)
		err = parseQuery(req.URL.RawQuery, values, true)
		if err == nil {
			data, err = h.handleMvccGetByIdx(params, values)
		}
	case opMvccGetByKey:
		data, err = h.handleMvccGetByKey(params)
	case opMvccGetByTxn:
		data, err = h.handleMvccGetByTxn(params)
	default:
		err = errors.NotSupportedf("Operation not supported.")
	}
	if err != nil {
		writeError(w, err)
	} else {
		writeData(w, data)
	}
}

// handleMvccGetByIdx gets MVCC info by an index key.
func (h mvccTxnHandler) handleMvccGetByIdx(params map[string]string, values url.Values) (interface{}, error) {
	dbName := params[pDBName]
	tableName := params[pTableName]
	handleStr := params[pHandle]
	schema, err := h.schema()
	if err != nil {
		return nil, errors.Trace(err)
	}
	// get table's schema.
	t, err := schema.TableByName(model.NewCIStr(dbName), model.NewCIStr(tableName))
	if err != nil {
		return nil, errors.Trace(err)
	}
	var idxCols []*model.ColumnInfo
	var idx table.Index
	for _, v := range t.Indices() {
		if strings.EqualFold(v.Meta().Name.String(), params[pIndexName]) {
			for _, c := range v.Meta().Columns {
				idxCols = append(idxCols, t.Meta().Columns[c.Offset])
			}
			idx = v
			break
		}
	}
	if idx == nil {
		return nil, errors.NotFoundf("Index %s not found!", params[pIndexName])
	}
	return h.getMvccByIdxValue(idx, values, idxCols, handleStr)
}

func (h mvccTxnHandler) handleMvccGetByKey(params map[string]string) (interface{}, error) {
	handle, err := strconv.ParseInt(params[pHandle], 0, 64)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tableID, err := h.getTableID(params[pDBName], params[pTableName])
	if err != nil {
		return nil, errors.Trace(err)
	}
	return h.getMvccByHandle(tableID, handle)
}

func (h *mvccTxnHandler) handleMvccGetByTxn(params map[string]string) (interface{}, error) {
	startTS, err := strconv.ParseInt(params[pStartTS], 0, 64)
	if err != nil {
		return nil, errors.Trace(err)
	}

	startKey := []byte("")
	endKey := []byte("")
	dbName := params[pDBName]
	if len(dbName) > 0 {
		tableID, err := h.getTableID(params[pDBName], params[pTableName])
		if err != nil {
			return nil, errors.Trace(err)
		}
		startKey = tablecodec.EncodeTablePrefix(tableID)
		endKey = tablecodec.EncodeRowKeyWithHandle(tableID, math.MaxInt64)
	}
	return h.getMvccByStartTs(uint64(startTS), startKey, endKey)
}
