// Copyright 2017 PingCAP, Inc.
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
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	goctx "golang.org/x/net/context"
)

const (
	pDBName    = "db"
	pTableName = "table"
	pRegionID  = "regionID"
	pRecordID  = "recordID"
	pStartTS   = "startTS"
)

const (
	headerContentType = "Content-Type"
	contentTypeJSON   = "application/json"
)

type kvStore interface {
	GetRegionCache() *tikv.RegionCache
	SendReq(bo *tikv.Backoffer, req *tikvrpc.Request, regionID tikv.RegionVerID, timeout time.Duration) (*tikvrpc.Response, error)
}

type regionHandlerTool struct {
	bo          *tikv.Backoffer
	regionCache *tikv.RegionCache
	store       kvStore
}

// RegionHandler is the common field for http region handler. It contains
// some common functions for all handlers.
type regionHandler struct {
	*regionHandlerTool
}

// tableRegionsHandler is the handler for list table's regions.
type tableRegionsHandler struct {
	*regionHandler
}

// MvccTxnHandler is the handler for txn debugger
type mvccTxnHandler struct {
	*regionHandler
	op string
}

const (
	opMvccGetByKey = "key"
	opMvccGetByTxn = "txn"
)

// newRegionHandler checks and prepares for region handler.
// It would panic when any error happens.
func (s *Server) newRegionHandler() (hanler *regionHandler) {
	var tikvStore kvStore
	store, ok := s.driver.(*TiDBDriver)
	if !ok {
		panic("Invalid KvStore with illegal driver")
	}

	if tikvStore, ok = store.store.(kvStore); !ok {
		panic("Invalid KvStore with illegal store")
	}

	regionCache := tikvStore.GetRegionCache()

	// init backOffer && infoSchema.
	backOffer := tikv.NewBackoffer(500, goctx.Background())

	tool := &regionHandlerTool{
		regionCache: regionCache,
		bo:          backOffer,
		store:       tikvStore,
	}
	return &regionHandler{tool}
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
	RegionID uint64      `json:"region_id"`
	StartKey []byte      `json:"start_key"`
	EndKey   []byte      `json:"end_key"`
	Frames   []FrameItem `json:"frames"`
}

// addIndex insert a index into RegionDetail.
func (rt *RegionDetail) addIndex(dbName, tName string, tID int64, indexName string, indexID int64) {
	rt.Frames = append(rt.Frames, FrameItem{
		DBName:    dbName,
		TableName: tName,
		TableID:   tID,
		IndexName: indexName,
		IndexID:   indexID,
		IsRecord:  false,
	})
}

// addRecord insert a table's record into RegionDetail.
func (rt *RegionDetail) addRecord(dbName, tName string, tID int64) {
	rt.Frames = append(rt.Frames, FrameItem{
		DBName:    dbName,
		TableName: tName,
		TableID:   tID,
		IsRecord:  true,
	})
}

// addTableInRange insert a table into RegionDetail
// with index's id in range [startID,endID]. Table's
// record would be included when endID is MaxInt64.
func (rt *RegionDetail) addTableInRange(dbName string, curTable *model.TableInfo, startID, endID int64) {
	tName := curTable.Name.String()
	tID := curTable.ID

	for _, index := range curTable.Indices {
		if index.ID >= startID && index.ID <= endID {
			rt.addIndex(
				dbName,
				tName,
				tID,
				index.Name.String(),
				index.ID)
		}
	}
	if endID == math.MaxInt64 {
		rt.addRecord(dbName, tName, tID)
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

func (t *regionHandlerTool) getRegionsMeta(regionIDs []uint64) ([]RegionMeta, error) {
	regions := make([]RegionMeta, len(regionIDs))
	for i, regionID := range regionIDs {
		meta, leader, err := t.regionCache.PDClient().GetRegionByID(goctx.TODO(), regionID)
		if err != nil {
			return nil, err
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

// ServeHTTP handles request of list a table's regions.
func (rh tableRegionsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// parse params
	params := mux.Vars(req)
	dbName := params[pDBName]
	tableName := params[pTableName]
	schema, err := rh.schema()
	if err != nil {
		rh.writeError(w, err)
		return
	}
	// get table's schema.
	table, err := schema.TableByName(model.NewCIStr(dbName), model.NewCIStr(tableName))
	if err != nil {
		rh.writeError(w, err)
		return
	}
	tableID := table.Meta().ID

	// for record
	startKey, endKey := tablecodec.GetTableHandleKeyRange(tableID)
	recordRegionIDs, err := rh.regionCache.ListRegionIDsInKeyRange(rh.bo, startKey, endKey)
	if err != nil {
		rh.writeError(w, err)
		return
	}
	recordRegions, err := rh.getRegionsMeta(recordRegionIDs)
	if err != nil {
		rh.writeError(w, err)
		return
	}

	// for indices
	indices := make([]IndexRegions, len(table.Indices()))
	for i, index := range table.Indices() {
		indexID := index.Meta().ID
		indices[i].Name = index.Meta().Name.String()
		indices[i].ID = indexID
		startKey, endKey := tablecodec.GetTableIndexKeyRange(tableID, indexID)
		rIDs, err := rh.regionCache.ListRegionIDsInKeyRange(rh.bo, startKey, endKey)
		if err != nil {
			rh.writeError(w, err)
			return
		}
		indices[i].Regions, err = rh.getRegionsMeta(rIDs)
		if err != nil {
			rh.writeError(w, err)
			return
		}
	}

	tableRegions := &TableRegions{
		TableName:     tableName,
		TableID:       tableID,
		Indices:       indices,
		RecordRegions: recordRegions,
	}

	rh.writeData(w, tableRegions)
}

// ServeHTTP handles request of get region by ID.
func (rh regionHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// parse and check params
	params := mux.Vars(req)
	regionIDInt, err := strconv.ParseInt(params[pRegionID], 0, 64)
	if err != nil {
		rh.writeError(w, err)
		return
	}
	regionID := uint64(regionIDInt)

	// locate region
	region, err := rh.regionCache.LocateRegionByID(rh.bo, regionID)
	if err != nil {
		rh.writeError(w, err)
		return
	}

	frameRange, err := NewRegionFrameRange(region)
	if err != nil {
		rh.writeError(w, err)
		return
	}

	// create RegionDetail from RegionFrameRange
	regionDetail := &RegionDetail{
		RegionID: regionID,
		StartKey: region.StartKey,
		EndKey:   region.EndKey,
	}
	schema, err := rh.schema()
	if err != nil {
		rh.writeError(w, err)
		return
	}
	// Since we need a database's name for each frame, and a table's database name can not
	// get from table's ID directly. Above all, here do dot process like
	// 		`for id in [frameRange.firstTableID,frameRange.endTableID]`
	// on [frameRange.firstTableID,frameRange.endTableID] is small enough.
	for _, db := range schema.AllSchemas() {
		for _, table := range db.Tables {
			start, end := frameRange.getIndexRangeForTable(table.ID)
			regionDetail.addTableInRange(db.Name.String(), table, start, end)
		}
	}
	rh.writeData(w, regionDetail)
}

func (rh *regionHandler) writeError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	w.Write([]byte(err.Error()))
}

func (rh *regionHandler) writeData(w http.ResponseWriter, data interface{}) {
	js, err := json.Marshal(data)
	if err != nil {
		rh.writeError(w, err)
		return
	}
	log.Info(string(js))
	// write response
	w.Header().Set(headerContentType, contentTypeJSON)
	w.WriteHeader(http.StatusOK)
	w.Write(js)
}

// NewFrameItemFromRegionKey creates a FrameItem with region's startKey or endKey,
// returns err when key is illegal.
func NewFrameItemFromRegionKey(key []byte) (frame *FrameItem, err error) {
	_, key, err = codec.DecodeBytes(key)
	if err != nil {
		return
	}
	frame = &FrameItem{}
	frame.TableID, frame.IndexID, frame.IsRecord, err = tablecodec.DecodeKeyHead(key)
	if err == nil || bytes.HasPrefix(key, tablecodec.TablePrefix()) {
		return
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

// getFirstIdxIdRange return the first table's index range.
// 1. [start,end] means index id in [start,end] are needed,while record key is not in.
// 2. [start,~) means index's id in [start,~) are needed including record key index.
// 3. (~,~) means only record key index is needed
func (ir *RegionFrameRange) getFirstIdxIDRange() (start, end int64) {
	start = int64(math.MinInt64)
	end = int64(math.MaxInt64)
	if ir.first.IsRecord {
		start = end // need record key only,
		return
	}

	start = ir.first.IndexID
	if ir.first.TableID != ir.last.TableID || ir.last.IsRecord {
		return // [start,~)
	}
	end = ir.last.IndexID // [start,end]
	return
}

// getLastInxIdRange return the last table's index range.
// (~,end] means index's id in (~,end] are legal, record key index not included.
// (~,~) means all indexes are legal include record key index.
func (ir *RegionFrameRange) getLastInxIDRange() (start, end int64) {
	start = int64(math.MinInt64)
	end = int64(math.MaxInt64)
	if ir.last.IsRecord {
		return
	}
	end = ir.last.IndexID
	return
}

// getIndexRangeForTable return the legal index range for table with tableID.
// end=math.MaxInt64 means record key index is included.
func (ir *RegionFrameRange) getIndexRangeForTable(tableID int64) (start, end int64) {
	switch tableID {
	case ir.firstTableID():
		return ir.getFirstIdxIDRange()
	case ir.lastTableID():
		return ir.getLastInxIDRange()
	default:
		if tableID < ir.lastTableID() && tableID > ir.firstTableID() {
			return int64(math.MinInt64), int64(math.MaxInt64)
		}
	}
	return int64(math.MaxInt64), int64(math.MinInt64)
}

func (ir RegionFrameRange) firstTableID() int64 {
	return ir.first.TableID
}

func (ir RegionFrameRange) lastTableID() int64 {
	return ir.last.TableID
}

// ServeHTTP handles request of list a table's regions.
func (rh mvccTxnHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var data interface{}
	params := mux.Vars(req)
	var err error
	if rh.op == opMvccGetByKey {
		data, err = rh.handleMvccGetByKey(params)
	} else {
		data, err = rh.handleMvccGetByTxn(params)
	}
	if err != nil {
		rh.writeError(w, err)
	} else {
		rh.writeData(w, data)
	}
}

func (rh mvccTxnHandler) handleMvccGetByKey(params map[string]string) (interface{}, error) {
	recordID, err := strconv.ParseInt(params[pRecordID], 0, 64)
	if err != nil {
		return nil, err
	}

	tableID, err := rh.getTableID(params[pDBName], params[pTableName])
	if err != nil {
		return nil, err
	}
	return rh.getMvccByRecordID(tableID, recordID)
}

func (rh *mvccTxnHandler) handleMvccGetByTxn(params map[string]string) (interface{}, error) {
	startTS, err := strconv.ParseInt(params[pStartTS], 0, 64)
	if err != nil {
		return nil, err
	}

	startKey := []byte("")
	endKey := []byte("")
	dbName := params[pDBName]
	if len(dbName) > 0 {
		tableID, err := rh.getTableID(params[pDBName], params[pTableName])
		if err != nil {
			return nil, err
		}
		startKey = tablecodec.EncodeTablePrefix(tableID)
		endKey = tablecodec.EncodeRowKeyWithHandle(tableID, math.MaxInt64)
	}
	return rh.getMvccByStartTs(uint64(startTS), startKey, endKey)
}

func (t *regionHandlerTool) getMvccByRecordID(tableID, recordID int64) (*kvrpcpb.MvccGetByKeyResponse, error) {
	encodeKey := tablecodec.EncodeRowKeyWithHandle(tableID, recordID)
	keyLocation, err := t.regionCache.LocateKey(t.bo, encodeKey)
	if err != nil {
		return nil, err
	}

	tikvReq := &tikvrpc.Request{
		Type:     tikvrpc.CmdMvccGetByKey,
		Priority: kvrpcpb.CommandPri_Normal,
		MvccGetByKey: &kvrpcpb.MvccGetByKeyRequest{
			Key: encodeKey,
		},
	}
	kvResp, err := t.store.SendReq(t.bo, tikvReq, keyLocation.Region, time.Minute)
	log.Info(string(encodeKey), keyLocation.Region, string(keyLocation.StartKey), string(keyLocation.EndKey), kvResp, err)

	if err != nil {
		return nil, err
	}
	return kvResp.MvccGetByKey, nil
}

func (t *regionHandlerTool) getMvccByStartTs(startTS uint64, startKey, endKey []byte) (*kvrpcpb.MvccGetByStartTsResponse, error) {
	for {
		curRegion, err := t.regionCache.LocateKey(t.bo, startKey)
		if err != nil {
			log.Error(startTS, startKey, err)
			return nil, errors.Trace(err)
		}

		tikvReq := &tikvrpc.Request{
			Type:     tikvrpc.CmdMvccGetByStartTs,
			Priority: kvrpcpb.CommandPri_Low,
			MvccGetByStartTs: &kvrpcpb.MvccGetByStartTsRequest{
				StartTs: startTS,
			},
		}
		kvResp, err := t.store.SendReq(t.bo, tikvReq, curRegion.Region, time.Hour)
		log.Info(startTS, string(startKey), curRegion.Region, string(curRegion.StartKey), string(curRegion.EndKey), kvResp)
		if err != nil {
			log.Error(startTS, string(startKey), curRegion.Region, string(curRegion.StartKey), string(curRegion.EndKey), err)
			return nil, err
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

func (t *regionHandlerTool) getTableID(dbName, tableName string) (int64, error) {
	schema, err := t.schema()
	if err != nil {
		return 0, err
	}
	table, err := schema.TableByName(model.NewCIStr(dbName), model.NewCIStr(tableName))
	if err != nil {
		return 0, err
	}
	return table.Meta().ID, nil
}

func (t *regionHandlerTool) schema() (infoschema.InfoSchema, error) {
	session, err := tidb.CreateSession(t.store.(kv.Storage))
	if err != nil {
		err = errors.Trace(err)
		return nil, err
	}
	return sessionctx.GetDomain(session.(context.Context)).InfoSchema(), nil
}
