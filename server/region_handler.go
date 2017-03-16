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
	"fmt"
	"math"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/pd-client"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	goctx "golang.org/x/net/context"
)

const (
	pDBName    = "db"
	pTableName = "table"
	pRegionID  = "regionID"
)

const (
	headerContentType = "Content-Type"
	contentTypeJSON   = "application/json"
)

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

// RegionHandler is the common field for http region handler. It contains
// some common functions which would be used in TableRegionsHandler and
// HTTPGetRegionByIDHandler.
type RegionHandler struct {
	pdClient pd.Client
	server   *Server
}

// TableRegionsHandler is the handler for list table's regions.
type TableRegionsHandler struct {
	RegionHandler
}

// regionHandlerTool contains some common tool which
// would be used in each RegionHandler request.
type regionHandlerTool struct {
	bo          *tikv.Backoffer
	infoSchema  infoschema.InfoSchema
	regionCache *tikv.RegionCache
}

func (s *Server) newRegionHandler(pdClient pd.Client) RegionHandler {
	return RegionHandler{
		pdClient: pdClient,
		server:   s,
	}
}

func (s *Server) newTableRegionsHandler(pdClient pd.Client) http.Handler {
	return TableRegionsHandler{
		s.newRegionHandler(pdClient),
	}
}

func (rh TableRegionsHandler) getRegionsMeta(regionIDs []uint64) ([]RegionMeta, error) {
	regions := make([]RegionMeta, len(regionIDs))
	for i, regionID := range regionIDs {
		meta, leader, err := rh.pdClient.GetRegionByID(regionID)
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
func (rh TableRegionsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// parse params
	params := mux.Vars(req)
	dbName := params[pDBName]
	tableName := params[pTableName]
	// check and prepare tools
	tool, err := rh.prepare()
	if err != nil {
		rh.writeError(w, err)
		return
	}
	// get table's schema.
	table, err := tool.infoSchema.TableByName(model.NewCIStr(dbName), model.NewCIStr(tableName))
	if err != nil {
		rh.writeError(w, err)
		return
	}
	tableID := table.Meta().ID

	// for record
	startKey, endKey := tablecodec.GetTableHandleKeyRange(tableID)
	recordRegionIDs, err := tool.regionCache.ListRegionIDsInKeyRange(tool.bo, startKey, endKey)
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
		rIDs, err := tool.regionCache.ListRegionIDsInKeyRange(tool.bo, startKey, endKey)
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
func (rh RegionHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// parse and check params
	params := mux.Vars(req)
	regionIDInt, err := strconv.ParseInt(params[pRegionID], 0, 64)
	if err != nil {
		rh.writeError(w, err)
		return
	}
	regionID := uint64(regionIDInt)

	// check and prepare tools
	tool, err := rh.prepare()
	if err != nil {
		rh.writeError(w, err)
		return
	}

	// locate region
	region, err := tool.regionCache.LocateRegionByID(tool.bo, regionID)
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
	// Since we need a database's name for each frame, and a table's database name can not
	// get from table's ID directly. Above all, here do dot process like
	// 		`for id in [frameRange.firstTableID,frameRange.endTableID]`
	// on [frameRange.firstTableID,frameRange.endTableID] is small enough.
	for _, db := range tool.infoSchema.AllSchemas() {
		for _, table := range db.Tables {
			start, end := frameRange.getIndexRangeForTable(table.ID)
			regionDetail.addTableInRange(db.Name.String(), table, start, end)
		}
	}
	rh.writeData(w, regionDetail)
}

// prepare checks and prepares for region request. It returns
// regionHandlerTool on success while return an err on any
// error happens.
func (rh *RegionHandler) prepare() (tool *regionHandlerTool, err error) {
	// check store
	if rh.server.cfg.Store != "tikv" {
		err = fmt.Errorf("only store tikv support,current store:%s", rh.server.cfg.Store)
		return
	}

	// create regionCache.
	regionCache := tikv.NewRegionCache(rh.pdClient)
	var session tidb.Session
	session, err = tidb.CreateSession(rh.server.driver.(*TiDBDriver).store)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	// init backOffer && infoSchema.
	backOffer := tikv.NewBackoffer(500, goctx.Background())
	infoSchema := sessionctx.GetDomain(session.(context.Context)).InfoSchema()

	tool = &regionHandlerTool{
		regionCache: regionCache,
		bo:          backOffer,
		infoSchema:  infoSchema,
	}
	return
}

func (rh *RegionHandler) writeError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	w.Write([]byte(err.Error()))
}

func (rh *RegionHandler) writeData(w http.ResponseWriter, data interface{}) {
	js, err := json.Marshal(data)
	if err != nil {
		rh.writeError(w, err)
		return
	}
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
