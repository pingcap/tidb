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
	"time"

	"github.com/gorilla/mux"
	"github.com/juju/errors"
	"github.com/pingcap/pd/pd-client"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	goctx "golang.org/x/net/context"
)

// RegionsHTTPRequest is for regions http request.
type RegionsHTTPRequest struct {
	resp        http.ResponseWriter
	req         *http.Request
	server      *Server
	params      map[string]string
	reqID       string
	regionCache *tikv.RegionCache
	bo          *tikv.Backoffer
	domain      *domain.Domain
}

// NewRegionsHTTPRequest create a RegionsHTTPRequest
func NewRegionsHTTPRequest(s *Server, w http.ResponseWriter, req *http.Request,
	pdClient *pd.Client) (regionReq *RegionsHTTPRequest, err error) {
	regionReq = &RegionsHTTPRequest{
		server: s,
		req:    req,
		resp:   w,
		params: mux.Vars(req),
		//TODO genuuid
		reqID: fmt.Sprintf("http_%d", time.Now().UnixNano()),
	}
	err = regionReq.init(pdClient)
	return regionReq, errors.Trace(err)
}

func (req *RegionsHTTPRequest) writeResponse(data interface{}, err error) {
	var retItem *RegionsResponse
	if err != nil {
		retItem = NewRegionsResponseWithError(req.reqID, err)
	} else {
		retItem = NewRegionsResponseWithData(req.reqID, data)
	}
	js, err := json.Marshal(retItem)
	if err != nil {
		req.resp.WriteHeader(http.StatusInternalServerError)
		req.resp.Write([]byte(err.Error()))
		return
	}

	req.resp.Header().Set("Content-Type", "application/json")
	if !retItem.Success {
		req.resp.WriteHeader(http.StatusBadRequest)
	} else {
		req.resp.WriteHeader(http.StatusOK)
	}
	req.resp.Write(js)
}

// HandleListRegions handles http request for list regions.
func (req *RegionsHTTPRequest) HandleListRegions() {
	dbName, _ := req.params["db"]
	tableName, _ := req.params["table"]
	data, err := req.listTableRegions(dbName, tableName)
	req.writeResponse(data, err)
}

// HandleGetRegionByID handles request for get region by ID.
func (req *RegionsHTTPRequest) HandleGetRegionByID() {
	regionIDStr, _ := req.params["regionID"]
	regionID, err := strconv.ParseInt(regionIDStr, 0, 64)
	if err != nil {
		req.writeResponse(nil, err)
		return
	}
	data, err := req.getRegionByID(uint64(regionID))
	req.writeResponse(data, err)
}

// init check and creates a Backoffer, Domain when store is tikv.
func (req *RegionsHTTPRequest) init(pdClient *pd.Client) (err error) {
	defer func() {
		if err != nil {
			req.writeResponse(nil, err)
		}
	}()
	if req.server.cfg.Store != "tikv" {
		err = fmt.Errorf("only store tikv support,current store:%s", req.server.cfg.Store)
		return
	}
	if pdClient == nil {
		err = fmt.Errorf("Invalid PdClient: nil")
		return
	}

	req.regionCache = tikv.NewRegionCache(*pdClient)
	var session tidb.Session
	session, err = tidb.CreateSession(req.server.driver.(*TiDBDriver).store)
	if err != nil {
		return
	}
	req.bo = tikv.NewBackoffer(5000, goctx.Background())
	req.domain = sessionctx.GetDomain(session.(context.Context))
	return err
}

func (req *RegionsHTTPRequest) listTableRegions(dbName, tableName string) (tableRegions *TableRegions, err error) {
	table, err := req.domain.InfoSchema().TableByName(model.NewCIStr(dbName), model.NewCIStr(tableName))
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableID := table.Meta().ID

	tableRegions = &TableRegions{
		TableName: tableName,
		TableID:   tableID,
		Indices:   make([]IndexRegions, len(table.Indices())),
	}

	// for primary
	startKey, endKey := getTableHandleKeyRange(tableID)
	tableRegions.RowRegions, err = req.regionCache.ListRegionIDsInKeyRange(req.bo, startKey, endKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// for indexes
	for i, index := range table.Indices() {
		indexID := index.Meta().ID
		tableRegions.Indices[i].Name = index.Meta().Name.String()
		tableRegions.Indices[i].ID = indexID
		startKey, endKey := getTableIndexKeyRange(tableID, indexID)
		tableRegions.Indices[i].Regions, err = req.regionCache.ListRegionIDsInKeyRange(req.bo, startKey, endKey)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return tableRegions, nil
}

func (req *RegionsHTTPRequest) getRegionByID(regionID uint64) (*RegionItem, error) {
	region, err := req.regionCache.LocateRegionByID(req.bo, regionID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	indexRange := NewRegionIndexRange(region)

	regionItem := &RegionItem{
		RegionID: regionID,
		StartKey: region.StartKey,
		EndKey:   region.EndKey,
		Indices:  []IndexItem{},
	}

	// table's number is smaller than 1000, iterate table ID in index range.
	if indexRange.lastTableID()-indexRange.firstTableID() <= 1000 {
		for tableID := indexRange.firstTableID(); tableID <= indexRange.lastTableID(); tableID++ {
			curTable, exist := req.domain.InfoSchema().TableByID(tableID)
			if exist {
				start, end := indexRange.getLastInxIDRange()
				regionItem.addTableIndicesInRange(curTable, start, end)
			}
		}
		return regionItem, nil
	}

	for _, db := range req.domain.InfoSchema().AllSchemaNames() {
		for _, table := range req.domain.InfoSchema().SchemaTables(model.NewCIStr(db)) {
			curTable, exist := req.domain.InfoSchema().TableByID(table.Meta().ID)
			if exist {
				start, end := indexRange.getLastInxIDRange()
				regionItem.addTableIndicesInRange(curTable, start, end)
			}
		}
	}
	return regionItem, nil
}

// RegionsResponse is the response struct for regions.
type RegionsResponse struct {
	Success   bool        `json:"status"`
	Msg       string      `json:"message,omitempty"`
	RequestID string      `json:"request_id"`
	Data      interface{} `json:"data,omitempty"`
}

// NewRegionsResponseWithError create a response item with error
func NewRegionsResponseWithError(reqID string, err error) *RegionsResponse {
	return &RegionsResponse{
		Success:   false,
		Msg:       err.Error(),
		RequestID: reqID,
	}
}

// NewRegionsResponseWithData create a response item with data
func NewRegionsResponseWithData(reqID string, data interface{}) *RegionsResponse {
	return &RegionsResponse{
		Success:   true,
		RequestID: reqID,
		Data:      data,
	}
}

func getTableHandleKeyRange(tableID int64) (startKey, endKey []byte) {
	tableStartKey := tablecodec.EncodeRowKeyWithHandle(tableID, math.MinInt64)
	tableEndKey := tablecodec.EncodeRowKeyWithHandle(tableID, math.MaxInt64)
	startKey = codec.EncodeBytes(nil, tableStartKey)
	endKey = codec.EncodeBytes(nil, tableEndKey)
	return
}

func getTableIndexKeyRange(tableID, indexID int64) (startKey, endKey []byte) {
	start := tablecodec.EncodeIndexSeekKey(tableID, indexID, nil)
	end := tablecodec.EncodeIndexSeekKey(tableID, indexID, []byte{255})
	startKey = codec.EncodeBytes(nil, start)
	endKey = codec.EncodeBytes(nil, end)
	return
}

// IndexInfo include a index of table,include tableID,
// 	indexID,and whether is a record key.
type IndexInfo struct {
	indexID     int64
	isRecordKey bool
	tableID     int64
}

// NewIndexInfoFromKey creates a IndexInfo with key,returns err when key is illegal.
func NewIndexInfoFromKey(key []byte) (info *IndexInfo, err error) {
	_, key, err = codec.DecodeBytes(key)
	if err != nil {
		return
	}
	info = &IndexInfo{}
	info.tableID, info.indexID, info.isRecordKey, err = tablecodec.DecodeKeyHead(key)
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
		info.tableID = math.MinInt64
		info.indexID = math.MinInt64
		info.isRecordKey = false
		return
	}

	info.tableID = math.MaxInt64
	info.tableID = math.MaxInt64
	info.isRecordKey = true
	return
}

// RegionIndexRange contains index's range for a region.
type RegionIndexRange struct {
	first  *IndexInfo
	last   *IndexInfo
	region *tikv.KeyLocation
}

// NewRegionIndexRange init a RegionIndexRange with region info.
func NewRegionIndexRange(region *tikv.KeyLocation) *RegionIndexRange {
	idxRange := &RegionIndexRange{
		region: region,
	}
	idxRange.initFirstIndexRange()
	idxRange.initLastIndexRange()
	return idxRange
}

func (ir *RegionIndexRange) initFirstIndexRange() {
	// try to parse from start_key
	first, err := NewIndexInfoFromKey(ir.region.StartKey)
	if err == nil {
		ir.first = first
		return
	}

	// if start_key start with tablePrefix but is
	// not a legal key ,try to binary search first table
	ir.first = &IndexInfo{
		tableID:     int64(math.MaxInt64),
		indexID:     int64(math.MaxInt64),
		isRecordKey: false,
	}
	left, right := int64(0), int64(math.MaxInt64)
	// init table
	for left < right {
		mid := left>>1 + right>>1
		prefix := codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(mid))
		if ir.region.Contains(prefix) ||
			bytes.Compare(ir.region.EndKey, prefix) < 0 {
			right = mid - 1
		} else {
			left = mid
		}
	}
	tableID := right
	prefix := codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(tableID))
	if !ir.region.Contains(prefix) {
		tableID++
		prefix = codec.EncodeBytes(nil, tablecodec.EncodeTablePrefix(tableID))
	}

	if !ir.region.Contains(prefix) {
		ir.first.tableID = int64(math.MaxInt64)
		return // no table in current region
	}
	ir.first.tableID = tableID
	// we won't binary search for index id
	// since if a region start with key which is not index,
	// means the first table's front part was in the region.
	ir.first.indexID = int64(math.MinInt64)
	ir.first.isRecordKey = false
}

func (ir *RegionIndexRange) initLastIndexRange() {
	// try to parse from end_key
	last, err := NewIndexInfoFromKey(ir.region.EndKey)
	if err == nil {
		ir.last = last
		return
	}
	// if end_key start with tablePrefix but
	// is not a legal index,binary search end index
	ir.last = &IndexInfo{
		tableID:     int64(math.MinInt64),
		indexID:     int64(math.MinInt64),
		isRecordKey: false,
	}

	left, right := int64(0), int64(math.MaxInt64)
	// init table
	for left < right {
		mid := left>>1 + right>>1
		prefix := codec.EncodeBytes(nil, tablecodec.EncodeRowKeyWithHandle(mid, int64(math.MaxInt64)))
		if ir.region.Contains(prefix) || bytes.Compare(ir.region.StartKey, prefix) > 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}
	tableID := left
	prefix := codec.EncodeBytes(nil, tablecodec.EncodeRowKeyWithHandle(tableID, int64(math.MaxInt64)))
	if !ir.region.Contains(prefix) {
		tableID--
		prefix = tablecodec.EncodeRowKeyWithHandle(tableID, int64(math.MaxInt64))
	}

	if !ir.region.Contains(prefix) { // no table found
		ir.last.tableID = int64(math.MinInt64)
		return
	}

	ir.last.tableID = tableID
	// we won't binary search for last index id
	// since if a region end with key which is not index,
	// means the last table's last part was already in the region.
	ir.last.indexID = int64(math.MaxInt64)
	ir.last.isRecordKey = true
	return
}

// getFirstIdxIdRange return the first table's index range.
// 1. [start,end] means index id in [start,end] are needed,while record key is not in.
// 2. [start,~) means index's id in [start,~) are needed including record key index.
// 3. [~,~] means only record key index is needed
func (ir *RegionIndexRange) getFirstIdxIDRange() (start, end int64) {
	start = int64(math.MinInt64)
	end = int64(math.MaxInt64)
	if ir.first.isRecordKey {
		start = end // need record key only,
		return
	}

	start = ir.first.indexID
	if ir.first.tableID != ir.last.tableID || ir.last.isRecordKey {
		return // [start,~)
	}
	end = ir.last.indexID // [start,end]
	return
}

// getLastInxIdRange return the last table's index range.
// (~,end] means index's id in (~,end] are legal, record key index not included.
// (~,~) means all indexes are legal include record key index.
func (ir *RegionIndexRange) getLastInxIDRange() (start, end int64) {
	start = int64(math.MinInt64)
	end = int64(math.MaxInt64)
	if ir.last.isRecordKey {
		return
	}
	end = ir.last.indexID
	return
}

// getIndexRangeForTable return the legal index range for table with tableID.
// end=math.MaxInt64 means record key index is included.
func (ir *RegionIndexRange) getIndexRangeForTable(tableID int64) (start, end int64) {
	start = int64(math.MinInt64)
	end = int64(math.MaxInt64)
	switch tableID {
	case ir.firstTableID():
		return ir.getFirstIdxIDRange()
	case ir.lastTableID():
		return ir.getLastInxIDRange()
	default:
		return
	}
}

func (ir RegionIndexRange) firstTableID() int64 {
	return ir.first.tableID
}

func (ir RegionIndexRange) lastTableID() int64 {
	return ir.last.tableID
}

// IndexRegions is the region info for one index.
type IndexRegions struct {
	Name    string   `json:"name"`
	ID      int64    `json:"id"`
	Regions []uint64 `json:"regions"`
}

// TableRegions is the region info for one table.
type TableRegions struct {
	TableName  string         `json:"name"`
	TableID    int64          `json:"id"`
	RowRegions []uint64       `json:"row_regions"`
	Indices    []IndexRegions `json:"indices"`
}

// RegionItem is the response data for get region by ID
// it includes indices detail in current region.
type RegionItem struct {
	RegionID uint64      `json:"region_id"`
	StartKey []byte      `json:"start_key"`
	EndKey   []byte      `json:"end_key"`
	Indices  []IndexItem `json:"indices"`
}

func (rt *RegionItem) addIndex(tName string, tID int64, indexName string, indexID int64) {
	rt.Indices = append(rt.Indices, IndexItem{
		TableName: tName,
		TableID:   tID,
		IndexName: indexName,
		IndexID:   indexID,
	})
}

func (rt *RegionItem) addTableIndicesInRange(curTable table.Table, startID, endID int64) {
	tName := curTable.Meta().Name.String()
	tID := curTable.Meta().ID
	for _, index := range curTable.Indices() {
		if index.Meta().ID >= startID && index.Meta().ID <= endID {
			rt.addIndex(tName,
				tID,
				index.Meta().Name.String(),
				index.Meta().ID)
		}
	}
	if endID == math.MaxInt64 {
		rt.addIndex(tName, tID, "", 0)
	}
}

// IndexItem includes a index's meta data which includes table's info.
type IndexItem struct {
	TableName string `json:"table_name"`
	TableID   int64  `json:"table_id"`
	IndexName string `json:"index_name"`
	IndexID   int64  `json:"index_id"`
}
