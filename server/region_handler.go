// Copyright 2016 PingCAP, Inc.
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
	"encoding/json"
	"fmt"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	gContext "golang.org/x/net/context"
	"math"
	"net/http"
	"strings"
	"time"
)

// RegionsHTTPRequest is for regions http request.
type RegionsHTTPRequest struct {
	resp    http.ResponseWriter
	req     *http.Request
	server  *Server
	path    string
	retData *RegionsResponse
}

func (req *RegionsHTTPRequest) showResponse() {
	js, err := json.Marshal(req.retData)
	if err != nil {
		req.resp.WriteHeader(http.StatusInternalServerError)
		req.resp.Write([]byte(err.Error()))
		return
	}

	req.resp.Header().Set("Content-Type", "application/json")
	if !req.retData.Success {
		req.resp.WriteHeader(http.StatusBadRequest)
	} else {
		req.resp.WriteHeader(http.StatusOK)
	}
	req.resp.Write(js)
}

// handleListTableRegions lists regions info for table.
func (req *RegionsHTTPRequest) handleListTableRegions() {
	// tables/${dbname}/{table}
	params := strings.Split(req.path, "/")
	if len(params) < 3 {
		req.showInvalidPath()
		return
	}
	data, err := req.server.listTableRegions(params[1], params[2])
	if err != nil {
		req.retData.SetError(err.Error())
	} else {
		req.retData.SetData(data)
	}
	req.showResponse()
}

func (req *RegionsHTTPRequest) showInvalidPath() {
	req.retData.SetError("InvalidPath")
	req.showResponse()
}

// Handle handles http request for regions.
func (req *RegionsHTTPRequest) Handle() {
	if strings.HasPrefix(req.path, "tables") {
		// /tables/${database_name}/${table_name}
		req.handleListTableRegions()
		return
	} else if strings.HasPrefix(req.path, "regions") {
		// /regions/${region_id}
		// TODO
	}
	req.showInvalidPath()
}

// RegionsResponse is the response struct for regions.
type RegionsResponse struct {
	Success   bool        `json:"status"`
	Msg       string      `json:"message,omitempty"`
	RequestID string      `json:"request_id"`
	Data      interface{} `json:"data,omitempty"`
	Path      string      `json:"path"`
}

// NewRegionsResponse create a response for regions.
func NewRegionsResponse(path string) *RegionsResponse {
	return &RegionsResponse{
		Success: false,
		Msg:     "",
		//TODO genuuid
		RequestID: fmt.Sprintf("http_%d", time.Now().UnixNano()),
		Data:      nil,
		Path:      path,
	}
}

// SetError sets error for response.
func (res *RegionsResponse) SetError(errMsg string) {
	res.Msg = errMsg
	res.Success = false
}

// SetData sets data for response.
func (res *RegionsResponse) SetData(data interface{}) {
	res.Success = true
	res.Msg = ""
	res.Data = data
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

func (s *Server) onlyStoreTIKVSupported() error {
	if s.cfg.Store != "tikv" {
		return fmt.Errorf("only store tikv support,current store:%s", s.cfg.Store)
	}
	return nil
}

func (s *Server) listTableRegions(dbName, tableName string) (data *TableRegions, err error) {
	if err = s.onlyStoreTIKVSupported(); err != nil {
		return
	}
	// prepare table structure
	session, err := tidb.CreateSession(s.driver.(*TiDBDriver).store)
	if err != nil {
		return nil, err
	}
	dom := sessionctx.GetDomain(session.(context.Context))
	table, err := dom.InfoSchema().TableByName(model.NewCIStr(dbName), model.NewCIStr(tableName))
	if err != nil {
		return nil, err
	}
	tableID := table.Meta().ID

	// create regionCache
	storePath := fmt.Sprintf("%s://%s", s.cfg.Store, s.cfg.StorePath)
	regionCache, err := tikv.NewRegionCacheFromStorePath(storePath)
	if err != nil {
		return nil, err
	}
	bo := tikv.NewBackoffer(5000, gContext.Background())

	data = &TableRegions{
		TableName:  tableName,
		TableID:    tableID,
		RowRegions: nil,
		Indices:    make([]IndexRegions, len(table.Indices()), len(table.Indices())),
	}

	// for primary
	startKey, endKey := getTableHandleKeyRange(tableID)
	data.RowRegions, err = regionCache.ListRegionIDsInKeyRange(bo, startKey, endKey)
	if err != nil {
		return nil, err
	}

	// for indexes
	for id, index := range table.Indices() {
		indexID := index.Meta().ID
		data.Indices[id].Name = index.Meta().Name.String()
		data.Indices[id].ID = indexID
		startKey, endKey := getTableIndexKeyRange(tableID, indexID)
		data.Indices[id].Regions, err = regionCache.ListRegionIDsInKeyRange(bo, startKey, endKey)
		if err != nil {
			return nil, err
		}
	}
	return data, nil
}
