package server

import (
	"encoding/json"
	"fmt"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/tablecodec"
	gContext "golang.org/x/net/context"
	"math"
	"net/http"
	"strings"
	"time"
)

// HTTPResponseItem is the response struct for http
type HTTPResponseItem struct {
	Success   bool        `json:"status"`
	Msg       string      `json:"message"`
	RequestID string      `json:"request_id"`
	Data      interface{} `json:"data,omitempty"`
}

// RegionItem is the regions info for one index
type RegionItem struct {
	TableName string
	TableID   int64
	IndexName string
	IndexID   int64
	Regions   []uint64
}

func (s *Server) listTableRegions(dbName, tableName string) (data []RegionItem, err error) {
	if s.cfg.Store != "tikv" {
		return nil, fmt.Errorf("only store tikv support,current store:%s", s.cfg.Store)
	}
	session, err := tidb.CreateSession(s.driver.(*TiDBDriver).store)
	if err != nil {
		return data, err
	}
	dom := sessionctx.GetDomain(session.(context.Context))
	table, err := dom.InfoSchema().TableByName(model.NewCIStr(dbName), model.NewCIStr(tableName))
	if err != nil {
		return data,err
	}
	tableID := table.Meta().ID

	// create regionCache
	storePath := fmt.Sprintf("%s://%s", s.cfg.Store, s.cfg.StorePath)
	regionCache, err := tikv.NewRegionCacheFromStorePath(storePath)
	if err != nil {
		return data, err
	}
	bo := tikv.NewBackoffer(5000, gContext.Background())

	// for primary
	tableStartKey := tablecodec.EncodeRowKeyWithHandle(tableID, math.MinInt64)
	tableEndKey := tablecodec.EncodeRowKeyWithHandle(tableID, math.MaxInt64)

	regions, err := regionCache.ListRegionIDsInKeyRange(bo, tableStartKey, tableEndKey)
	if err != nil {
		return data, err
	}
	data = make([]RegionItem, len(table.Indices())+1, len(table.Indices())+1)
	data[0] = RegionItem{
		TableName: table.Meta().Name.String(),
		TableID:   table.Meta().ID,
		IndexName: "primary",
		IndexID:   0,
		Regions:   regions,
	}

	// for indexes
	offset := 1
	for _, index := range table.Indices() {
		startKey := tablecodec.EncodeIndexSeekKey(tableID, index.Meta().ID, nil)
		endKey := tablecodec.EncodeIndexSeekKey(tableID, index.Meta().ID, []byte{255})
		regions, err := regionCache.ListRegionIDsInKeyRange(bo, startKey, endKey)
		if err != nil {
			return nil, err
		}
		data[offset] = RegionItem{
			TableName: tableName,
			TableID:   tableID,
			IndexName: index.Meta().Name.String(),
			IndexID:   index.Meta().ID,
			Regions:   regions,
		}
		offset++
	}

	return data, nil
}

func (s *Server) handle(w http.ResponseWriter, req *http.Request) {
	path := strings.Trim(req.URL.Path, "/")
	data := HTTPResponseItem{
		Success:   false,
		Msg:       "",
		RequestID: fmt.Sprintf("http_%d", time.Now().UnixNano()), //TODO genuuid
		Data:      nil,
	}
	params := strings.Split(path, "/")
	if len(params) >= 3 && strings.HasPrefix(path, "tables") {
		// /tables/${db}/${table}
		curData, err := s.listTableRegions(params[1], params[2])
		if err != nil {
			data.Msg = err.Error()
		} else {
			data.Data = curData
			data.Success = true
		}

	} else if len(params) >= 2 && strings.HasPrefix(path, "regions") {
		// /regions/${region_id}
		// TODO
		data.Success = true

	} else {
		data.Msg = "Illegal Request:" + path
	}

	w.Header().Set("Content-Type", "application/json")

	js, err := json.Marshal(data)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Error("Encode json error", err)
	} else {
		w.Write(js)
	}

}
