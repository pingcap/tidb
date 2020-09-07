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
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/gcworker"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/pdapi"
	log "github.com/sirupsen/logrus"
	"go.uber.org/zap"
)

const (
	pDBName     = "db"
	pHexKey     = "hexKey"
	pIndexName  = "index"
	pHandle     = "handle"
	pRegionID   = "regionID"
	pStartTS    = "startTS"
	pTableName  = "table"
	pTableID    = "tableID"
	pColumnID   = "colID"
	pColumnTp   = "colTp"
	pColumnFlag = "colFlag"
	pColumnLen  = "colLen"
	pRowBin     = "rowBin"
	pSnapshot   = "snapshot"
)

// For query string
const (
	qTableID   = "table_id"
	qLimit     = "limit"
	qOperation = "op"
	qSeconds   = "seconds"
)

const (
	headerContentType = "Content-Type"
	contentTypeJSON   = "application/json"
)

func writeError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	_, err = w.Write([]byte(err.Error()))
	terror.Log(errors.Trace(err))
}

func writeData(w http.ResponseWriter, data interface{}) {
	js, err := json.MarshalIndent(data, "", " ")
	if err != nil {
		writeError(w, err)
		return
	}
	// write response
	w.Header().Set(headerContentType, contentTypeJSON)
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(js)
	terror.Log(errors.Trace(err))
}

type tikvHandlerTool struct {
	helper.Helper
}

// newTikvHandlerTool checks and prepares for tikv handler.
// It would panic when any error happens.
func (s *Server) newTikvHandlerTool() *tikvHandlerTool {
	var tikvStore tikv.Storage
	store, ok := s.driver.(*TiDBDriver)
	if !ok {
		panic("Invalid KvStore with illegal driver")
	}

	if tikvStore, ok = store.store.(tikv.Storage); !ok {
		panic("Invalid KvStore with illegal store")
	}

	regionCache := tikvStore.GetRegionCache()

	return &tikvHandlerTool{
		helper.Helper{
			RegionCache: regionCache,
			Store:       tikvStore,
		},
	}
}

type mvccKV struct {
	Key      string                        `json:"key"`
	RegionID uint64                        `json:"region_id"`
	Value    *kvrpcpb.MvccGetByKeyResponse `json:"value"`
}

func (t *tikvHandlerTool) getRegionIDByKey(encodedKey []byte) (uint64, error) {
	keyLocation, err := t.RegionCache.LocateKey(tikv.NewBackofferWithVars(context.Background(), 500, nil), encodedKey)
	if err != nil {
		return 0, err
	}
	return keyLocation.Region.GetID(), nil
}

func (t *tikvHandlerTool) getMvccByHandle(tableID, handle int64) (*mvccKV, error) {
	encodedKey := tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(handle))
	data, err := t.GetMvccByEncodedKey(encodedKey)
	if err != nil {
		return nil, err
	}
	regionID, err := t.getRegionIDByKey(encodedKey)
	if err != nil {
		return nil, err
	}
	return &mvccKV{Key: strings.ToUpper(hex.EncodeToString(encodedKey)), Value: data, RegionID: regionID}, err
}

func (t *tikvHandlerTool) getMvccByStartTs(startTS uint64, startKey, endKey kv.Key) (*mvccKV, error) {
	bo := tikv.NewBackofferWithVars(context.Background(), 5000, nil)
	for {
		curRegion, err := t.RegionCache.LocateKey(bo, startKey)
		if err != nil {
			logutil.BgLogger().Error("get MVCC by startTS failed", zap.Uint64("txnStartTS", startTS),
				zap.Stringer("startKey", startKey), zap.Error(err))
			return nil, errors.Trace(err)
		}

		tikvReq := tikvrpc.NewRequest(tikvrpc.CmdMvccGetByStartTs, &kvrpcpb.MvccGetByStartTsRequest{
			StartTs: startTS,
		})
		tikvReq.Context.Priority = kvrpcpb.CommandPri_Low
		kvResp, err := t.Store.SendReq(bo, tikvReq, curRegion.Region, time.Hour)
		if err != nil {
			logutil.BgLogger().Error("get MVCC by startTS failed",
				zap.Uint64("txnStartTS", startTS),
				zap.Stringer("startKey", startKey),
				zap.Reflect("region", curRegion.Region),
				zap.Stringer("curRegion startKey", curRegion.StartKey),
				zap.Stringer("curRegion endKey", curRegion.EndKey),
				zap.Reflect("kvResp", kvResp),
				zap.Error(err))
			return nil, errors.Trace(err)
		}
		data := kvResp.Resp.(*kvrpcpb.MvccGetByStartTsResponse)
		if err := data.GetRegionError(); err != nil {
			logutil.BgLogger().Warn("get MVCC by startTS failed",
				zap.Uint64("txnStartTS", startTS),
				zap.Stringer("startKey", startKey),
				zap.Reflect("region", curRegion.Region),
				zap.Stringer("curRegion startKey", curRegion.StartKey),
				zap.Stringer("curRegion endKey", curRegion.EndKey),
				zap.Reflect("kvResp", kvResp),
				zap.Stringer("error", err))
			continue
		}

		if len(data.GetError()) > 0 {
			logutil.BgLogger().Error("get MVCC by startTS failed",
				zap.Uint64("txnStartTS", startTS),
				zap.Stringer("startKey", startKey),
				zap.Reflect("region", curRegion.Region),
				zap.Stringer("curRegion startKey", curRegion.StartKey),
				zap.Stringer("curRegion endKey", curRegion.EndKey),
				zap.Reflect("kvResp", kvResp),
				zap.String("error", data.GetError()))
			return nil, errors.New(data.GetError())
		}

		key := data.GetKey()
		if len(key) > 0 {
			resp := &kvrpcpb.MvccGetByKeyResponse{Info: data.Info, RegionError: data.RegionError, Error: data.Error}
			return &mvccKV{Key: strings.ToUpper(hex.EncodeToString(key)), Value: resp, RegionID: curRegion.Region.GetID()}, nil
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

func (t *tikvHandlerTool) getMvccByIdxValue(idx table.Index, values url.Values, idxCols []*model.ColumnInfo, handleStr string) (*mvccKV, error) {
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
	encodedKey, _, err := idx.GenIndexKey(sc, idxRow, kv.IntHandle(handle), nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	data, err := t.GetMvccByEncodedKey(encodedKey)
	if err != nil {
		return nil, err
	}
	regionID, err := t.getRegionIDByKey(encodedKey)
	if err != nil {
		return nil, err
	}
	return &mvccKV{strings.ToUpper(hex.EncodeToString(encodedKey)), regionID, data}, err
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
	tbl, err := t.getTable(dbName, tableName)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return tbl.GetPhysicalID(), nil
}

func (t *tikvHandlerTool) getTable(dbName, tableName string) (table.PhysicalTable, error) {
	schema, err := t.schema()
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableName, partitionName := extractTableAndPartitionName(tableName)
	tableVal, err := schema.TableByName(model.NewCIStr(dbName), model.NewCIStr(tableName))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return t.getPartition(tableVal, partitionName)
}

func (t *tikvHandlerTool) getPartition(tableVal table.Table, partitionName string) (table.PhysicalTable, error) {
	if pt, ok := tableVal.(table.PartitionedTable); ok {
		if partitionName == "" {
			return tableVal.(table.PhysicalTable), errors.New("work on partitioned table, please specify the table name like this: table(partition)")
		}
		tblInfo := pt.Meta()
		pid, err := tables.FindPartitionByName(tblInfo, partitionName)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return pt.GetPartition(pid), nil
	}
	if partitionName != "" {
		return nil, fmt.Errorf("%s is not a partitionted table", tableVal.Meta().Name)
	}
	return tableVal.(table.PhysicalTable), nil
}

func (t *tikvHandlerTool) schema() (infoschema.InfoSchema, error) {
	session, err := session.CreateSession(t.Store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return domain.GetDomain(session.(sessionctx.Context)).InfoSchema(), nil
}

func (t *tikvHandlerTool) handleMvccGetByHex(params map[string]string) (*mvccKV, error) {
	encodedKey, err := hex.DecodeString(params[pHexKey])
	if err != nil {
		return nil, errors.Trace(err)
	}
	data, err := t.GetMvccByEncodedKey(encodedKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	regionID, err := t.getRegionIDByKey(encodedKey)
	if err != nil {
		return nil, err
	}
	return &mvccKV{Key: strings.ToUpper(params[pHexKey]), Value: data, RegionID: regionID}, nil
}

// settingsHandler is the handler for list tidb server settings.
type settingsHandler struct {
}

// binlogRecover is used to recover binlog service.
// When config binlog IgnoreError, binlog service will stop after meeting the first error.
// It can be recovered using HTTP API.
type binlogRecover struct{}

// schemaHandler is the handler for list database or table schemas.
type schemaHandler struct {
	*tikvHandlerTool
}

type dbTableHandler struct {
	*tikvHandlerTool
}

type flashReplicaHandler struct {
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

// ddlHistoryJobHandler is the handler for list job history.
type ddlHistoryJobHandler struct {
	*tikvHandlerTool
}

// ddlResignOwnerHandler is the handler for resigning ddl owner.
type ddlResignOwnerHandler struct {
	store kv.Storage
}

type serverInfoHandler struct {
	*tikvHandlerTool
}

type allServerInfoHandler struct {
	*tikvHandlerTool
}

type profileHandler struct {
	*tikvHandlerTool
}

// valueHandler is the handler for get value.
type valueHandler struct {
}

const (
	opTableRegions     = "regions"
	opTableDiskUsage   = "disk-usage"
	opTableScatter     = "scatter-table"
	opStopTableScatter = "stop-scatter-table"
)

// mvccTxnHandler is the handler for txn debugger.
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
	m[colID] = ft
	loc := time.UTC
	vals, err := tablecodec.DecodeRowToDatumMap(valData, m, loc)
	if err != nil {
		writeError(w, err)
		return
	}

	v := vals[colID]
	val, err := v.ToString()
	if err != nil {
		writeError(w, err)
		return
	}
	writeData(w, val)
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
	RegionID uint64              `json:"region_id"`
	StartKey []byte              `json:"start_key"`
	EndKey   []byte              `json:"end_key"`
	Frames   []*helper.FrameItem `json:"frames"`
}

// addTableInRange insert a table into RegionDetail
// with index's id or record in the range if r.
func (rt *RegionDetail) addTableInRange(dbName string, curTable *model.TableInfo, r *helper.RegionFrameRange) {
	tName := curTable.Name.String()
	tID := curTable.ID
	pi := curTable.GetPartitionInfo()
	isCommonHandle := curTable.IsCommonHandle
	for _, index := range curTable.Indices {
		if index.Primary && isCommonHandle {
			continue
		}
		if pi != nil {
			for _, def := range pi.Definitions {
				if f := r.GetIndexFrame(def.ID, index.ID, dbName, fmt.Sprintf("%s(%s)", tName, def.Name.O), index.Name.String()); f != nil {
					rt.Frames = append(rt.Frames, f)
				}
			}
		} else {
			if f := r.GetIndexFrame(tID, index.ID, dbName, tName, index.Name.String()); f != nil {
				rt.Frames = append(rt.Frames, f)
			}
		}

	}

	if pi != nil {
		for _, def := range pi.Definitions {
			if f := r.GetRecordFrame(def.ID, dbName, fmt.Sprintf("%s(%s)", tName, def.Name.O), isCommonHandle); f != nil {
				rt.Frames = append(rt.Frames, f)
			}
		}
	} else {
		if f := r.GetRecordFrame(tID, dbName, tName, isCommonHandle); f != nil {
			rt.Frames = append(rt.Frames, f)
		}
	}
}

// FrameItem includes a index's or record's meta data with table's info.
type FrameItem struct {
	DBName      string   `json:"db_name"`
	TableName   string   `json:"table_name"`
	TableID     int64    `json:"table_id"`
	IsRecord    bool     `json:"is_record"`
	RecordID    int64    `json:"record_id,omitempty"`
	IndexName   string   `json:"index_name,omitempty"`
	IndexID     int64    `json:"index_id,omitempty"`
	IndexValues []string `json:"index_values,omitempty"`
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
		region, err := t.RegionCache.PDClient().GetRegionByID(context.TODO(), regionID)
		if err != nil {
			return nil, errors.Trace(err)
		}

		failpoint.Inject("errGetRegionByIDEmpty", func(val failpoint.Value) {
			if val.(bool) {
				region.Meta = nil
			}
		})

		if region.Meta == nil {
			return nil, errors.Errorf("region not found for regionID %q", regionID)
		}
		regions[i] = RegionMeta{
			ID:          regionID,
			Leader:      region.Leader,
			Peers:       region.Meta.Peers,
			RegionEpoch: region.Meta.RegionEpoch,
		}

	}
	return regions, nil
}

// ServeHTTP handles request of list tidb server settings.
func (h settingsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method == "POST" {
		err := req.ParseForm()
		if err != nil {
			writeError(w, err)
			return
		}
		if levelStr := req.Form.Get("log_level"); levelStr != "" {
			err1 := logutil.SetLevel(levelStr)
			if err1 != nil {
				writeError(w, err1)
				return
			}

			l, err1 := log.ParseLevel(levelStr)
			if err1 != nil {
				writeError(w, err1)
				return
			}
			log.SetLevel(l)

			config.GetGlobalConfig().Log.Level = levelStr
		}
		if generalLog := req.Form.Get("tidb_general_log"); generalLog != "" {
			switch generalLog {
			case "0":
				atomic.StoreUint32(&variable.ProcessGeneralLog, 0)
			case "1":
				atomic.StoreUint32(&variable.ProcessGeneralLog, 1)
			default:
				writeError(w, errors.New("illegal argument"))
				return
			}
		}
		if ddlSlowThreshold := req.Form.Get("ddl_slow_threshold"); ddlSlowThreshold != "" {
			threshold, err1 := strconv.Atoi(ddlSlowThreshold)
			if err1 != nil {
				writeError(w, err1)
				return
			}
			if threshold > 0 {
				atomic.StoreUint32(&variable.DDLSlowOprThreshold, uint32(threshold))
			}
		}
		if checkMb4ValueInUtf8 := req.Form.Get("check_mb4_value_in_utf8"); checkMb4ValueInUtf8 != "" {
			switch checkMb4ValueInUtf8 {
			case "0":
				config.GetGlobalConfig().CheckMb4ValueInUTF8 = false
			case "1":
				config.GetGlobalConfig().CheckMb4ValueInUTF8 = true
			default:
				writeError(w, errors.New("illegal argument"))
				return
			}
		}
	} else {
		writeData(w, config.GetGlobalConfig())
	}
}

// ServeHTTP recovers binlog service.
func (h binlogRecover) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	op := req.FormValue(qOperation)
	switch op {
	case "reset":
		binloginfo.ResetSkippedCommitterCounter()
	case "nowait":
		binloginfo.DisableSkipBinlogFlag()
	case "status":
	default:
		sec, err := strconv.ParseInt(req.FormValue(qSeconds), 10, 64)
		if sec <= 0 || err != nil {
			sec = 1800
		}
		binloginfo.DisableSkipBinlogFlag()
		timeout := time.Duration(sec) * time.Second
		err = binloginfo.WaitBinlogRecover(timeout)
		if err != nil {
			writeError(w, err)
			return
		}
	}
	writeData(w, binloginfo.GetBinlogStatus())
}

type tableFlashReplicaInfo struct {
	// Modifying the field name needs to negotiate with TiFlash colleague.
	ID             int64    `json:"id"`
	ReplicaCount   uint64   `json:"replica_count"`
	LocationLabels []string `json:"location_labels"`
	Available      bool     `json:"available"`
	HighPriority   bool     `json:"high_priority"`
}

func (h flashReplicaHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		h.handleStatusReport(w, req)
		return
	}
	schema, err := h.schema()
	if err != nil {
		writeError(w, err)
		return
	}
	replicaInfos := make([]*tableFlashReplicaInfo, 0)
	allDBs := schema.AllSchemas()
	for _, db := range allDBs {
		tbls := schema.SchemaTables(db.Name)
		for _, tbl := range tbls {
			replicaInfos = h.getTiFlashReplicaInfo(tbl.Meta(), replicaInfos)
		}
	}
	dropedOrTruncateReplicaInfos, err := h.getDropOrTruncateTableTiflash(schema)
	if err != nil {
		writeError(w, err)
		return
	}
	replicaInfos = append(replicaInfos, dropedOrTruncateReplicaInfos...)
	writeData(w, replicaInfos)
}

func (h flashReplicaHandler) getTiFlashReplicaInfo(tblInfo *model.TableInfo, replicaInfos []*tableFlashReplicaInfo) []*tableFlashReplicaInfo {
	if tblInfo.TiFlashReplica == nil {
		return replicaInfos
	}
	if pi := tblInfo.GetPartitionInfo(); pi != nil {
		for _, p := range pi.Definitions {
			replicaInfos = append(replicaInfos, &tableFlashReplicaInfo{
				ID:             p.ID,
				ReplicaCount:   tblInfo.TiFlashReplica.Count,
				LocationLabels: tblInfo.TiFlashReplica.LocationLabels,
				Available:      tblInfo.TiFlashReplica.IsPartitionAvailable(p.ID),
			})
		}
		for _, p := range pi.AddingDefinitions {
			replicaInfos = append(replicaInfos, &tableFlashReplicaInfo{
				ID:             p.ID,
				ReplicaCount:   tblInfo.TiFlashReplica.Count,
				LocationLabels: tblInfo.TiFlashReplica.LocationLabels,
				Available:      tblInfo.TiFlashReplica.IsPartitionAvailable(p.ID),
				HighPriority:   true,
			})
		}
		return replicaInfos
	}
	replicaInfos = append(replicaInfos, &tableFlashReplicaInfo{
		ID:             tblInfo.ID,
		ReplicaCount:   tblInfo.TiFlashReplica.Count,
		LocationLabels: tblInfo.TiFlashReplica.LocationLabels,
		Available:      tblInfo.TiFlashReplica.Available,
	})
	return replicaInfos
}

func (h flashReplicaHandler) getDropOrTruncateTableTiflash(currentSchema infoschema.InfoSchema) ([]*tableFlashReplicaInfo, error) {
	s, err := session.CreateSession(h.Store.(kv.Storage))
	if err != nil {
		return nil, errors.Trace(err)
	}

	if s != nil {
		defer s.Close()
	}

	store := domain.GetDomain(s).Store()
	txn, err := store.Begin()
	if err != nil {
		return nil, errors.Trace(err)
	}
	gcSafePoint, err := gcutil.GetGCSafePoint(s)
	if err != nil {
		return nil, err
	}
	replicaInfos := make([]*tableFlashReplicaInfo, 0)
	uniqueIDMap := make(map[int64]struct{})
	handleJobAndTableInfo := func(job *model.Job, tblInfo *model.TableInfo) (bool, error) {
		// Avoid duplicate table ID info.
		if _, ok := currentSchema.TableByID(tblInfo.ID); ok {
			return false, nil
		}
		if _, ok := uniqueIDMap[tblInfo.ID]; ok {
			return false, nil
		}
		uniqueIDMap[tblInfo.ID] = struct{}{}
		replicaInfos = h.getTiFlashReplicaInfo(tblInfo, replicaInfos)
		return false, nil
	}
	dom := domain.GetDomain(s)
	fn := func(jobs []*model.Job) (bool, error) {
		return executor.GetDropOrTruncateTableInfoFromJobs(jobs, gcSafePoint, dom, handleJobAndTableInfo)
	}

	err = admin.IterAllDDLJobs(txn, fn)
	if err != nil {
		if terror.ErrorEqual(variable.ErrSnapshotTooOld, err) {
			// The err indicate that current ddl job and remain DDL jobs was been deleted by GC,
			// just ignore the error and return directly.
			return replicaInfos, nil
		}
		return nil, err
	}
	return replicaInfos, nil
}

type tableFlashReplicaStatus struct {
	// Modifying the field name needs to negotiate with TiFlash colleague.
	ID int64 `json:"id"`
	// RegionCount is the number of regions that need sync.
	RegionCount uint64 `json:"region_count"`
	// FlashRegionCount is the number of regions that already sync completed.
	FlashRegionCount uint64 `json:"flash_region_count"`
}

// checkTableFlashReplicaAvailable uses to check the available status of table flash replica.
func (tf *tableFlashReplicaStatus) checkTableFlashReplicaAvailable() bool {
	return tf.FlashRegionCount == tf.RegionCount
}

func (h flashReplicaHandler) handleStatusReport(w http.ResponseWriter, req *http.Request) {
	var status tableFlashReplicaStatus
	err := json.NewDecoder(req.Body).Decode(&status)
	if err != nil {
		writeError(w, err)
		return
	}
	do, err := session.GetDomain(h.Store.(kv.Storage))
	if err != nil {
		writeError(w, err)
		return
	}
	s, err := session.CreateSession(h.Store.(kv.Storage))
	if err != nil {
		writeError(w, err)
		return
	}
	available := status.checkTableFlashReplicaAvailable()
	err = do.DDL().UpdateTableReplicaInfo(s, status.ID, available)
	if err != nil {
		writeError(w, err)
	}
	if available {
		err = infosync.DeleteTiFlashTableSyncProgress(status.ID)
	} else {
		err = infosync.UpdateTiFlashTableSyncProgress(context.Background(), status.ID, float64(status.FlashRegionCount)/float64(status.RegionCount))
	}
	if err != nil {
		writeError(w, err)
	}

	logutil.BgLogger().Info("handle flash replica report", zap.Int64("table ID", status.ID), zap.Uint64("region count",
		status.RegionCount),
		zap.Uint64("flash region count", status.FlashRegionCount),
		zap.Error(err))
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
		writeError(w, infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName))
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
			writeError(w, infoschema.ErrTableNotExists.GenWithStack("Table which ID = %s does not exist.", tableID))
			return
		}
		if data, ok := schema.TableByID(int64(tid)); ok {
			writeData(w, data.Meta())
			return
		}
		writeError(w, infoschema.ErrTableNotExists.GenWithStack("Table which ID = %s does not exist.", tableID))
		return
	}

	// all databases' schemas
	writeData(w, schema.AllSchemas())
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

	tableName, partitionName := extractTableAndPartitionName(tableName)
	tableVal, err := schema.TableByName(model.NewCIStr(dbName), model.NewCIStr(tableName))
	if err != nil {
		writeError(w, err)
		return
	}
	switch h.op {
	case opTableRegions:
		h.handleRegionRequest(schema, tableVal, w, req)
	case opTableDiskUsage:
		h.handleDiskUsageRequest(tableVal, w)
	case opTableScatter:
		// supports partition table, only get one physical table, prevent too many scatter schedulers.
		ptbl, err := h.getPartition(tableVal, partitionName)
		if err != nil {
			writeError(w, err)
			return
		}
		h.handleScatterTableRequest(schema, ptbl, w, req)
	case opStopTableScatter:
		ptbl, err := h.getPartition(tableVal, partitionName)
		if err != nil {
			writeError(w, err)
			return
		}
		h.handleStopScatterTableRequest(schema, ptbl, w, req)
	default:
		writeError(w, errors.New("method not found"))
	}
}

// ServeHTTP handles request of ddl jobs history.
func (h ddlHistoryJobHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if limitID := req.FormValue(qLimit); len(limitID) > 0 {
		lid, err := strconv.Atoi(limitID)

		if err != nil {
			writeError(w, err)
			return
		}

		if lid < 1 {
			writeError(w, errors.New("ddl history limit must be greater than 1"))
			return
		}

		jobs, err := h.getAllHistoryDDL()
		if err != nil {
			writeError(w, errors.New("ddl history not found"))
			return
		}

		jobsLen := len(jobs)
		if jobsLen > lid {
			start := jobsLen - lid
			jobs = jobs[start:]
		}

		writeData(w, jobs)
		return
	}
	jobs, err := h.getAllHistoryDDL()
	if err != nil {
		writeError(w, errors.New("ddl history not found"))
		return
	}
	writeData(w, jobs)
}

func (h ddlHistoryJobHandler) getAllHistoryDDL() ([]*model.Job, error) {
	s, err := session.CreateSession(h.Store.(kv.Storage))
	if err != nil {
		return nil, errors.Trace(err)
	}

	if s != nil {
		defer s.Close()
	}

	store := domain.GetDomain(s.(sessionctx.Context)).Store()
	txn, err := store.Begin()

	if err != nil {
		return nil, errors.Trace(err)
	}
	txnMeta := meta.NewMeta(txn)

	jobs, err := txnMeta.GetAllHistoryDDLJobs()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return jobs, nil
}

func (h ddlResignOwnerHandler) resignDDLOwner() error {
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
func (h ddlResignOwnerHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, errors.Errorf("This api only support POST method."))
		return
	}

	err := h.resignDDLOwner()
	if err != nil {
		log.Error(err)
		writeError(w, err)
		return
	}

	writeData(w, "success!")
}

func (h tableHandler) getPDAddr() ([]string, error) {
	etcd, ok := h.Store.(tikv.EtcdBackend)
	if !ok {
		return nil, errors.New("not implemented")
	}
	pdAddrs, err := etcd.EtcdAddrs()
	if err != nil {
		return nil, err
	}
	if len(pdAddrs) < 0 {
		return nil, errors.New("pd unavailable")
	}
	return pdAddrs, nil
}

func (h tableHandler) addScatterSchedule(startKey, endKey []byte, name string) error {
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
	scheduleURL := fmt.Sprintf("%s://%s/pd/api/v1/schedulers", util.InternalHTTPSchema(), pdAddrs[0])
	resp, err := util.InternalHTTPClient().Post(scheduleURL, "application/json", bytes.NewBuffer(v))
	if err != nil {
		return err
	}
	if err := resp.Body.Close(); err != nil {
		log.Error(err)
	}
	return nil
}

func (h tableHandler) deleteScatterSchedule(name string) error {
	pdAddrs, err := h.getPDAddr()
	if err != nil {
		return err
	}
	scheduleURL := fmt.Sprintf("%s://%s/pd/api/v1/schedulers/scatter-range-%s", util.InternalHTTPSchema(), pdAddrs[0], name)
	req, err := http.NewRequest(http.MethodDelete, scheduleURL, nil)
	if err != nil {
		return err
	}
	resp, err := util.InternalHTTPClient().Do(req)
	if err != nil {
		return err
	}
	if err := resp.Body.Close(); err != nil {
		log.Error(err)
	}
	return nil
}

func (h tableHandler) handleScatterTableRequest(schema infoschema.InfoSchema, tbl table.PhysicalTable, w http.ResponseWriter, req *http.Request) {
	// for record
	tableID := tbl.GetPhysicalID()
	startKey, endKey := tablecodec.GetTableHandleKeyRange(tableID)
	startKey = codec.EncodeBytes([]byte{}, startKey)
	endKey = codec.EncodeBytes([]byte{}, endKey)
	tableName := fmt.Sprintf("%s-%d", tbl.Meta().Name.String(), tableID)
	err := h.addScatterSchedule(startKey, endKey, tableName)
	if err != nil {
		writeError(w, errors.Annotate(err, "scatter record error"))
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
			writeError(w, errors.Annotatef(err, "scatter index(%s) error", name))
			return
		}
	}
	writeData(w, "success!")
}

func (h tableHandler) handleStopScatterTableRequest(schema infoschema.InfoSchema, tbl table.PhysicalTable, w http.ResponseWriter, req *http.Request) {
	// for record
	tableName := fmt.Sprintf("%s-%d", tbl.Meta().Name.String(), tbl.GetPhysicalID())
	err := h.deleteScatterSchedule(tableName)
	if err != nil {
		writeError(w, errors.Annotate(err, "stop scatter record error"))
		return
	}
	// for indices
	for _, index := range tbl.Indices() {
		indexName := index.Meta().Name.String()
		name := tableName + "-" + indexName
		err := h.deleteScatterSchedule(name)
		if err != nil {
			writeError(w, errors.Annotatef(err, "delete scatter index(%s) error", name))
			return
		}
	}
	writeData(w, "success!")
}

func (h tableHandler) handleRegionRequest(schema infoschema.InfoSchema, tbl table.Table, w http.ResponseWriter, req *http.Request) {
	pi := tbl.Meta().GetPartitionInfo()
	if pi != nil {
		// Partitioned table.
		var data []*TableRegions
		for _, def := range pi.Definitions {
			tableRegions, err := h.getRegionsByID(tbl, def.ID, def.Name.O)
			if err != nil {
				writeError(w, err)
				return
			}

			data = append(data, tableRegions)
		}
		writeData(w, data)
		return
	}

	meta := tbl.Meta()
	tableRegions, err := h.getRegionsByID(tbl, meta.ID, meta.Name.O)
	if err != nil {
		writeError(w, err)
		return
	}

	writeData(w, tableRegions)
}

func (h tableHandler) getRegionsByID(tbl table.Table, id int64, name string) (*TableRegions, error) {
	// for record
	startKey, endKey := tablecodec.GetTableHandleKeyRange(id)
	ctx := context.Background()
	pdCli := h.RegionCache.PDClient()
	regions, err := pdCli.ScanRegions(ctx, startKey, endKey, -1)
	if err != nil {
		return nil, err
	}

	recordRegions := make([]RegionMeta, 0, len(regions))
	for _, region := range regions {
		meta := RegionMeta{
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
		regions, err := pdCli.ScanRegions(ctx, startKey, endKey, -1)
		if err != nil {
			return nil, err
		}
		indexRegions := make([]RegionMeta, 0, len(regions))
		for _, region := range regions {
			meta := RegionMeta{
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

func (h tableHandler) handleDiskUsageRequest(tbl table.Table, w http.ResponseWriter) {
	tableID := tbl.Meta().ID
	var stats helper.PDRegionStats
	err := h.GetPDRegionStats(tableID, &stats)
	if err != nil {
		writeError(w, err)
		return
	}
	writeData(w, stats.StorageSize)
}

type hotRegion struct {
	helper.TblIndex
	helper.RegionMetric
}
type hotRegions []hotRegion

func (rs hotRegions) Len() int {
	return len(rs)
}

func (rs hotRegions) Less(i, j int) bool {
	return rs[i].MaxHotDegree > rs[j].MaxHotDegree || (rs[i].MaxHotDegree == rs[j].MaxHotDegree && rs[i].FlowBytes > rs[j].FlowBytes)
}

func (rs hotRegions) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}

// ServeHTTP handles request of get region by ID.
func (h regionHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// parse and check params
	params := mux.Vars(req)
	if _, ok := params[pRegionID]; !ok {
		router := mux.CurrentRoute(req).GetName()
		if router == "RegionsMeta" {
			startKey := []byte{'m'}
			endKey := []byte{'n'}

			recordRegionIDs, err := h.RegionCache.ListRegionIDsInKeyRange(tikv.NewBackofferWithVars(context.Background(), 500, nil), startKey, endKey)
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
		if router == "RegionHot" {
			schema, err := h.schema()
			if err != nil {
				writeError(w, err)
				return
			}
			hotRead, err := h.ScrapeHotInfo(pdapi.HotRead, schema.AllSchemas())
			if err != nil {
				writeError(w, err)
				return
			}
			hotWrite, err := h.ScrapeHotInfo(pdapi.HotWrite, schema.AllSchemas())
			if err != nil {
				writeError(w, err)
				return
			}
			writeData(w, map[string]interface{}{
				"write": hotWrite,
				"read":  hotRead,
			})
			return
		}
		return
	}

	regionIDInt, err := strconv.ParseInt(params[pRegionID], 0, 64)
	if err != nil {
		writeError(w, err)
		return
	}
	regionID := uint64(regionIDInt)

	// locate region
	region, err := h.RegionCache.LocateRegionByID(tikv.NewBackofferWithVars(context.Background(), 500, nil), regionID)
	if err != nil {
		writeError(w, err)
		return
	}

	frameRange, err := helper.NewRegionFrameRange(region)
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
		if util.IsMemDB(db.Name.L) {
			continue
		}
		for _, tableVal := range db.Tables {
			regionDetail.addTableInRange(db.Name.String(), tableVal, frameRange)
		}
	}
	writeData(w, regionDetail)
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
		decode := len(req.URL.Query().Get("decode")) > 0
		data, err = h.handleMvccGetByKey(params, decode)
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

func extractTableAndPartitionName(str string) (string, string) {
	// extract table name and partition name from this "table(partition)":
	// A sane person would not let the the table name or partition name contain '('.
	start := strings.IndexByte(str, '(')
	if start == -1 {
		return str, ""
	}
	end := strings.IndexByte(str, ')')
	if end == -1 {
		return str, ""
	}
	return str[:start], str[start+1 : end]
}

// handleMvccGetByIdx gets MVCC info by an index key.
func (h mvccTxnHandler) handleMvccGetByIdx(params map[string]string, values url.Values) (interface{}, error) {
	dbName := params[pDBName]
	tableName := params[pTableName]
	handleStr := params[pHandle]

	t, err := h.getTable(dbName, tableName)
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

func (h mvccTxnHandler) handleMvccGetByKey(params map[string]string, decodeData bool) (interface{}, error) {
	handle, err := strconv.ParseInt(params[pHandle], 0, 64)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tb, err := h.getTable(params[pDBName], params[pTableName])
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp, err := h.getMvccByHandle(tb.GetPhysicalID(), handle)
	if err != nil {
		return nil, err
	}
	if !decodeData {
		return resp, nil
	}
	colMap := make(map[int64]*types.FieldType, 3)
	for _, col := range tb.Meta().Columns {
		colMap[col.ID] = &col.FieldType
	}

	respValue := resp.Value
	var result interface{} = resp
	if respValue.Info != nil {
		datas := make(map[string][]map[string]string)
		for _, w := range respValue.Info.Writes {
			if len(w.ShortValue) > 0 {
				datas[strconv.FormatUint(w.StartTs, 10)], err = h.decodeMvccData(w.ShortValue, colMap, tb.Meta())
			}
		}

		for _, v := range respValue.Info.Values {
			if len(v.Value) > 0 {
				datas[strconv.FormatUint(v.StartTs, 10)], err = h.decodeMvccData(v.Value, colMap, tb.Meta())
			}
		}

		if len(datas) > 0 {
			re := map[string]interface{}{
				"key":  resp.Key,
				"info": respValue.Info,
				"data": datas,
			}
			if err != nil {
				re["decode_error"] = err.Error()
			}
			result = re
		}
	}

	return result, nil
}

func (h mvccTxnHandler) decodeMvccData(bs []byte, colMap map[int64]*types.FieldType, tb *model.TableInfo) ([]map[string]string, error) {
	rs, err := tablecodec.DecodeRowToDatumMap(bs, colMap, time.UTC)
	var record []map[string]string
	for _, col := range tb.Columns {
		if c, ok := rs[col.ID]; ok {
			data := "nil"
			if !c.IsNull() {
				data, err = c.ToString()
			}
			record = append(record, map[string]string{col.Name.O: data})
		}
	}
	return record, err
}

func (h *mvccTxnHandler) handleMvccGetByTxn(params map[string]string) (interface{}, error) {
	startTS, err := strconv.ParseInt(params[pStartTS], 0, 64)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableID, err := h.getTableID(params[pDBName], params[pTableName])
	if err != nil {
		return nil, errors.Trace(err)
	}
	startKey := tablecodec.EncodeTablePrefix(tableID)
	endKey := tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(math.MaxInt64))
	return h.getMvccByStartTs(uint64(startTS), startKey, endKey)
}

// serverInfo is used to report the servers info when do http request.
type serverInfo struct {
	IsOwner bool `json:"is_owner"`
	*infosync.ServerInfo
}

// ServeHTTP handles request of ddl server info.
func (h serverInfoHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	do, err := session.GetDomain(h.Store.(kv.Storage))
	if err != nil {
		writeError(w, errors.New("create session error"))
		log.Error(err)
		return
	}
	info := serverInfo{}
	info.ServerInfo, err = infosync.GetServerInfo()
	if err != nil {
		writeError(w, err)
		log.Error(err)
		return
	}
	info.IsOwner = do.DDL().OwnerManager().IsOwner()
	writeData(w, info)
}

// clusterServerInfo is used to report cluster servers info when do http request.
type clusterServerInfo struct {
	ServersNum                   int                             `json:"servers_num,omitempty"`
	OwnerID                      string                          `json:"owner_id"`
	IsAllServerVersionConsistent bool                            `json:"is_all_server_version_consistent,omitempty"`
	AllServersDiffVersions       []infosync.ServerVersionInfo    `json:"all_servers_diff_versions,omitempty"`
	AllServersInfo               map[string]*infosync.ServerInfo `json:"all_servers_info,omitempty"`
}

// ServeHTTP handles request of all ddl servers info.
func (h allServerInfoHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	do, err := session.GetDomain(h.Store.(kv.Storage))
	if err != nil {
		writeError(w, errors.New("create session error"))
		log.Error(err)
		return
	}
	ctx := context.Background()
	allServersInfo, err := infosync.GetAllServerInfo(ctx)
	if err != nil {
		writeError(w, errors.New("ddl server information not found"))
		log.Error(err)
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	ownerID, err := do.DDL().OwnerManager().GetOwnerID(ctx)
	cancel()
	if err != nil {
		writeError(w, errors.New("ddl server information not found"))
		log.Error(err)
		return
	}
	allVersionsMap := map[infosync.ServerVersionInfo]struct{}{}
	allVersions := make([]infosync.ServerVersionInfo, 0, len(allServersInfo))
	for _, v := range allServersInfo {
		if _, ok := allVersionsMap[v.ServerVersionInfo]; ok {
			continue
		}
		allVersionsMap[v.ServerVersionInfo] = struct{}{}
		allVersions = append(allVersions, v.ServerVersionInfo)
	}
	clusterInfo := clusterServerInfo{
		ServersNum: len(allServersInfo),
		OwnerID:    ownerID,
		// len(allVersions) = 1 indicates there has only 1 tidb version in cluster, so all server versions are consistent.
		IsAllServerVersionConsistent: len(allVersions) == 1,
		AllServersInfo:               allServersInfo,
	}
	// if IsAllServerVersionConsistent is false, return the all tidb servers version.
	if !clusterInfo.IsAllServerVersionConsistent {
		clusterInfo.AllServersDiffVersions = allVersions
	}
	writeData(w, clusterInfo)
}

// dbTableInfo is used to report the database, table information and the current schema version.
type dbTableInfo struct {
	DBInfo        *model.DBInfo    `json:"db_info"`
	TableInfo     *model.TableInfo `json:"table_info"`
	SchemaVersion int64            `json:"schema_version"`
}

// ServeHTTP handles request of database information and table information by tableID.
func (h dbTableHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	tableID := params[pTableID]
	physicalID, err := strconv.Atoi(tableID)
	if err != nil {
		writeError(w, errors.Errorf("Wrong tableID: %v", tableID))
		return
	}

	schema, err := h.schema()
	if err != nil {
		writeError(w, err)
		return
	}

	dbTblInfo := dbTableInfo{
		SchemaVersion: schema.SchemaMetaVersion(),
	}
	tbl, ok := schema.TableByID(int64(physicalID))
	if ok {
		dbTblInfo.TableInfo = tbl.Meta()
		dbInfo, ok := schema.SchemaByTable(dbTblInfo.TableInfo)
		if !ok {
			logutil.BgLogger().Error("can not find the database of the table", zap.Int64("table id", dbTblInfo.TableInfo.ID), zap.String("table name", dbTblInfo.TableInfo.Name.L))
			writeError(w, infoschema.ErrTableNotExists.GenWithStack("Table which ID = %s does not exist.", tableID))
			return
		}
		dbTblInfo.DBInfo = dbInfo
		writeData(w, dbTblInfo)
		return
	}
	// The physicalID maybe a partition ID of the partition-table.
	tbl, dbInfo := schema.FindTableByPartitionID(int64(physicalID))
	if tbl == nil {
		writeError(w, infoschema.ErrTableNotExists.GenWithStack("Table which ID = %s does not exist.", tableID))
		return
	}
	dbTblInfo.TableInfo = tbl.Meta()
	dbTblInfo.DBInfo = dbInfo
	writeData(w, dbTblInfo)
}

// ServeHTTP handles request of TiDB metric profile.
func (h profileHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	sctx, err := session.CreateSession(h.Store)
	if err != nil {
		writeError(w, err)
		return
	}
	var start, end time.Time
	if req.FormValue("end") != "" {
		end, err = time.ParseInLocation(time.RFC3339, req.FormValue("end"), sctx.GetSessionVars().Location())
		if err != nil {
			writeError(w, err)
			return
		}
	} else {
		end = time.Now()
	}
	if req.FormValue("start") != "" {
		start, err = time.ParseInLocation(time.RFC3339, req.FormValue("start"), sctx.GetSessionVars().Location())
		if err != nil {
			writeError(w, err)
			return
		}
	} else {
		start = end.Add(-time.Minute * 10)
	}
	valueTp := req.FormValue("type")
	pb, err := executor.NewProfileBuilder(sctx, start, end, valueTp)
	if err != nil {
		writeError(w, err)
		return
	}
	err = pb.Collect()
	if err != nil {
		writeError(w, err)
		return
	}
	_, err = w.Write(pb.Build())
	terror.Log(errors.Trace(err))
}

// testHandler is the handler for tests. It's convenient to provide some APIs for integration tests.
type testHandler struct {
	*tikvHandlerTool
	gcIsRunning uint32
}

// ServeHTTP handles test related requests.
func (h *testHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	mod := strings.ToLower(params["mod"])
	op := strings.ToLower(params["op"])

	switch mod {
	case "gc":
		h.handleGC(op, w, req)
	default:
		writeError(w, errors.NotSupportedf("module(%s)", mod))
	}
}

// Supported operations:
//   * resolvelock?safepoint={uint64}&physical={bool}:
//	   * safepoint: resolve all locks whose timestamp is less than the safepoint.
//	   * physical: whether it uses physical(green GC) mode to scan locks. Default is true.
func (h *testHandler) handleGC(op string, w http.ResponseWriter, req *http.Request) {
	if !atomic.CompareAndSwapUint32(&h.gcIsRunning, 0, 1) {
		writeError(w, errors.New("GC is running"))
		return
	}
	defer atomic.StoreUint32(&h.gcIsRunning, 0)

	switch op {
	case "resolvelock":
		h.handleGCResolveLocks(w, req)
	default:
		writeError(w, errors.NotSupportedf("operation(%s)", op))
	}
}

func (h *testHandler) handleGCResolveLocks(w http.ResponseWriter, req *http.Request) {
	s := req.FormValue("safepoint")
	safePoint, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		writeError(w, errors.Errorf("parse safePoint(%s) failed", s))
		return
	}
	usePhysical := true
	s = req.FormValue("physical")
	if s != "" {
		usePhysical, err = strconv.ParseBool(s)
		if err != nil {
			writeError(w, errors.Errorf("parse physical(%s) failed", s))
			return
		}
	}

	ctx := req.Context()
	logutil.Logger(ctx).Info("start resolving locks", zap.Uint64("safePoint", safePoint), zap.Bool("physical", usePhysical))
	physicalUsed, err := gcworker.RunResolveLocks(ctx, h.Store, h.RegionCache.PDClient(), safePoint, "testGCWorker", 3, usePhysical)
	if err != nil {
		writeError(w, errors.Annotate(err, "resolveLocks failed"))
	} else {
		writeData(w, map[string]interface{}{
			"physicalUsed": physicalUsed,
		})
	}
}
