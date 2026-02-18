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
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/gcworker"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

// ServeHTTP handles request of get region by ID.
func (h RegionHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// parse and check params
	params := mux.Vars(req)
	if _, ok := params[handler.RegionID]; !ok {
		router := mux.CurrentRoute(req).GetName()
		if router == "RegionsMeta" {
			startKey := []byte{'m'}
			endKey := []byte{'n'}

			recordRegionIDs, err := h.RegionCache.ListRegionIDsInKeyRange(tikv.NewBackofferWithVars(context.Background(), 500, nil), startKey, endKey)
			if err != nil {
				handler.WriteError(w, err)
				return
			}

			recordRegions, err := h.GetRegionsMeta(recordRegionIDs)
			if err != nil {
				handler.WriteError(w, err)
				return
			}
			handler.WriteData(w, recordRegions)
			return
		}
		if router == "RegionHot" {
			schema, err := h.Schema()
			if err != nil {
				handler.WriteError(w, err)
				return
			}
			ctx := context.Background()
			hotRead, err := h.ScrapeHotInfo(ctx, helper.HotRead, schema, nil)
			if err != nil {
				handler.WriteError(w, err)
				return
			}
			hotWrite, err := h.ScrapeHotInfo(ctx, helper.HotWrite, schema, nil)
			if err != nil {
				handler.WriteError(w, err)
				return
			}
			handler.WriteData(w, map[string]any{
				"write": hotWrite,
				"read":  hotRead,
			})
			return
		}
		return
	}

	regionIDInt, err := strconv.ParseInt(params[handler.RegionID], 0, 64)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	regionID := uint64(regionIDInt)

	// locate region
	region, err := h.RegionCache.LocateRegionByID(tikv.NewBackofferWithVars(context.Background(), 500, nil), regionID)
	if err != nil {
		handler.WriteError(w, err)
		return
	}

	frameRange, err := helper.NewRegionFrameRange(region)
	if err != nil {
		handler.WriteError(w, err)
		return
	}

	// create RegionDetail from RegionFrameRange
	regionDetail := &RegionDetail{
		RegionID:    regionID,
		RangeDetail: createRangeDetail(region.StartKey, region.EndKey),
	}
	schema, err := h.Schema()
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	// Since we need a database's name for each frame, and a table's database name can not
	// get from table's ID directly. Above all, here do dot process like
	// 		`for id in [frameRange.firstTableID,frameRange.endTableID]`
	// on [frameRange.firstTableID,frameRange.endTableID] is small enough.
	for _, dbName := range schema.AllSchemaNames() {
		if metadef.IsMemDB(dbName.L) {
			continue
		}
		tables, err := schema.SchemaTableInfos(context.Background(), dbName)
		if err != nil {
			handler.WriteError(w, err)
			return
		}
		for _, tableVal := range tables {
			regionDetail.addTableInRange(dbName.String(), tableVal, frameRange)
		}
	}
	handler.WriteData(w, regionDetail)
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
func (h MvccTxnHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var data any
	params := mux.Vars(req)
	var err error
	switch h.op {
	case OpMvccGetByHex:
		data, err = h.HandleMvccGetByHex(params)
	case OpMvccGetByIdx, OpMvccGetByKey:
		if req.URL == nil {
			err = errors.BadRequestf("Invalid URL")
			break
		}
		values := make(url.Values)
		err = parseQuery(req.URL.RawQuery, values, true)
		if err == nil {
			if h.op == OpMvccGetByIdx {
				data, err = h.handleMvccGetByIdx(params, values)
			} else {
				data, err = h.handleMvccGetByKey(params, values)
			}
		}
	case OpMvccGetByTxn:
		data, err = h.handleMvccGetByTxn(params)
	default:
		err = errors.NotSupportedf("Operation not supported.")
	}
	if err != nil {
		handler.WriteError(w, err)
	} else {
		handler.WriteData(w, data)
	}
}

// handleMvccGetByIdx gets MVCC info by an index key.
func (h MvccTxnHandler) handleMvccGetByIdx(params map[string]string, values url.Values) (any, error) {
	dbName := params[handler.DBName]
	tableName := params[handler.TableName]

	t, err := h.GetTable(dbName, tableName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	handle, err := h.GetHandle(t, params, values)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var idxCols []*model.ColumnInfo
	var idx table.Index
	for _, v := range t.Indices() {
		if strings.EqualFold(v.Meta().Name.String(), params[handler.IndexName]) {
			for _, c := range v.Meta().Columns {
				idxCols = append(idxCols, t.Meta().Columns[c.Offset])
			}
			idx = v
			break
		}
	}
	if idx == nil {
		return nil, errors.NotFoundf("Index %s not found!", params[handler.IndexName])
	}
	return h.GetMvccByIdxValue(idx, values, idxCols, handle)
}

func (h MvccTxnHandler) handleMvccGetByKey(params map[string]string, values url.Values) (any, error) {
	dbName := params[handler.DBName]
	tableName := params[handler.TableName]
	tb, err := h.GetTable(dbName, tableName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	handle, err := h.GetHandle(tb, params, values)
	if err != nil {
		return nil, err
	}

	encodedKey := tablecodec.EncodeRecordKey(tb.RecordPrefix(), handle)
	data, err := h.GetMvccByEncodedKey(encodedKey)
	if err != nil {
		return nil, err
	}
	regionID, err := h.GetRegionIDByKey(encodedKey)
	if err != nil {
		return nil, err
	}
	resp := &helper.MvccKV{Key: strings.ToUpper(hex.EncodeToString(encodedKey)), Value: data, RegionID: regionID}
	if len(values.Get("decode")) == 0 {
		return resp, nil
	}
	colMap := make(map[int64]*types.FieldType, 3)
	for _, col := range tb.Meta().Columns {
		colMap[col.ID] = &(col.FieldType)
	}

	respValue := resp.Value
	var result any = resp
	if respValue.Info != nil {
		datas := make(map[string]map[string]string)
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
			re := map[string]any{
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

func (MvccTxnHandler) decodeMvccData(bs []byte, colMap map[int64]*types.FieldType, tb *model.TableInfo) (map[string]string, error) {
	rs, err := tablecodec.DecodeRowToDatumMap(bs, colMap, time.UTC)
	record := make(map[string]string, len(tb.Columns))
	for _, col := range tb.Columns {
		if c, ok := rs[col.ID]; ok {
			data := "nil"
			if !c.IsNull() {
				data, err = c.ToString()
			}
			record[col.Name.O] = data
		}
	}
	return record, err
}

func (h *MvccTxnHandler) handleMvccGetByTxn(params map[string]string) (any, error) {
	startTS, err := strconv.ParseInt(params[handler.StartTS], 0, 64)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableID, err := h.GetTableID(params[handler.DBName], params[handler.TableName])
	if err != nil {
		return nil, errors.Trace(err)
	}
	startKey := tablecodec.EncodeTablePrefix(tableID)
	endKey := tablecodec.EncodeRowKeyWithHandle(tableID, kv.IntHandle(math.MaxInt64))
	return h.GetMvccByStartTs(uint64(startTS), startKey, endKey)
}

// ServerInfo is used to report the servers info when do http request.
type ServerInfo struct {
	IsOwner  bool `json:"is_owner"`
	MaxProcs int  `json:"max_procs"`
	GOGC     int  `json:"gogc"`
	*serverinfo.ServerInfo
}

// ServeHTTP handles request of ddl server info.
func (h ServerInfoHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	do, err := session.GetDomain(h.Store)
	if err != nil {
		handler.WriteError(w, errors.New("create session error"))
		log.Error("failed to get session domain", zap.Error(err))
		return
	}
	info := ServerInfo{}
	info.ServerInfo, err = infosync.GetServerInfo()
	if err != nil {
		handler.WriteError(w, err)
		log.Error("failed to get server info", zap.Error(err))
		return
	}
	info.IsOwner = do.DDL().OwnerManager().IsOwner()
	info.MaxProcs = runtime.GOMAXPROCS(0)
	info.GOGC = util.GetGOGC()
	handler.WriteData(w, info)
}

// ClusterServerInfo is used to report cluster servers info when do http request.
type ClusterServerInfo struct {
	ServersNum                   int                               `json:"servers_num,omitempty"`
	OwnerID                      string                            `json:"owner_id"`
	IsAllServerVersionConsistent bool                              `json:"is_all_server_version_consistent,omitempty"`
	AllServersDiffVersions       []serverinfo.VersionInfo          `json:"all_servers_diff_versions,omitempty"`
	AllServersInfo               map[string]*serverinfo.ServerInfo `json:"all_servers_info,omitempty"`
}

// ServeHTTP handles request of all ddl servers info.
func (h AllServerInfoHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	do, err := session.GetDomain(h.Store)
	if err != nil {
		handler.WriteError(w, errors.New("create session error"))
		log.Error("failed to get session domain", zap.Error(err))
		return
	}
	ctx := context.Background()
	allServersInfo, err := infosync.GetAllServerInfo(ctx)
	if err != nil {
		handler.WriteError(w, errors.New("ddl server information not found"))
		log.Error("failed to get all server info", zap.Error(err))
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	ownerID, err := do.DDL().OwnerManager().GetOwnerID(ctx)
	cancel()
	if err != nil {
		handler.WriteError(w, errors.New("ddl server information not found"))
		log.Error("failed to get owner id", zap.Error(err))
		return
	}
	allVersionsMap := map[serverinfo.VersionInfo]struct{}{}
	allVersions := make([]serverinfo.VersionInfo, 0, len(allServersInfo))
	for _, v := range allServersInfo {
		if _, ok := allVersionsMap[v.VersionInfo]; ok {
			continue
		}
		allVersionsMap[v.VersionInfo] = struct{}{}
		allVersions = append(allVersions, v.VersionInfo)
	}
	clusterInfo := ClusterServerInfo{
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
	handler.WriteData(w, clusterInfo)
}

// DBTableInfo is used to report the database, table information and the current schema version.
type DBTableInfo struct {
	DBInfo        *model.DBInfo    `json:"db_info"`
	TableInfo     *model.TableInfo `json:"table_info"`
	SchemaVersion int64            `json:"schema_version"`
}

// ServeHTTP handles request of database information and table information by tableID.
func (h DBTableHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	tableID := params[handler.TableID]
	physicalID, err := strconv.Atoi(tableID)
	if err != nil {
		handler.WriteError(w, errors.Errorf("Wrong tableID: %v", tableID))
		return
	}

	schema, err := h.Schema()
	if err != nil {
		handler.WriteError(w, err)
		return
	}

	dbTblInfo := DBTableInfo{
		SchemaVersion: schema.SchemaMetaVersion(),
	}
	tbl, ok := schema.TableByID(context.Background(), int64(physicalID))
	if ok {
		dbTblInfo.TableInfo = tbl.Meta()
		dbInfo, ok := infoschema.SchemaByTable(schema, dbTblInfo.TableInfo)
		if !ok {
			logutil.BgLogger().Error("can not find the database of the table", zap.Int64("table id", dbTblInfo.TableInfo.ID), zap.String("table name", dbTblInfo.TableInfo.Name.L))
			handler.WriteError(w, infoschema.ErrTableNotExists.GenWithStack("Table which ID = %s does not exist.", tableID))
			return
		}
		dbTblInfo.DBInfo = dbInfo
		handler.WriteData(w, dbTblInfo)
		return
	}
	// The physicalID maybe a partition ID of the partition-table.
	tbl, dbInfo, _ := schema.FindTableByPartitionID(int64(physicalID))
	if tbl == nil {
		handler.WriteError(w, infoschema.ErrTableNotExists.GenWithStack("Table which ID = %s does not exist.", tableID))
		return
	}
	dbTblInfo.TableInfo = tbl.Meta()
	dbTblInfo.DBInfo = dbInfo
	handler.WriteData(w, dbTblInfo)
}

// ServeHTTP handles request of TiDB metric profile.
func (h ProfileHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	sctx, err := session.CreateSession(h.Store)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	defer sctx.Close()

	var start, end time.Time
	if req.FormValue("end") != "" {
		end, err = time.ParseInLocation(time.RFC3339, req.FormValue("end"), sctx.GetSessionVars().Location())
		if err != nil {
			handler.WriteError(w, err)
			return
		}
	} else {
		end = time.Now()
	}
	if req.FormValue("start") != "" {
		start, err = time.ParseInLocation(time.RFC3339, req.FormValue("start"), sctx.GetSessionVars().Location())
		if err != nil {
			handler.WriteError(w, err)
			return
		}
	} else {
		start = end.Add(-time.Minute * 10)
	}
	valueTp := req.FormValue("type")
	pb, err := executor.NewProfileBuilder(sctx, start, end, valueTp)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	err = pb.Collect()
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	_, err = w.Write(pb.Build())
	terror.Log(errors.Trace(err))
}

// TestHandler is the handler for tests. It's convenient to provide some APIs for integration tests.
type TestHandler struct {
	*handler.TikvHandlerTool
	gcIsRunning uint32
}

// NewTestHandler creates a new TestHandler.
func NewTestHandler(tool *handler.TikvHandlerTool, gcIsRunning uint32) *TestHandler {
	return &TestHandler{
		TikvHandlerTool: tool,
		gcIsRunning:     gcIsRunning,
	}
}

// ServeHTTP handles test related requests.
func (h *TestHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	params := mux.Vars(req)
	mod := strings.ToLower(params["mod"])
	op := strings.ToLower(params["op"])

	switch mod {
	case "gc":
		h.handleGC(op, w, req)
	default:
		handler.WriteError(w, errors.NotSupportedf("module(%s)", mod))
	}
}

// Supported operations:
//   - resolvelock?safepoint={uint64}&physical={bool}:
//   - safepoint: resolve all locks whose timestamp is less than the safepoint.
//   - physical: whether it uses physical(green GC) mode to scan locks. Default is true.
func (h *TestHandler) handleGC(op string, w http.ResponseWriter, req *http.Request) {
	if !atomic.CompareAndSwapUint32(&h.gcIsRunning, 0, 1) {
		handler.WriteError(w, errors.New("GC is running"))
		return
	}
	defer atomic.StoreUint32(&h.gcIsRunning, 0)

	switch op {
	case "resolvelock":
		h.handleGCResolveLocks(w, req)
	default:
		handler.WriteError(w, errors.NotSupportedf("operation(%s)", op))
	}
}

func (h *TestHandler) handleGCResolveLocks(w http.ResponseWriter, req *http.Request) {
	s := req.FormValue("safepoint")
	safePoint, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		handler.WriteError(w, errors.Errorf("parse safePoint(%s) failed", s))
		return
	}
	ctx := req.Context()
	logutil.Logger(ctx).Info("start resolving locks", zap.Uint64("safePoint", safePoint))
	err = gcworker.RunResolveLocks(ctx, h.Store, h.RegionCache.PDClient(), safePoint, "testGCWorker", 3)
	if err != nil {
		handler.WriteError(w, errors.Annotate(err, "resolveLocks failed"))
	}
}

// ServeHTTP handles request of resigning ddl owner.
func (DDLHookHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		handler.WriteError(w, errors.Errorf("This api only support POST method"))
		return
	}

	hook := req.FormValue("ddl_hook")
	switch hook {
	case "ctc_hook":
		err := failpoint.EnableCall("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
			log.Info("on job run before", zap.String("job", job.String()))
			// Only block the ctc type ddl here.
			if job.Type != model.ActionModifyColumn {
				return
			}
			switch job.SchemaState {
			case model.StateDeleteOnly, model.StateWriteOnly, model.StateWriteReorganization:
				log.Warn(fmt.Sprintf("[DDL_HOOK] Hang for 0.5 seconds on %s state triggered", job.SchemaState.String()))
				time.Sleep(500 * time.Millisecond)
			}
		})
		if err != nil {
			handler.WriteError(w, err)
			return
		}
	case "default_hook":
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep")
	}

	handler.WriteData(w, "success!")

	ctx := req.Context()
	logutil.Logger(ctx).Info("change ddl hook success", zap.String("to_ddl_hook", req.FormValue("ddl_hook")))
}

// ServeHTTP handles request of set server labels.
func (LabelHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		handler.WriteError(w, errors.Errorf("This api only support POST method"))
		return
	}

	labels := make(map[string]string)
	err := json.NewDecoder(req.Body).Decode(&labels)
	if err != nil {
		handler.WriteError(w, err)
		return
	}

	if len(labels) > 0 {
		cfg := *config.GetGlobalConfig()
		// Be careful of data race. The key & value of cfg.Labels must not be changed.
		if cfg.Labels != nil {
			for k, v := range cfg.Labels {
				if _, found := labels[k]; !found {
					labels[k] = v
				}
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), requestDefaultTimeout)
		if err := infosync.UpdateServerLabel(ctx, labels); err != nil {
			logutil.BgLogger().Error("update etcd labels failed", zap.Any("labels", cfg.Labels), zap.Error(err))
		}
		cancel()
		cfg.Labels = labels
		config.StoreGlobalConfig(&cfg)
		logutil.BgLogger().Info("update server labels", zap.Any("labels", cfg.Labels))
	}

	handler.WriteData(w, config.GetGlobalConfig().Labels)
}

// IngestParam is the type for lightning ingest parameters.
type IngestParam string

const (
	// IngestParamMaxBatchSplitRanges is the parameter for lightning max_batch_split_ranges.
	IngestParamMaxBatchSplitRanges IngestParam = "max_batch_split_ranges"
	// IngestParamMaxSplitRangesPerSec is the parameter for lightning max_split_ranges_per_sec.
	IngestParamMaxSplitRangesPerSec IngestParam = "max_split_ranges_per_sec"
	// IngestParamMaxInflight is the parameter for lightning max_inflight.
	IngestParamMaxInflight IngestParam = "max_inflight"
	// IngestParamMaxPerSecond is the parameter for lightning max_per_second.
	IngestParamMaxPerSecond IngestParam = "max_per_second"
)

// IngestConcurrencyHandler is the handler for lightning max_batch_split_ranges and max_inflight.
type IngestConcurrencyHandler struct {
	*handler.TikvHandlerTool
	param IngestParam
}

// NewIngestConcurrencyHandler creates a new IngestConcurrencyHandler.
func NewIngestConcurrencyHandler(tool *handler.TikvHandlerTool, param IngestParam) IngestConcurrencyHandler {
	return IngestConcurrencyHandler{tool, param}
}

// ServeHTTP handles request of lightning max_batch_split_ranges.
func (h IngestConcurrencyHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var getter func(*meta.Mutator) (float64, bool, error)
	var setter func(*meta.Mutator, float64) error
	var updateGlobal func(v float64) float64
	switch h.param {
	case IngestParamMaxBatchSplitRanges:
		getter = func(m *meta.Mutator) (float64, bool, error) {
			v, isNull, err := m.GetIngestMaxBatchSplitRanges()
			return float64(v), isNull, err
		}
		setter = func(m *meta.Mutator, value float64) error {
			return m.SetIngestMaxBatchSplitRanges(int(value))
		}
		updateGlobal = func(v float64) float64 {
			old := local.CurrentMaxBatchSplitRanges.Load()
			intV := int(v)
			local.CurrentMaxBatchSplitRanges.Store(&intV)
			return float64(*old)
		}
	case IngestParamMaxSplitRangesPerSec:
		getter = func(m *meta.Mutator) (float64, bool, error) {
			return m.GetIngestMaxSplitRangesPerSec()
		}
		setter = func(m *meta.Mutator, value float64) error {
			return m.SetIngestMaxSplitRangesPerSec(value)
		}
		updateGlobal = func(v float64) float64 {
			old := local.CurrentMaxSplitRangesPerSec.Load()
			local.CurrentMaxSplitRangesPerSec.Store(&v)
			return *old
		}
	case IngestParamMaxPerSecond:
		getter = func(m *meta.Mutator) (float64, bool, error) {
			return m.GetIngestMaxPerSec()
		}
		setter = func(m *meta.Mutator, value float64) error {
			return m.SetIngestMaxPerSec(value)
		}
		updateGlobal = func(v float64) float64 {
			old := local.CurrentMaxIngestPerSec.Load()
			local.CurrentMaxIngestPerSec.Store(&v)
			return *old
		}
	case IngestParamMaxInflight:
		getter = func(m *meta.Mutator) (float64, bool, error) {
			v, isNull, err := m.GetIngestMaxInflight()
			return float64(v), isNull, err
		}
		setter = func(m *meta.Mutator, value float64) error {
			return m.SetIngestMaxInflight(int(value))
		}
		updateGlobal = func(v float64) float64 {
			old := local.CurrentMaxIngestInflight.Load()
			intV := int(v)
			local.CurrentMaxIngestInflight.Store(&intV)
			return float64(*old)
		}
	default:
		handler.WriteError(w, errors.Errorf("unsupported ingest parameter: %s", h.param))
	}
	switch req.Method {
	case http.MethodGet:
		var respValue float64
		var respIsNull bool
		err := kv.RunInNewTxn(context.Background(), h.Store.(kv.Storage), false, func(_ context.Context, txn kv.Transaction) error {
			m := meta.NewMutator(txn)
			var getErr error
			respValue, respIsNull, getErr = getter(m)
			return getErr
		})

		if err != nil {
			handler.WriteError(w, err)
			return
		}

		data := map[string]any{
			"value":   respValue,
			"is_null": respIsNull,
		}
		handler.WriteData(w, data)
	case http.MethodPost:
		var payload struct {
			Value float64 `json:"value"`
		}
		if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
			handler.WriteError(w, err)
			return
		}
		newValue := payload.Value
		if newValue < 0 {
			handler.WriteError(w, errors.New("value must be >= 0"))
			return
		}
		err := kv.RunInNewTxn(context.Background(), h.Store.(kv.Storage), true, func(_ context.Context, txn kv.Transaction) error {
			m := meta.NewMutator(txn)
			return setter(m, newValue)
		})

		if err != nil {
			handler.WriteError(w, err)
			return
		}
		oldVal := updateGlobal(newValue)
		logutil.BgLogger().Info("set ingest concurrency",
			zap.String("param", string(h.param)),
			zap.Float64("oldValue", oldVal),
			zap.Float64("newValue", newValue))
		handler.WriteData(w, map[string]string{"message": "success"})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		handler.WriteError(w, errors.New("method not allowed"))
	}
}

// TxnGCStatesHandler is the handler for GC related API.
type TxnGCStatesHandler struct {
	store kv.Storage
}

// NewTxnGCStatesHandler creates a TxnGCStatesHandler.
func NewTxnGCStatesHandler(store kv.Storage) *TxnGCStatesHandler {
	return &TxnGCStatesHandler{
		store: store,
	}
}

// ServeHTTP implements the HTTP handler interface.
func (gc *TxnGCStatesHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		http.Error(w, "This API only supports GET method", http.StatusMethodNotAllowed)
		return
	}

	pdStoreBackend, ok := gc.store.(kv.StorageWithPD)
	if !ok {
		handler.WriteError(w, errors.New("GC API only support storage with PD"))
		return
	}

	pdCli := pdStoreBackend.GetPDClient()
	keyspaceID := gc.store.GetCodec().GetKeyspaceID()
	gcCli := pdCli.GetGCStatesClient(uint32(keyspaceID))
	state, err := gcCli.GetGCState(context.Background())
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	handler.WriteData(w, state)
}
