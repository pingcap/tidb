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
	"encoding/base64"
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
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/session/txninfo"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/binloginfo"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/gcworker"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/deadlockhistory"
	"github.com/pingcap/tidb/pkg/util/gcutil"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

// SettingsHandler is the handler for list tidb server settings.
type SettingsHandler struct {
	*handler.TikvHandlerTool
}

// NewSettingsHandler creates a new SettingsHandler.
func NewSettingsHandler(tool *handler.TikvHandlerTool) *SettingsHandler {
	return &SettingsHandler{tool}
}

// BinlogRecover is used to recover binlog service.
// When config binlog IgnoreError, binlog service will stop after meeting the first error.
// It can be recovered using HTTP API.
type BinlogRecover struct{}

// SchemaHandler is the handler for list database or table schemas.
type SchemaHandler struct {
	*handler.TikvHandlerTool
}

// NewSchemaHandler creates a new SchemaHandler.
func NewSchemaHandler(tool *handler.TikvHandlerTool) *SchemaHandler {
	return &SchemaHandler{tool}
}

// SchemaStorageHandler is the handler for list database or table schemas.
type SchemaStorageHandler struct {
	*handler.TikvHandlerTool
}

// NewSchemaStorageHandler creates a new SchemaStorageHandler.
func NewSchemaStorageHandler(tool *handler.TikvHandlerTool) *SchemaStorageHandler {
	return &SchemaStorageHandler{tool}
}

// DBTableHandler is the handler for list table's regions.
type DBTableHandler struct {
	*handler.TikvHandlerTool
}

// NewDBTableHandler creates a new DBTableHandler.
func NewDBTableHandler(tool *handler.TikvHandlerTool) *DBTableHandler {
	return &DBTableHandler{tool}
}

// FlashReplicaHandler is the handler for flash replica.
type FlashReplicaHandler struct {
	*handler.TikvHandlerTool
}

// NewFlashReplicaHandler creates a new FlashReplicaHandler.
func NewFlashReplicaHandler(tool *handler.TikvHandlerTool) *FlashReplicaHandler {
	return &FlashReplicaHandler{tool}
}

// RegionHandler is the common field for http handler. It contains
// some common functions for all handlers.
type RegionHandler struct {
	*handler.TikvHandlerTool
}

// NewRegionHandler creates a new RegionHandler.
func NewRegionHandler(tool *handler.TikvHandlerTool) *RegionHandler {
	return &RegionHandler{tool}
}

// TableHandler is the handler for list table's regions.
type TableHandler struct {
	*handler.TikvHandlerTool
	op string
}

// NewTableHandler creates a new TableHandler.
func NewTableHandler(tool *handler.TikvHandlerTool, op string) *TableHandler {
	return &TableHandler{tool, op}
}

// DDLHistoryJobHandler is the handler for list job history.
type DDLHistoryJobHandler struct {
	*handler.TikvHandlerTool
}

// NewDDLHistoryJobHandler creates a new DDLHistoryJobHandler.
func NewDDLHistoryJobHandler(tool *handler.TikvHandlerTool) *DDLHistoryJobHandler {
	return &DDLHistoryJobHandler{tool}
}

// DDLResignOwnerHandler is the handler for resigning ddl owner.
type DDLResignOwnerHandler struct {
	store kv.Storage
}

// NewDDLResignOwnerHandler creates a new DDLResignOwnerHandler.
func NewDDLResignOwnerHandler(store kv.Storage) *DDLResignOwnerHandler {
	return &DDLResignOwnerHandler{store}
}

// ServerInfoHandler is the handler for getting statistics.
type ServerInfoHandler struct {
	*handler.TikvHandlerTool
}

// NewServerInfoHandler creates a new ServerInfoHandler.
func NewServerInfoHandler(tool *handler.TikvHandlerTool) *ServerInfoHandler {
	return &ServerInfoHandler{tool}
}

// AllServerInfoHandler is the handler for getting all servers information.
type AllServerInfoHandler struct {
	*handler.TikvHandlerTool
}

// NewAllServerInfoHandler creates a new AllServerInfoHandler.
func NewAllServerInfoHandler(tool *handler.TikvHandlerTool) *AllServerInfoHandler {
	return &AllServerInfoHandler{tool}
}

// ProfileHandler is the handler for getting profile.
type ProfileHandler struct {
	*handler.TikvHandlerTool
}

// NewProfileHandler creates a new ProfileHandler.
func NewProfileHandler(tool *handler.TikvHandlerTool) *ProfileHandler {
	return &ProfileHandler{tool}
}

// DDLHookHandler is the handler for use pre-defined ddl callback.
// It's convenient to provide some APIs for integration tests.
type DDLHookHandler struct {
	store kv.Storage
}

// NewDDLHookHandler creates a new DDLHookHandler.
func NewDDLHookHandler(store kv.Storage) *DDLHookHandler {
	return &DDLHookHandler{store}
}

// ValueHandler is the handler for get value.
type ValueHandler struct {
}

// LabelHandler is the handler for set labels
type LabelHandler struct{}

const (
	// OpTableRegions is the operation for getting regions of a table.
	OpTableRegions = "regions"
	// OpTableRanges is the operation for getting ranges of a table.
	OpTableRanges = "ranges"
	// OpTableDiskUsage is the operation for getting disk usage of a table.
	OpTableDiskUsage = "disk-usage"
	// OpTableScatter is the operation for scattering a table.
	OpTableScatter = "scatter-table"
	// OpStopTableScatter is the operation for stopping scattering a table.
	OpStopTableScatter = "stop-scatter-table"
)

// MvccTxnHandler is the handler for txn debugger.
type MvccTxnHandler struct {
	*handler.TikvHandlerTool
	op string
}

// NewMvccTxnHandler creates a new MvccTxnHandler.
func NewMvccTxnHandler(tool *handler.TikvHandlerTool, op string) *MvccTxnHandler {
	return &MvccTxnHandler{tool, op}
}

const (
	// OpMvccGetByHex is the operation for getting mvcc value by hex format.
	OpMvccGetByHex = "hex"
	// OpMvccGetByKey is the operation for getting mvcc value by key.
	OpMvccGetByKey = "key"
	// OpMvccGetByIdx is the operation for getting mvcc value by idx.
	OpMvccGetByIdx = "idx"
	// OpMvccGetByTxn is the operation for getting mvcc value by txn.
	OpMvccGetByTxn = "txn"
)

// ServeHTTP handles request of list a database or table's schemas.
func (ValueHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// parse params
	params := mux.Vars(req)

	colID, err := strconv.ParseInt(params[handler.ColumnID], 0, 64)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	colTp, err := strconv.ParseInt(params[handler.ColumnTp], 0, 64)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	colFlag, err := strconv.ParseUint(params[handler.ColumnFlag], 0, 64)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	colLen, err := strconv.ParseInt(params[handler.ColumnLen], 0, 64)
	if err != nil {
		handler.WriteError(w, err)
		return
	}

	// Get the unchanged binary.
	if req.URL == nil {
		err = errors.BadRequestf("Invalid URL")
		handler.WriteError(w, err)
		return
	}
	values := make(url.Values)

	err = parseQuery(req.URL.RawQuery, values, false)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	if len(values[handler.RowBin]) != 1 {
		err = errors.BadRequestf("Invalid Query:%v", values[handler.RowBin])
		handler.WriteError(w, err)
		return
	}
	bin := values[handler.RowBin][0]
	valData, err := base64.StdEncoding.DecodeString(bin)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	// Construct field type.
	defaultDecimal := 6
	ft := types.NewFieldTypeBuilder().SetType(byte(colTp)).SetFlag(uint(colFlag)).SetFlen(int(colLen)).SetDecimal(defaultDecimal).BuildP()
	// Decode a column.
	m := make(map[int64]*types.FieldType, 1)
	m[colID] = ft
	loc := time.UTC
	vals, err := tablecodec.DecodeRowToDatumMap(valData, m, loc)
	if err != nil {
		handler.WriteError(w, err)
		return
	}

	v := vals[colID]
	val, err := v.ToString()
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	handler.WriteData(w, val)
}

// TableRegions is the response data for list table's regions.
// It contains regions list for record and indices.
type TableRegions struct {
	TableName     string               `json:"name"`
	TableID       int64                `json:"id"`
	RecordRegions []handler.RegionMeta `json:"record_regions"`
	Indices       []IndexRegions       `json:"indices"`
}

// RangeDetail contains detail information about a particular range
type RangeDetail struct {
	StartKey    []byte `json:"start_key"`
	EndKey      []byte `json:"end_key"`
	StartKeyHex string `json:"start_key_hex"`
	EndKeyHex   string `json:"end_key_hex"`
}

func createRangeDetail(start, end []byte) RangeDetail {
	return RangeDetail{
		StartKey:    start,
		EndKey:      end,
		StartKeyHex: hex.EncodeToString(start),
		EndKeyHex:   hex.EncodeToString(end),
	}
}

// TableRanges is the response data for list table's ranges.
// It contains ranges list for record and indices as well as the whole table.
type TableRanges struct {
	TableName string                 `json:"name"`
	TableID   int64                  `json:"id"`
	Range     RangeDetail            `json:"table"`
	Record    RangeDetail            `json:"record"`
	Index     RangeDetail            `json:"index"`
	Indices   map[string]RangeDetail `json:"indices,omitempty"`
}

// IndexRegions is the region info for one index.
type IndexRegions struct {
	Name    string               `json:"name"`
	ID      int64                `json:"id"`
	Regions []handler.RegionMeta `json:"regions"`
}

// RegionDetail is the response data for get region by ID
// it includes indices and records detail in current region.
type RegionDetail struct {
	RangeDetail `json:",inline"`
	RegionID    uint64              `json:"region_id"`
	Frames      []*helper.FrameItem `json:"frames"`
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
		} else if f := r.GetIndexFrame(tID, index.ID, dbName, tName, index.Name.String()); f != nil {
			rt.Frames = append(rt.Frames, f)
		}
	}

	if pi != nil {
		for _, def := range pi.Definitions {
			if f := r.GetRecordFrame(def.ID, dbName, fmt.Sprintf("%s(%s)", tName, def.Name.O), isCommonHandle); f != nil {
				rt.Frames = append(rt.Frames, f)
			}
		}
	} else if f := r.GetRecordFrame(tID, dbName, tName, isCommonHandle); f != nil {
		rt.Frames = append(rt.Frames, f)
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

// ServeHTTP handles request of list tidb server settings.
func (h SettingsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method == "POST" {
		err := req.ParseForm()
		if err != nil {
			handler.WriteError(w, err)
			return
		}
		if levelStr := req.Form.Get("log_level"); levelStr != "" {
			err1 := logutil.SetLevel(levelStr)
			if err1 != nil {
				handler.WriteError(w, err1)
				return
			}

			config.GetGlobalConfig().Log.Level = levelStr
		}
		if generalLog := req.Form.Get("tidb_general_log"); generalLog != "" {
			switch generalLog {
			case "0":
				variable.ProcessGeneralLog.Store(false)
			case "1":
				variable.ProcessGeneralLog.Store(true)
			default:
				handler.WriteError(w, errors.New("illegal argument"))
				return
			}
		}
		if asyncCommit := req.Form.Get("tidb_enable_async_commit"); asyncCommit != "" {
			s, err := session.CreateSession(h.Store)
			if err != nil {
				handler.WriteError(w, err)
				return
			}
			defer s.Close()

			switch asyncCommit {
			case "0":
				err = s.GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.TiDBEnableAsyncCommit, variable.Off)
			case "1":
				err = s.GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.TiDBEnableAsyncCommit, variable.On)
			default:
				handler.WriteError(w, errors.New("illegal argument"))
				return
			}
			if err != nil {
				handler.WriteError(w, err)
				return
			}
		}
		if onePC := req.Form.Get("tidb_enable_1pc"); onePC != "" {
			s, err := session.CreateSession(h.Store)
			if err != nil {
				handler.WriteError(w, err)
				return
			}
			defer s.Close()

			switch onePC {
			case "0":
				err = s.GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.TiDBEnable1PC, variable.Off)
			case "1":
				err = s.GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.TiDBEnable1PC, variable.On)
			default:
				handler.WriteError(w, errors.New("illegal argument"))
				return
			}
			if err != nil {
				handler.WriteError(w, err)
				return
			}
		}
		if ddlSlowThreshold := req.Form.Get("ddl_slow_threshold"); ddlSlowThreshold != "" {
			threshold, err1 := strconv.Atoi(ddlSlowThreshold)
			if err1 != nil {
				handler.WriteError(w, err1)
				return
			}
			if threshold > 0 {
				atomic.StoreUint32(&variable.DDLSlowOprThreshold, uint32(threshold))
			}
		}
		if checkMb4ValueInUtf8 := req.Form.Get("check_mb4_value_in_utf8"); checkMb4ValueInUtf8 != "" {
			switch checkMb4ValueInUtf8 {
			case "0":
				config.GetGlobalConfig().Instance.CheckMb4ValueInUTF8.Store(false)
			case "1":
				config.GetGlobalConfig().Instance.CheckMb4ValueInUTF8.Store(true)
			default:
				handler.WriteError(w, errors.New("illegal argument"))
				return
			}
		}
		if deadlockHistoryCapacity := req.Form.Get("deadlock_history_capacity"); deadlockHistoryCapacity != "" {
			capacity, err := strconv.Atoi(deadlockHistoryCapacity)
			if err != nil {
				handler.WriteError(w, errors.New("illegal argument"))
				return
			} else if capacity < 0 || capacity > 10000 {
				handler.WriteError(w, errors.New("deadlock_history_capacity out of range, should be in 0 to 10000"))
				return
			}
			cfg := config.GetGlobalConfig()
			cfg.PessimisticTxn.DeadlockHistoryCapacity = uint(capacity)
			config.StoreGlobalConfig(cfg)
			deadlockhistory.GlobalDeadlockHistory.Resize(uint(capacity))
		}
		if deadlockCollectRetryable := req.Form.Get("deadlock_history_collect_retryable"); deadlockCollectRetryable != "" {
			collectRetryable, err := strconv.ParseBool(deadlockCollectRetryable)
			if err != nil {
				handler.WriteError(w, errors.New("illegal argument"))
				return
			}
			cfg := config.GetGlobalConfig()
			cfg.PessimisticTxn.DeadlockHistoryCollectRetryable = collectRetryable
			config.StoreGlobalConfig(cfg)
		}
		if mutationChecker := req.Form.Get("tidb_enable_mutation_checker"); mutationChecker != "" {
			s, err := session.CreateSession(h.Store)
			if err != nil {
				handler.WriteError(w, err)
				return
			}
			defer s.Close()

			switch mutationChecker {
			case "0":
				err = s.GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.TiDBEnableMutationChecker, variable.Off)
			case "1":
				err = s.GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.TiDBEnableMutationChecker, variable.On)
			default:
				handler.WriteError(w, errors.New("illegal argument"))
				return
			}
			if err != nil {
				handler.WriteError(w, err)
				return
			}
		}
		if transactionSummaryCapacity := req.Form.Get("transaction_summary_capacity"); transactionSummaryCapacity != "" {
			capacity, err := strconv.Atoi(transactionSummaryCapacity)
			if err != nil {
				handler.WriteError(w, errors.New("illegal argument"))
				return
			} else if capacity < 0 || capacity > 5000 {
				handler.WriteError(w, errors.New("transaction_summary_capacity out of range, should be in 0 to 5000"))
				return
			}
			cfg := config.GetGlobalConfig()
			cfg.TrxSummary.TransactionSummaryCapacity = uint(capacity)
			config.StoreGlobalConfig(cfg)
			txninfo.Recorder.ResizeSummaries(uint(capacity))
		}
		if transactionIDDigestMinDuration := req.Form.Get("transaction_id_digest_min_duration"); transactionIDDigestMinDuration != "" {
			duration, err := strconv.Atoi(transactionIDDigestMinDuration)
			if err != nil {
				handler.WriteError(w, errors.New("illegal argument"))
				return
			} else if duration < 0 || duration > 2147483647 {
				handler.WriteError(w, errors.New("transaction_id_digest_min_duration out of range, should be in 0 to 2147483647"))
				return
			}
			cfg := config.GetGlobalConfig()
			cfg.TrxSummary.TransactionIDDigestMinDuration = uint(duration)
			config.StoreGlobalConfig(cfg)
			txninfo.Recorder.SetMinDuration(time.Duration(duration) * time.Millisecond)
		}
	} else {
		handler.WriteData(w, config.GetGlobalConfig())
	}
}

// ServeHTTP recovers binlog service.
func (BinlogRecover) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	op := req.FormValue(handler.Operation)
	switch op {
	case "reset":
		binloginfo.ResetSkippedCommitterCounter()
	case "nowait":
		err := binloginfo.DisableSkipBinlogFlag()
		if err != nil {
			handler.WriteError(w, err)
			return
		}
	case "status":
	default:
		sec, err := strconv.ParseInt(req.FormValue(handler.Seconds), 10, 64)
		if sec <= 0 || err != nil {
			sec = 1800
		}
		err = binloginfo.DisableSkipBinlogFlag()
		if err != nil {
			handler.WriteError(w, err)
			return
		}
		timeout := time.Duration(sec) * time.Second
		err = binloginfo.WaitBinlogRecover(timeout)
		if err != nil {
			handler.WriteError(w, err)
			return
		}
	}
	handler.WriteData(w, binloginfo.GetBinlogStatus())
}

// TableFlashReplicaInfo is the replica information of a table.
type TableFlashReplicaInfo struct {
	// Modifying the field name needs to negotiate with TiFlash colleague.
	ID             int64    `json:"id"`
	ReplicaCount   uint64   `json:"replica_count"`
	LocationLabels []string `json:"location_labels"`
	Available      bool     `json:"available"`
	HighPriority   bool     `json:"high_priority"`
}

// ServeHTTP implements the HTTPHandler interface.
func (h FlashReplicaHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		h.handleStatusReport(w, req)
		return
	}
	schema, err := h.Schema()
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	replicaInfos := make([]*TableFlashReplicaInfo, 0)
	allDBs := schema.AllSchemaNames()
	for _, db := range allDBs {
		tbls := schema.SchemaTables(db)
		for _, tbl := range tbls {
			replicaInfos = h.getTiFlashReplicaInfo(tbl.Meta(), replicaInfos)
		}
	}
	dropedOrTruncateReplicaInfos, err := h.getDropOrTruncateTableTiflash(schema)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	replicaInfos = append(replicaInfos, dropedOrTruncateReplicaInfos...)
	handler.WriteData(w, replicaInfos)
}

func (FlashReplicaHandler) getTiFlashReplicaInfo(tblInfo *model.TableInfo, replicaInfos []*TableFlashReplicaInfo) []*TableFlashReplicaInfo {
	if tblInfo.TiFlashReplica == nil {
		return replicaInfos
	}
	if pi := tblInfo.GetPartitionInfo(); pi != nil {
		for _, p := range pi.Definitions {
			replicaInfos = append(replicaInfos, &TableFlashReplicaInfo{
				ID:             p.ID,
				ReplicaCount:   tblInfo.TiFlashReplica.Count,
				LocationLabels: tblInfo.TiFlashReplica.LocationLabels,
				Available:      tblInfo.TiFlashReplica.IsPartitionAvailable(p.ID),
			})
		}
		for _, p := range pi.AddingDefinitions {
			replicaInfos = append(replicaInfos, &TableFlashReplicaInfo{
				ID:             p.ID,
				ReplicaCount:   tblInfo.TiFlashReplica.Count,
				LocationLabels: tblInfo.TiFlashReplica.LocationLabels,
				Available:      tblInfo.TiFlashReplica.IsPartitionAvailable(p.ID),
				HighPriority:   true,
			})
		}
		return replicaInfos
	}
	replicaInfos = append(replicaInfos, &TableFlashReplicaInfo{
		ID:             tblInfo.ID,
		ReplicaCount:   tblInfo.TiFlashReplica.Count,
		LocationLabels: tblInfo.TiFlashReplica.LocationLabels,
		Available:      tblInfo.TiFlashReplica.Available,
	})
	return replicaInfos
}

func (h FlashReplicaHandler) getDropOrTruncateTableTiflash(currentSchema infoschema.InfoSchema) ([]*TableFlashReplicaInfo, error) {
	s, err := session.CreateSession(h.Store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer s.Close()

	store := domain.GetDomain(s).Store()
	txn, err := store.Begin()
	if err != nil {
		return nil, errors.Trace(err)
	}
	gcSafePoint, err := gcutil.GetGCSafePoint(s)
	if err != nil {
		return nil, err
	}
	replicaInfos := make([]*TableFlashReplicaInfo, 0)
	uniqueIDMap := make(map[int64]struct{})
	handleJobAndTableInfo := func(_ *model.Job, tblInfo *model.TableInfo) (bool, error) {
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
	err = ddl.IterAllDDLJobs(s, txn, fn)
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

func (h FlashReplicaHandler) handleStatusReport(w http.ResponseWriter, req *http.Request) {
	var status tableFlashReplicaStatus
	err := json.NewDecoder(req.Body).Decode(&status)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	do, err := session.GetDomain(h.Store)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	s, err := session.CreateSession(h.Store)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	defer s.Close()

	available := status.checkTableFlashReplicaAvailable()
	err = do.DDL().UpdateTableReplicaInfo(s, status.ID, available)
	if err != nil {
		handler.WriteError(w, err)
	}
	if available {
		var tableInfo model.TableInfo
		tableInfo.ID = status.ID
		err = infosync.DeleteTiFlashTableSyncProgress(&tableInfo)
	} else {
		progress := float64(status.FlashRegionCount) / float64(status.RegionCount)
		err = infosync.UpdateTiFlashProgressCache(status.ID, progress)
	}
	if err != nil {
		handler.WriteError(w, err)
	}

	logutil.BgLogger().Info("handle flash replica report", zap.Int64("table ID", status.ID), zap.Uint64("region count",
		status.RegionCount),
		zap.Uint64("flash region count", status.FlashRegionCount),
		zap.Error(err))
}

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

func getSchemaTablesStorageInfo(h *SchemaStorageHandler, schema *model.CIStr, table *model.CIStr) (messages []*SchemaTableStorage, err error) {
	var s sessiontypes.Session
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

			for i := 0; i < req.NumRows(); i++ {
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
		dbName    *model.CIStr
		tableName *model.CIStr
		isSingle  bool
	)

	if reqDbName, ok := params[handler.DBName]; ok {
		cDBName := model.NewCIStr(reqDbName)
		// all table schemas in a specified database
		schemaInfo, exists := schema.SchemaByName(cDBName)
		if !exists {
			handler.WriteError(w, infoschema.ErrDatabaseNotExists.GenWithStackByArgs(reqDbName))
			return
		}
		dbName = &schemaInfo.Name

		if reqTableName, ok := params[handler.TableName]; ok {
			// table schema of a specified table name
			cTableName := model.NewCIStr(reqTableName)
			data, e := schema.TableByName(cDBName, cTableName)
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

// WriteDBTablesData writes all the table data in a database. The format is the marshal result of []*model.TableInfo, you can
// unmarshal it to []*model.TableInfo. In this function, we manually construct the marshal result so that the memory
// can be deallocated quickly.
// For every table in the input, we marshal them. The result such as {tb1} {tb2} {tb3}.
// Then we add some bytes to make it become [{tb1}, {tb2}, {tb3}], so we can unmarshal it to []*model.TableInfo.
// Note: It would return StatusOK even if errors occur. But if errors occur, there must be some bugs.
func WriteDBTablesData(w http.ResponseWriter, tbs []*model.TableInfo) {
	if len(tbs) == 0 {
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
	for _, tb := range tbs {
		if init {
			_, err = w.Write(hack.Slice(",\n"))
			if err != nil {
				terror.Log(errors.Trace(err))
				return
			}
		} else {
			init = true
		}
		js, err := json.MarshalIndent(tb, "", " ")
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
		cDBName := model.NewCIStr(dbName)
		if tableName, ok := params[handler.TableName]; ok {
			// table schema of a specified table name
			cTableName := model.NewCIStr(tableName)
			data, err := schema.TableByName(cDBName, cTableName)
			if err != nil {
				handler.WriteError(w, err)
				return
			}
			handler.WriteData(w, data.Meta())
			return
		}
		// all table schemas in a specified database
		if schema.SchemaExists(cDBName) {
			tbs := schema.SchemaTableInfos(cDBName)
			WriteDBTablesData(w, tbs)
			return
		}
		handler.WriteError(w, infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName))
		return
	}

	if tableID := req.FormValue(handler.TableIDQuery); len(tableID) > 0 {
		// table schema of a specified tableID
		tid, err := strconv.Atoi(tableID)
		if err != nil {
			handler.WriteError(w, err)
			return
		}
		if tid < 0 {
			handler.WriteError(w, infoschema.ErrTableNotExists.GenWithStack("Table which ID = %s does not exist.", tableID))
			return
		}
		if data, ok := schema.TableByID(int64(tid)); ok {
			handler.WriteData(w, data.Meta())
			return
		}
		// The tid maybe a partition ID of the partition-table.
		tbl, _, _ := schema.FindTableByPartitionID(int64(tid))
		if tbl == nil {
			handler.WriteError(w, infoschema.ErrTableNotExists.GenWithStack("Table which ID = %s does not exist.", tableID))
			return
		}
		handler.WriteData(w, tbl)
		return
	}

	// all databases' schemas
	handler.WriteData(w, schema.AllSchemas())
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
	tableVal, err := schema.TableByName(model.NewCIStr(dbName), model.NewCIStr(tableName))
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
	var jobID, limitID int
	var err error
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
		if limitID < 1 {
			handler.WriteError(w, errors.New("ddl history limit must be greater than 0"))
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
	txnMeta := meta.NewMeta(txn)

	if jobID == 0 && limit == 0 {
		jobs, err = ddl.GetAllHistoryDDLJobs(txnMeta)
	} else {
		jobs, err = ddl.ScanHistoryDDLJobs(txnMeta, int64(jobID), limit)
	}
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
	regions, err := pdCli.ScanRegions(ctx, startKey, endKey, -1)
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
		regions, err := pdCli.ScanRegions(ctx, startKey, endKey, -1)
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
			hotRead, err := h.ScrapeHotInfo(ctx, helper.HotRead, schema.AllSchemas())
			if err != nil {
				handler.WriteError(w, err)
				return
			}
			hotWrite, err := h.ScrapeHotInfo(ctx, helper.HotWrite, schema.AllSchemas())
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
		if util.IsMemDB(dbName.L) {
			continue
		}
		tables := schema.SchemaTables(dbName)
		for _, tableVal := range tables {
			regionDetail.addTableInRange(dbName.String(), tableVal.Meta(), frameRange)
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
	*infosync.ServerInfo
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
	ServersNum                   int                             `json:"servers_num,omitempty"`
	OwnerID                      string                          `json:"owner_id"`
	IsAllServerVersionConsistent bool                            `json:"is_all_server_version_consistent,omitempty"`
	AllServersDiffVersions       []infosync.ServerVersionInfo    `json:"all_servers_diff_versions,omitempty"`
	AllServersInfo               map[string]*infosync.ServerInfo `json:"all_servers_info,omitempty"`
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
	allVersionsMap := map[infosync.ServerVersionInfo]struct{}{}
	allVersions := make([]infosync.ServerVersionInfo, 0, len(allServersInfo))
	for _, v := range allServersInfo {
		if _, ok := allVersionsMap[v.ServerVersionInfo]; ok {
			continue
		}
		allVersionsMap[v.ServerVersionInfo] = struct{}{}
		allVersions = append(allVersions, v.ServerVersionInfo)
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
	tbl, ok := schema.TableByID(int64(physicalID))
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
func (h DDLHookHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		handler.WriteError(w, errors.Errorf("This api only support POST method"))
		return
	}

	dom, err := session.GetDomain(h.store)
	if err != nil {
		log.Error("failed to get session domain", zap.Error(err))
		handler.WriteError(w, err)
	}

	newCallbackFunc, err := ddl.GetCustomizedHook(req.FormValue("ddl_hook"))
	if err != nil {
		log.Error("failed to get customized hook", zap.Error(err))
		handler.WriteError(w, err)
	}
	callback := newCallbackFunc(dom)

	dom.DDL().SetHook(callback)
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
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
