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
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemacontext "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/session/txninfo"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/deadlockhistory"
	"github.com/pingcap/tidb/pkg/util/gcutil"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const requestDefaultTimeout = 10 * time.Second

// SettingsHandler is the handler for list tidb server settings.
type SettingsHandler struct {
	*handler.TikvHandlerTool
}

// NewSettingsHandler creates a new SettingsHandler.
func NewSettingsHandler(tool *handler.TikvHandlerTool) *SettingsHandler {
	return &SettingsHandler{tool}
}

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
type DDLHookHandler struct{}

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
				vardef.ProcessGeneralLog.Store(false)
			case "1":
				vardef.ProcessGeneralLog.Store(true)
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
				err = s.GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiDBEnableAsyncCommit, vardef.Off)
			case "1":
				err = s.GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiDBEnableAsyncCommit, vardef.On)
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
				err = s.GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiDBEnable1PC, vardef.Off)
			case "1":
				err = s.GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiDBEnable1PC, vardef.On)
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
				atomic.StoreUint32(&vardef.DDLSlowOprThreshold, uint32(threshold))
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
				err = s.GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiDBEnableMutationChecker, vardef.Off)
			case "1":
				err = s.GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar(context.Background(), vardef.TiDBEnableMutationChecker, vardef.On)
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
	schemas := schema.ListTablesWithSpecialAttribute(infoschemacontext.TiFlashAttribute)
	for _, schema := range schemas {
		for _, tbl := range schema.TableInfos {
			replicaInfos = appendTiFlashReplicaInfo(replicaInfos, tbl)
		}
	}

	droppedOrTruncateReplicaInfos, err := h.getDropOrTruncateTableTiflash(schema)
	if err != nil {
		handler.WriteError(w, err)
		return
	}
	replicaInfos = append(replicaInfos, droppedOrTruncateReplicaInfos...)
	handler.WriteData(w, replicaInfos)
}

func appendTiFlashReplicaInfo(replicaInfos []*TableFlashReplicaInfo, tblInfo *model.TableInfo) []*TableFlashReplicaInfo {
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
		if _, ok := currentSchema.TableByID(context.Background(), tblInfo.ID); ok {
			return false, nil
		}
		if _, ok := uniqueIDMap[tblInfo.ID]; ok {
			return false, nil
		}
		uniqueIDMap[tblInfo.ID] = struct{}{}
		replicaInfos = appendTiFlashReplicaInfo(replicaInfos, tblInfo)
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
	err = do.DDLExecutor().UpdateTableReplicaInfo(s, status.ID, available)
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
