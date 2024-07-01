// Copyright 2019 PingCAP, Inc.
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

package helper

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	deadlockpb "github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	derr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	pd "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

// Storage represents a storage that connects TiKV.
// Methods copied from kv.Storage and tikv.Storage due to limitation of go1.13.
type Storage interface {
	Begin(opts ...tikv.TxnOption) (kv.Transaction, error)
	GetSnapshot(ver kv.Version) kv.Snapshot
	GetClient() kv.Client
	GetMPPClient() kv.MPPClient
	Close() error
	UUID() string
	CurrentVersion(txnScope string) (kv.Version, error)
	CurrentTimestamp(txnScop string) (uint64, error)
	GetOracle() oracle.Oracle
	SupportDeleteRange() (supported bool)
	Name() string
	Describe() string
	ShowStatus(ctx context.Context, key string) (any, error)
	GetMemCache() kv.MemManager
	GetRegionCache() *tikv.RegionCache
	SendReq(bo *tikv.Backoffer, req *tikvrpc.Request, regionID tikv.RegionVerID, timeout time.Duration) (*tikvrpc.Response, error)
	GetLockResolver() *txnlock.LockResolver
	GetSafePointKV() tikv.SafePointKV
	UpdateSPCache(cachedSP uint64, cachedTime time.Time)
	SetOracle(oracle oracle.Oracle)
	SetTiKVClient(client tikv.Client)
	GetTiKVClient() tikv.Client
	Closed() <-chan struct{}
	GetMinSafeTS(txnScope string) uint64
	GetLockWaits() ([]*deadlockpb.WaitForEntry, error)
	GetCodec() tikv.Codec
	GetPDHTTPClient() pd.Client
}

// Helper is a middleware to get some information from tikv/pd. It can be used for TiDB's http api or mem table.
type Helper struct {
	Store       Storage
	RegionCache *tikv.RegionCache
	// pdHTTPCli is used to send http request to PD.
	// This field is lazy initialized in `TryGetPDHTTPClient`,
	// and should be tagged with the caller ID before using.
	pdHTTPCli pd.Client
}

// NewHelper gets a Helper from Storage
func NewHelper(store Storage) *Helper {
	return &Helper{
		Store:       store,
		RegionCache: store.GetRegionCache(),
	}
}

// TryGetPDHTTPClient tries to get a PD HTTP client if it's available.
func (h *Helper) TryGetPDHTTPClient() (pd.Client, error) {
	if h.pdHTTPCli != nil {
		return h.pdHTTPCli, nil
	}
	cli := h.Store.GetPDHTTPClient()
	if cli == nil {
		return nil, errors.New("pd http client unavailable")
	}
	h.pdHTTPCli = cli.WithCallerID("tidb-store-helper")
	return h.pdHTTPCli, nil
}

// MaxBackoffTimeoutForMvccGet is a derived value from previous implementation possible experiencing value 5000ms.
const MaxBackoffTimeoutForMvccGet = 5000

// GetMvccByEncodedKeyWithTS get the MVCC value by the specific encoded key, if lock is encountered it would be resolved.
func (h *Helper) GetMvccByEncodedKeyWithTS(encodedKey kv.Key, startTS uint64) (*kvrpcpb.MvccGetByKeyResponse, error) {
	bo := tikv.NewBackofferWithVars(context.Background(), MaxBackoffTimeoutForMvccGet, nil)
	tikvReq := tikvrpc.NewRequest(tikvrpc.CmdMvccGetByKey, &kvrpcpb.MvccGetByKeyRequest{Key: encodedKey})
	for {
		keyLocation, err := h.RegionCache.LocateKey(bo, encodedKey)
		if err != nil {
			return nil, derr.ToTiDBErr(err)
		}
		kvResp, err := h.Store.SendReq(bo, tikvReq, keyLocation.Region, time.Minute)
		if err != nil {
			logutil.BgLogger().Warn("get MVCC by encoded key failed",
				zap.Stringer("encodeKey", encodedKey),
				zap.Reflect("region", keyLocation.Region),
				zap.Stringer("keyLocation", keyLocation),
				zap.Reflect("kvResp", kvResp),
				zap.Error(err))
			return nil, errors.Trace(err)
		}

		regionErr, err := kvResp.GetRegionError()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if regionErr != nil {
			if err = bo.Backoff(tikv.BoRegionMiss(), errors.New(regionErr.String())); err != nil {
				return nil, err
			}
			continue
		}

		mvccResp := kvResp.Resp.(*kvrpcpb.MvccGetByKeyResponse)
		if errMsg := mvccResp.GetError(); errMsg != "" {
			logutil.BgLogger().Warn("get MVCC by encoded key failed",
				zap.Stringer("encodeKey", encodedKey),
				zap.Reflect("region", keyLocation.Region),
				zap.Stringer("keyLocation", keyLocation),
				zap.Reflect("kvResp", kvResp),
				zap.String("error", errMsg))
			return nil, errors.New(errMsg)
		}
		if mvccResp.Info == nil {
			errMsg := "Invalid mvcc response result, the info field is nil"
			logutil.BgLogger().Warn(errMsg,
				zap.Stringer("encodeKey", encodedKey),
				zap.Reflect("region", keyLocation.Region),
				zap.Stringer("keyLocation", keyLocation),
				zap.Reflect("kvResp", kvResp))
			return nil, errors.New(errMsg)
		}

		// Try to resolve the lock and retry mvcc get again if the input startTS is a valid value.
		if startTS > 0 && mvccResp.Info.GetLock() != nil {
			latestTS, err := h.Store.GetOracle().GetLowResolutionTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
			if err != nil {
				logutil.BgLogger().Warn("Failed to get latest ts", zap.Error(err))
				return nil, err
			}
			if startTS > latestTS {
				errMsg := fmt.Sprintf("Snapshot ts=%v is larger than latest allocated ts=%v, lock could not be resolved",
					startTS, latestTS)
				logutil.BgLogger().Warn(errMsg)
				return nil, errors.New(errMsg)
			}
			lockInfo := mvccResp.Info.GetLock()
			lock := &txnlock.Lock{
				Key:             []byte(encodedKey),
				Primary:         lockInfo.GetPrimary(),
				TxnID:           lockInfo.GetStartTs(),
				TTL:             lockInfo.GetTtl(),
				TxnSize:         lockInfo.GetTxnSize(),
				LockType:        lockInfo.GetType(),
				UseAsyncCommit:  lockInfo.GetUseAsyncCommit(),
				LockForUpdateTS: lockInfo.GetForUpdateTs(),
			}
			// Disable for read to avoid async resolve.
			resolveLocksOpts := txnlock.ResolveLocksOptions{
				CallerStartTS: startTS,
				Locks:         []*txnlock.Lock{lock},
				Lite:          true,
				ForRead:       false,
				Detail:        nil,
			}
			resolveLockRes, err := h.Store.GetLockResolver().ResolveLocksWithOpts(bo, resolveLocksOpts)
			if err != nil {
				return nil, err
			}
			msBeforeExpired := resolveLockRes.TTL
			if msBeforeExpired > 0 {
				if err = bo.BackoffWithCfgAndMaxSleep(tikv.BoTxnLock(), int(msBeforeExpired),
					errors.Errorf("resolve lock fails lock: %v", lock)); err != nil {
					return nil, err
				}
			}
			continue
		}
		return mvccResp, nil
	}
}

// GetMvccByEncodedKey get the MVCC value by the specific encoded key.
func (h *Helper) GetMvccByEncodedKey(encodedKey kv.Key) (*kvrpcpb.MvccGetByKeyResponse, error) {
	return h.GetMvccByEncodedKeyWithTS(encodedKey, 0)
}

// MvccKV wraps the key's mvcc info in tikv.
type MvccKV struct {
	Key      string                        `json:"key"`
	RegionID uint64                        `json:"region_id"`
	Value    *kvrpcpb.MvccGetByKeyResponse `json:"value"`
}

// GetMvccByStartTs gets Mvcc info by startTS from tikv.
func (h *Helper) GetMvccByStartTs(startTS uint64, startKey, endKey kv.Key) (*MvccKV, error) {
	bo := tikv.NewBackofferWithVars(context.Background(), 5000, nil)
	for {
		curRegion, err := h.RegionCache.LocateKey(bo, startKey)
		if err != nil {
			logutil.BgLogger().Error("get MVCC by startTS failed", zap.Uint64("txnStartTS", startTS),
				zap.Stringer("startKey", startKey), zap.Error(err))
			return nil, derr.ToTiDBErr(err)
		}

		tikvReq := tikvrpc.NewRequest(tikvrpc.CmdMvccGetByStartTs, &kvrpcpb.MvccGetByStartTsRequest{
			StartTs: startTS,
		})
		tikvReq.Context.Priority = kvrpcpb.CommandPri_Low
		kvResp, err := h.Store.SendReq(bo, tikvReq, curRegion.Region, time.Hour)
		if err != nil {
			logutil.BgLogger().Error("get MVCC by startTS failed",
				zap.Uint64("txnStartTS", startTS),
				zap.Stringer("startKey", startKey),
				zap.Reflect("region", curRegion.Region),
				zap.Stringer("curRegion", curRegion),
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
				zap.Stringer("curRegion", curRegion),
				zap.Reflect("kvResp", kvResp),
				zap.Stringer("error", err))
			continue
		}

		if len(data.GetError()) > 0 {
			logutil.BgLogger().Error("get MVCC by startTS failed",
				zap.Uint64("txnStartTS", startTS),
				zap.Stringer("startKey", startKey),
				zap.Reflect("region", curRegion.Region),
				zap.Stringer("curRegion", curRegion),
				zap.Reflect("kvResp", kvResp),
				zap.String("error", data.GetError()))
			return nil, errors.New(data.GetError())
		}

		key := data.GetKey()
		if len(key) > 0 {
			resp := &kvrpcpb.MvccGetByKeyResponse{Info: data.Info, RegionError: data.RegionError, Error: data.Error}
			return &MvccKV{Key: strings.ToUpper(hex.EncodeToString(key)), Value: resp, RegionID: curRegion.Region.GetID()}, nil
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

// RegionMetric presents the final metric output entry.
type RegionMetric struct {
	FlowBytes    uint64 `json:"flow_bytes"`
	MaxHotDegree int    `json:"max_hot_degree"`
	Count        int    `json:"region_count"`
}

// Constants that used to distinguish the hot region info request.
const (
	HotRead  = "read"
	HotWrite = "write"
)

// ScrapeHotInfo gets the needed hot region information by the url given.
func (h *Helper) ScrapeHotInfo(ctx context.Context, rw string, allSchemas []*model.DBInfo) ([]HotTableIndex, error) {
	regionMetrics, err := h.FetchHotRegion(ctx, rw)
	if err != nil {
		return nil, err
	}
	return h.FetchRegionTableIndex(regionMetrics, allSchemas)
}

// FetchHotRegion fetches the hot region information from PD's http api.
func (h *Helper) FetchHotRegion(ctx context.Context, rw string) (map[uint64]RegionMetric, error) {
	pdCli, err := h.TryGetPDHTTPClient()
	if err != nil {
		return nil, err
	}
	var regionResp *pd.StoreHotPeersInfos
	switch rw {
	case HotRead:
		regionResp, err = pdCli.GetHotReadRegions(ctx)
	case HotWrite:
		regionResp, err = pdCli.GetHotWriteRegions(ctx)
	}
	if err != nil {
		return nil, err
	}
	metricCnt := 0
	for _, hotRegions := range regionResp.AsLeader {
		metricCnt += len(hotRegions.Stats)
	}
	metric := make(map[uint64]RegionMetric, metricCnt)
	for _, hotRegions := range regionResp.AsLeader {
		for _, region := range hotRegions.Stats {
			metric[region.RegionID] = RegionMetric{FlowBytes: uint64(region.ByteRate), MaxHotDegree: region.HotDegree}
		}
	}
	return metric, nil
}

// TblIndex stores the things to index one table.
type TblIndex struct {
	DbName    string
	TableName string
	TableID   int64
	IndexName string
	IndexID   int64
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
	First  *FrameItem        // start frame of the region
	Last   *FrameItem        // end frame of the region
	region *tikv.KeyLocation // the region
}

// HotTableIndex contains region and its table/index info.
type HotTableIndex struct {
	RegionID     uint64        `json:"region_id"`
	RegionMetric *RegionMetric `json:"region_metric"`
	DbName       string        `json:"db_name"`
	TableName    string        `json:"table_name"`
	TableID      int64         `json:"table_id"`
	IndexName    string        `json:"index_name"`
	IndexID      int64         `json:"index_id"`
}

// FetchRegionTableIndex constructs a map that maps a table to its hot region information by the given raw hot RegionMetric metrics.
func (h *Helper) FetchRegionTableIndex(metrics map[uint64]RegionMetric, allSchemas []*model.DBInfo) ([]HotTableIndex, error) {
	hotTables := make([]HotTableIndex, 0, len(metrics))
	for regionID, regionMetric := range metrics {
		regionMetric := regionMetric
		t := HotTableIndex{RegionID: regionID, RegionMetric: &regionMetric}
		region, err := h.RegionCache.LocateRegionByID(tikv.NewBackofferWithVars(context.Background(), 500, nil), regionID)
		if err != nil {
			logutil.BgLogger().Error("locate region failed", zap.Error(err))
			continue
		}

		hotRange, err := NewRegionFrameRange(region)
		if err != nil {
			return nil, err
		}
		f := h.FindTableIndexOfRegion(allSchemas, hotRange)
		if f != nil {
			t.DbName = f.DBName
			t.TableName = f.TableName
			t.TableID = f.TableID
			t.IndexName = f.IndexName
			t.IndexID = f.IndexID
		}
		hotTables = append(hotTables, t)
	}

	return hotTables, nil
}

// FindTableIndexOfRegion finds what table is involved in this hot region. And constructs the new frame item for future use.
func (*Helper) FindTableIndexOfRegion(allSchemas []*model.DBInfo, hotRange *RegionFrameRange) *FrameItem {
	for _, db := range allSchemas {
		for _, tbl := range db.Tables {
			if f := findRangeInTable(hotRange, db, tbl); f != nil {
				return f
			}
		}
	}
	return nil
}

func findRangeInTable(hotRange *RegionFrameRange, db *model.DBInfo, tbl *model.TableInfo) *FrameItem {
	pi := tbl.GetPartitionInfo()
	if pi == nil {
		return findRangeInPhysicalTable(hotRange, tbl.ID, db.Name.O, tbl.Name.O, tbl.Indices, tbl.IsCommonHandle)
	}

	for _, def := range pi.Definitions {
		tablePartition := fmt.Sprintf("%s(%s)", tbl.Name.O, def.Name)
		if f := findRangeInPhysicalTable(hotRange, def.ID, db.Name.O, tablePartition, tbl.Indices, tbl.IsCommonHandle); f != nil {
			return f
		}
	}
	return nil
}

func findRangeInPhysicalTable(hotRange *RegionFrameRange, physicalID int64, dbName, tblName string, indices []*model.IndexInfo, isCommonHandle bool) *FrameItem {
	if f := hotRange.GetRecordFrame(physicalID, dbName, tblName, isCommonHandle); f != nil {
		return f
	}
	for _, idx := range indices {
		if f := hotRange.GetIndexFrame(physicalID, idx.ID, dbName, tblName, idx.Name.O); f != nil {
			return f
		}
	}
	return nil
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
		First:  first,
		Last:   last,
	}
	return idxRange, nil
}

// NewFrameItemFromRegionKey creates a FrameItem with region's startKey or endKey,
// returns err when key is illegal.
func NewFrameItemFromRegionKey(key []byte) (frame *FrameItem, err error) {
	frame = &FrameItem{}
	frame.TableID, frame.IndexID, frame.IsRecord, err = tablecodec.DecodeKeyHead(key)
	if err == nil {
		if frame.IsRecord {
			var handle kv.Handle
			_, handle, err = tablecodec.DecodeRecordKey(key)
			if err == nil {
				if handle.IsInt() {
					frame.RecordID = handle.IntValue()
				} else {
					data, err := handle.Data()
					if err != nil {
						return nil, err
					}
					frame.IndexName = "PRIMARY"
					frame.IndexValues = make([]string, 0, len(data))
					for _, datum := range data {
						str, err := datum.ToString()
						if err != nil {
							return nil, err
						}
						frame.IndexValues = append(frame.IndexValues, str)
					}
				}
			}
		} else {
			_, _, frame.IndexValues, err = tablecodec.DecodeIndexKey(key)
		}
		logutil.BgLogger().Warn("decode region key failed", zap.ByteString("key", key), zap.Error(err))
		// Ignore decode errors.
		err = nil
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

// GetRecordFrame returns the record frame of a table. If the table's records
// are not covered by this frame range, it returns nil.
func (r *RegionFrameRange) GetRecordFrame(tableID int64, dbName, tableName string, isCommonHandle bool) (f *FrameItem) {
	if tableID == r.First.TableID && r.First.IsRecord {
		r.First.DBName, r.First.TableName = dbName, tableName
		f = r.First
	} else if tableID == r.Last.TableID && r.Last.IsRecord {
		r.Last.DBName, r.Last.TableName = dbName, tableName
		f = r.Last
	} else if tableID >= r.First.TableID && tableID < r.Last.TableID {
		f = &FrameItem{
			DBName:    dbName,
			TableName: tableName,
			TableID:   tableID,
			IsRecord:  true,
		}
	}
	if f != nil && f.IsRecord && isCommonHandle {
		f.IndexName = "PRIMARY"
	}
	return
}

// GetIndexFrame returns the indnex frame of a table. If the table's indices are
// not covered by this frame range, it returns nil.
func (r *RegionFrameRange) GetIndexFrame(tableID, indexID int64, dbName, tableName, indexName string) *FrameItem {
	if tableID == r.First.TableID && !r.First.IsRecord && indexID == r.First.IndexID {
		r.First.DBName, r.First.TableName, r.First.IndexName = dbName, tableName, indexName
		return r.First
	}
	if tableID == r.Last.TableID && indexID == r.Last.IndexID {
		r.Last.DBName, r.Last.TableName, r.Last.IndexName = dbName, tableName, indexName
		return r.Last
	}

	greaterThanFirst := tableID > r.First.TableID || (tableID == r.First.TableID && !r.First.IsRecord && indexID > r.First.IndexID)
	lessThanLast := tableID < r.Last.TableID || (tableID == r.Last.TableID && (r.Last.IsRecord || indexID < r.Last.IndexID))
	if greaterThanFirst && lessThanLast {
		return &FrameItem{
			DBName:    dbName,
			TableName: tableName,
			TableID:   tableID,
			IsRecord:  false,
			IndexName: indexName,
			IndexID:   indexID,
		}
	}
	return nil
}

// TableInfo stores the information of a table or an index
type TableInfo struct {
	DB          *model.DBInfo
	Table       *model.TableInfo
	IsPartition bool
	Partition   *model.PartitionDefinition
	IsIndex     bool
	Index       *model.IndexInfo
}

type withKeyRange interface {
	GetStartKey() string
	GetEndKey() string
}

// isIntersecting returns true if x and y intersect.
func isIntersecting(x, y withKeyRange) bool {
	return isIntersectingKeyRange(x, y.GetStartKey(), y.GetEndKey())
}

// isIntersectingKeyRange returns true if [startKey, endKey) intersect with x.
func isIntersectingKeyRange(x withKeyRange, startKey, endKey string) bool {
	return !isBeforeKeyRange(x, startKey, endKey) && !isBehindKeyRange(x, startKey, endKey)
}

// isBehind returns true is x is behind y
func isBehind(x, y withKeyRange) bool {
	return isBehindKeyRange(x, y.GetStartKey(), y.GetEndKey())
}

// IsBefore returns true is x is before [startKey, endKey)
func isBeforeKeyRange(x withKeyRange, startKey, _ string) bool {
	return x.GetEndKey() != "" && x.GetEndKey() <= startKey
}

// IsBehind returns true is x is behind [startKey, endKey)
func isBehindKeyRange(x withKeyRange, _, endKey string) bool {
	return endKey != "" && x.GetStartKey() >= endKey
}

// TableInfoWithKeyRange stores table or index information with its key range.
type TableInfoWithKeyRange struct {
	*TableInfo
	StartKey string
	EndKey   string
}

// GetStartKey implements `withKeyRange` interface.
func (t TableInfoWithKeyRange) GetStartKey() string { return t.StartKey }

// GetEndKey implements `withKeyRange` interface.
func (t TableInfoWithKeyRange) GetEndKey() string { return t.EndKey }

// NewTableWithKeyRange constructs TableInfoWithKeyRange for given table, it is exported only for test.
func NewTableWithKeyRange(db *model.DBInfo, table *model.TableInfo) TableInfoWithKeyRange {
	return newTableInfoWithKeyRange(db, table, nil, nil)
}

// NewIndexWithKeyRange constructs TableInfoWithKeyRange for given index, it is exported only for test.
func NewIndexWithKeyRange(db *model.DBInfo, table *model.TableInfo, index *model.IndexInfo) TableInfoWithKeyRange {
	return newTableInfoWithKeyRange(db, table, nil, index)
}

// FilterMemDBs filters memory databases in the input schemas.
func (*Helper) FilterMemDBs(oldSchemas []*model.DBInfo) (schemas []*model.DBInfo) {
	for _, dbInfo := range oldSchemas {
		if util.IsMemDB(dbInfo.Name.L) {
			continue
		}
		schemas = append(schemas, dbInfo)
	}
	return
}

// GetRegionsTableInfo returns a map maps region id to its tables or indices.
// Assuming tables or indices key ranges never intersect.
// Regions key ranges can intersect.
func (h *Helper) GetRegionsTableInfo(regionsInfo *pd.RegionsInfo, schemas []*model.DBInfo) map[int64][]TableInfo {
	tables := h.GetTablesInfoWithKeyRange(schemas)

	regions := make([]*pd.RegionInfo, 0, len(regionsInfo.Regions))
	for i := 0; i < len(regionsInfo.Regions); i++ {
		regions = append(regions, &regionsInfo.Regions[i])
	}

	tableInfos := h.ParseRegionsTableInfos(regions, tables)
	return tableInfos
}

func newTableInfoWithKeyRange(db *model.DBInfo, table *model.TableInfo, partition *model.PartitionDefinition, index *model.IndexInfo) TableInfoWithKeyRange {
	var sk, ek []byte
	if partition == nil && index == nil {
		sk, ek = tablecodec.GetTableHandleKeyRange(table.ID)
	} else if partition != nil && index == nil {
		sk, ek = tablecodec.GetTableHandleKeyRange(partition.ID)
	} else if partition == nil && index != nil {
		sk, ek = tablecodec.GetTableIndexKeyRange(table.ID, index.ID)
	} else {
		sk, ek = tablecodec.GetTableIndexKeyRange(partition.ID, index.ID)
	}
	startKey := bytesKeyToHex(codec.EncodeBytes(nil, sk))
	endKey := bytesKeyToHex(codec.EncodeBytes(nil, ek))
	return TableInfoWithKeyRange{
		&TableInfo{
			DB:          db,
			Table:       table,
			IsPartition: partition != nil,
			Partition:   partition,
			IsIndex:     index != nil,
			Index:       index,
		},
		startKey,
		endKey,
	}
}

// GetTablesInfoWithKeyRange returns a slice containing tableInfos with key ranges of all tables in schemas.
func (*Helper) GetTablesInfoWithKeyRange(schemas []*model.DBInfo) []TableInfoWithKeyRange {
	tables := []TableInfoWithKeyRange{}
	for _, db := range schemas {
		for _, table := range db.Tables {
			if table.Partition != nil {
				for i := range table.Partition.Definitions {
					tables = append(tables, newTableInfoWithKeyRange(db, table, &table.Partition.Definitions[i], nil))
				}
			} else {
				tables = append(tables, newTableInfoWithKeyRange(db, table, nil, nil))
			}
			for _, index := range table.Indices {
				if table.Partition == nil || index.Global {
					tables = append(tables, newTableInfoWithKeyRange(db, table, nil, index))
					continue
				}
				for i := range table.Partition.Definitions {
					tables = append(tables, newTableInfoWithKeyRange(db, table, &table.Partition.Definitions[i], index))
				}
			}
		}
	}
	slices.SortFunc(tables, func(i, j TableInfoWithKeyRange) int {
		return cmp.Compare(i.StartKey, j.StartKey)
	})
	return tables
}

// ParseRegionsTableInfos parses the tables or indices in regions according to key range.
func (*Helper) ParseRegionsTableInfos(regionsInfo []*pd.RegionInfo, tables []TableInfoWithKeyRange) map[int64][]TableInfo {
	tableInfos := make(map[int64][]TableInfo, len(regionsInfo))

	if len(tables) == 0 || len(regionsInfo) == 0 {
		return tableInfos
	}
	// tables is sorted in GetTablesInfoWithKeyRange func
	slices.SortFunc(regionsInfo, func(i, j *pd.RegionInfo) int {
		return cmp.Compare(i.StartKey, j.StartKey)
	})

	idx := 0
OutLoop:
	for _, region := range regionsInfo {
		id := region.ID
		tableInfos[id] = []TableInfo{}
		for isBehind(region, &tables[idx]) {
			idx++
			if idx >= len(tables) {
				break OutLoop
			}
		}
		for i := idx; i < len(tables) && isIntersecting(region, &tables[i]); i++ {
			tableInfos[id] = append(tableInfos[id], *tables[i].TableInfo)
		}
	}

	return tableInfos
}

func bytesKeyToHex(key []byte) string {
	return strings.ToUpper(hex.EncodeToString(key))
}

// GetPDAddr return the PD Address.
func (h *Helper) GetPDAddr() ([]string, error) {
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

// GetPDRegionStats get the RegionStats by tableID from PD by HTTP API.
func (h *Helper) GetPDRegionStats(ctx context.Context, tableID int64, noIndexStats bool) (*pd.RegionStats, error) {
	pdCli, err := h.TryGetPDHTTPClient()
	if err != nil {
		return nil, err
	}

	var startKey, endKey []byte
	if noIndexStats {
		startKey = tablecodec.GenTableRecordPrefix(tableID)
		endKey = kv.Key(startKey).PrefixNext()
	} else {
		startKey = tablecodec.EncodeTablePrefix(tableID)
		endKey = kv.Key(startKey).PrefixNext()
	}
	startKey = codec.EncodeBytes([]byte{}, startKey)
	endKey = codec.EncodeBytes([]byte{}, endKey)

	return pdCli.GetRegionStatusByKeyRange(ctx, pd.NewKeyRange(startKey, endKey), false)
}

// GetTiFlashTableIDFromEndKey computes tableID from pd rule's endKey.
func GetTiFlashTableIDFromEndKey(endKey string) int64 {
	e, _ := hex.DecodeString(endKey)
	_, decodedEndKey, _ := codec.DecodeBytes(e, []byte{})
	tableID := tablecodec.DecodeTableID(decodedEndKey)
	tableID--
	return tableID
}

// ComputeTiFlashStatus is helper function for CollectTiFlashStatus.
func ComputeTiFlashStatus(reader *bufio.Reader, regionReplica *map[int64]int) error {
	ns, err := reader.ReadString('\n')
	if err != nil {
		return errors.Trace(err)
	}
	// The count
	ns = strings.Trim(ns, "\r\n\t")
	n, err := strconv.ParseInt(ns, 10, 64)
	if err != nil {
		return errors.Trace(err)
	}
	// The regions
	regions, err := reader.ReadString('\n')
	if err != nil {
		return errors.Trace(err)
	}
	regions = strings.Trim(regions, "\r\n\t")
	splits := strings.Split(regions, " ")
	realN := int64(0)
	for _, s := range splits {
		// For (`table`, `store`), has region `r`
		if s == "" {
			continue
		}
		realN++
		r, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return errors.Trace(err)
		}
		if c, ok := (*regionReplica)[r]; ok {
			(*regionReplica)[r] = c + 1
		} else {
			(*regionReplica)[r] = 1
		}
	}
	if n != realN {
		logutil.BgLogger().Warn("ComputeTiFlashStatus count check failed", zap.Int64("claim", n), zap.Int64("real", realN))
	}
	return nil
}

// CollectTiFlashStatus query sync status of one table from TiFlash store.
// `regionReplica` is a map from RegionID to count of TiFlash Replicas in this region.
func CollectTiFlashStatus(statusAddress string, keyspaceID tikv.KeyspaceID, tableID int64, regionReplica *map[int64]int) error {
	// The new query schema is like: http://<host>/tiflash/sync-status/keyspace/<keyspaceID>/table/<tableID>.
	// For TiDB forward compatibility, we define the Nullspace as the "keyspace" of the old table.
	// The query URL is like: http://<host>/sync-status/keyspace/<NullspaceID>/table/<tableID>
	// The old query schema is like: http://<host>/sync-status/<tableID>
	// This API is preserved in TiFlash for compatibility with old versions of TiDB.
	statURL := fmt.Sprintf("%s://%s/tiflash/sync-status/keyspace/%d/table/%d",
		util.InternalHTTPSchema(),
		statusAddress,
		keyspaceID,
		tableID,
	)
	resp, err := util.InternalHTTPClient().Get(statURL)
	if err != nil {
		return errors.Trace(err)
	}

	defer func() {
		err = resp.Body.Close()
		if err != nil {
			logutil.BgLogger().Error("close body failed", zap.Error(err))
		}
	}()

	reader := bufio.NewReader(resp.Body)
	if err = ComputeTiFlashStatus(reader, regionReplica); err != nil {
		return errors.Trace(err)
	}
	return nil
}
