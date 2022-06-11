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
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	deadlockpb "github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	derr "github.com/pingcap/tidb/store/driver/error"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/pdapi"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
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
	ShowStatus(ctx context.Context, key string) (interface{}, error)
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
}

// Helper is a middleware to get some information from tikv/pd. It can be used for TiDB's http api or mem table.
type Helper struct {
	Store       Storage
	RegionCache *tikv.RegionCache
}

// NewHelper gets a Helper from Storage
func NewHelper(store Storage) *Helper {
	return &Helper{
		Store:       store,
		RegionCache: store.GetRegionCache(),
	}
}

// GetMvccByEncodedKey get the MVCC value by the specific encoded key.
func (h *Helper) GetMvccByEncodedKey(encodedKey kv.Key) (*kvrpcpb.MvccGetByKeyResponse, error) {
	keyLocation, err := h.RegionCache.LocateKey(tikv.NewBackofferWithVars(context.Background(), 500, nil), encodedKey)
	if err != nil {
		return nil, derr.ToTiDBErr(err)
	}

	tikvReq := tikvrpc.NewRequest(tikvrpc.CmdMvccGetByKey, &kvrpcpb.MvccGetByKeyRequest{Key: encodedKey})
	kvResp, err := h.Store.SendReq(tikv.NewBackofferWithVars(context.Background(), 500, nil), tikvReq, keyLocation.Region, time.Minute)
	if err != nil {
		logutil.BgLogger().Info("get MVCC by encoded key failed",
			zap.Stringer("encodeKey", encodedKey),
			zap.Reflect("region", keyLocation.Region),
			zap.Stringer("keyLocation", keyLocation),
			zap.Reflect("kvResp", kvResp),
			zap.Error(err))
		return nil, errors.Trace(err)
	}
	return kvResp.Resp.(*kvrpcpb.MvccGetByKeyResponse), nil
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
		startKey = kv.Key(curRegion.EndKey)
	}
}

// StoreHotRegionInfos records all hog region stores.
// it's the response of PD.
type StoreHotRegionInfos struct {
	AsPeer   map[uint64]*HotRegionsStat `json:"as_peer"`
	AsLeader map[uint64]*HotRegionsStat `json:"as_leader"`
}

// HotRegionsStat records echo store's hot region.
// it's the response of PD.
type HotRegionsStat struct {
	RegionsStat []RegionStat `json:"statistics"`
}

// RegionStat records each hot region's statistics
// it's the response of PD.
type RegionStat struct {
	RegionID  uint64  `json:"region_id"`
	FlowBytes float64 `json:"flow_bytes"`
	HotDegree int     `json:"hot_degree"`
}

// RegionMetric presents the final metric output entry.
type RegionMetric struct {
	FlowBytes    uint64 `json:"flow_bytes"`
	MaxHotDegree int    `json:"max_hot_degree"`
	Count        int    `json:"region_count"`
}

// ScrapeHotInfo gets the needed hot region information by the url given.
func (h *Helper) ScrapeHotInfo(rw string, allSchemas []*model.DBInfo) ([]HotTableIndex, error) {
	regionMetrics, err := h.FetchHotRegion(rw)
	if err != nil {
		return nil, err
	}
	return h.FetchRegionTableIndex(regionMetrics, allSchemas)
}

// FetchHotRegion fetches the hot region information from PD's http api.
func (h *Helper) FetchHotRegion(rw string) (map[uint64]RegionMetric, error) {
	var regionResp StoreHotRegionInfos
	if err := h.requestPD("FetchHotRegion", "GET", rw, nil, &regionResp); err != nil {
		return nil, err
	}
	metricCnt := 0
	for _, hotRegions := range regionResp.AsLeader {
		metricCnt += len(hotRegions.RegionsStat)
	}
	metric := make(map[uint64]RegionMetric, metricCnt)
	for _, hotRegions := range regionResp.AsLeader {
		for _, region := range hotRegions.RegionsStat {
			metric[region.RegionID] = RegionMetric{FlowBytes: uint64(region.FlowBytes), MaxHotDegree: region.HotDegree}
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
func (h *Helper) FindTableIndexOfRegion(allSchemas []*model.DBInfo, hotRange *RegionFrameRange) *FrameItem {
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

// RegionPeer stores information of one peer.
type RegionPeer struct {
	ID        int64 `json:"id"`
	StoreID   int64 `json:"store_id"`
	IsLearner bool  `json:"is_learner"`
}

// RegionEpoch stores the information about its epoch.
type RegionEpoch struct {
	ConfVer int64 `json:"conf_ver"`
	Version int64 `json:"version"`
}

// RegionPeerStat stores one field `DownSec` which indicates how long it's down than `RegionPeer`.
type RegionPeerStat struct {
	Peer    RegionPeer `json:"peer"`
	DownSec int64      `json:"down_seconds"`
}

// RegionInfo stores the information of one region.
type RegionInfo struct {
	ID              int64            `json:"id"`
	StartKey        string           `json:"start_key"`
	EndKey          string           `json:"end_key"`
	Epoch           RegionEpoch      `json:"epoch"`
	Peers           []RegionPeer     `json:"peers"`
	Leader          RegionPeer       `json:"leader"`
	DownPeers       []RegionPeerStat `json:"down_peers"`
	PendingPeers    []RegionPeer     `json:"pending_peers"`
	WrittenBytes    uint64           `json:"written_bytes"`
	ReadBytes       uint64           `json:"read_bytes"`
	ApproximateSize int64            `json:"approximate_size"`
	ApproximateKeys int64            `json:"approximate_keys"`

	ReplicationStatus *ReplicationStatus `json:"replication_status,omitempty"`
}

// RegionsInfo stores the information of regions.
type RegionsInfo struct {
	Count   int64        `json:"count"`
	Regions []RegionInfo `json:"regions"`
}

// NewRegionsInfo returns RegionsInfo
func NewRegionsInfo() *RegionsInfo {
	return &RegionsInfo{
		Regions: make([]RegionInfo, 0),
	}
}

// Merge merged 2 regionsInfo into one
func (r *RegionsInfo) Merge(other *RegionsInfo) *RegionsInfo {
	newRegionsInfo := &RegionsInfo{
		Regions: make([]RegionInfo, 0, r.Count+other.Count),
	}
	m := make(map[int64]RegionInfo, r.Count+other.Count)
	for _, region := range r.Regions {
		m[region.ID] = region
	}
	for _, region := range other.Regions {
		m[region.ID] = region
	}
	for _, region := range m {
		newRegionsInfo.Regions = append(newRegionsInfo.Regions, region)
	}
	newRegionsInfo.Count = int64(len(newRegionsInfo.Regions))
	return newRegionsInfo
}

// ReplicationStatus represents the replication mode status of the region.
type ReplicationStatus struct {
	State   string `json:"state"`
	StateID int64  `json:"state_id"`
}

// TableInfo stores the information of a table or an index
type TableInfo struct {
	DB      *model.DBInfo
	Table   *model.TableInfo
	IsIndex bool
	Index   *model.IndexInfo
}

type withKeyRange interface {
	getStartKey() string
	getEndKey() string
}

// isIntersecting returns true if x and y intersect.
func isIntersecting(x, y withKeyRange) bool {
	return isIntersectingKeyRange(x, y.getStartKey(), y.getEndKey())
}

// isIntersectingKeyRange returns true if [startKey, endKey) intersect with x.
func isIntersectingKeyRange(x withKeyRange, startKey, endKey string) bool {
	return !isBeforeKeyRange(x, startKey, endKey) && !isBehindKeyRange(x, startKey, endKey)
}

// isBehind returns true is x is behind y
func isBehind(x, y withKeyRange) bool {
	return isBehindKeyRange(x, y.getStartKey(), y.getEndKey())
}

// IsBefore returns true is x is before [startKey, endKey)
func isBeforeKeyRange(x withKeyRange, startKey, endKey string) bool {
	return x.getEndKey() != "" && x.getEndKey() <= startKey
}

// IsBehind returns true is x is behind [startKey, endKey)
func isBehindKeyRange(x withKeyRange, startKey, endKey string) bool {
	return endKey != "" && x.getStartKey() >= endKey
}

func (r *RegionInfo) getStartKey() string { return r.StartKey }
func (r *RegionInfo) getEndKey() string   { return r.EndKey }

// for sorting
type byRegionStartKey []*RegionInfo

func (xs byRegionStartKey) Len() int      { return len(xs) }
func (xs byRegionStartKey) Swap(i, j int) { xs[i], xs[j] = xs[j], xs[i] }
func (xs byRegionStartKey) Less(i, j int) bool {
	return xs[i].getStartKey() < xs[j].getStartKey()
}

// TableInfoWithKeyRange stores table or index informations with its key range.
type TableInfoWithKeyRange struct {
	*TableInfo
	StartKey string
	EndKey   string
}

func (t TableInfoWithKeyRange) getStartKey() string { return t.StartKey }
func (t TableInfoWithKeyRange) getEndKey() string   { return t.EndKey }

// for sorting
type byTableStartKey []TableInfoWithKeyRange

func (xs byTableStartKey) Len() int      { return len(xs) }
func (xs byTableStartKey) Swap(i, j int) { xs[i], xs[j] = xs[j], xs[i] }
func (xs byTableStartKey) Less(i, j int) bool {
	return xs[i].getStartKey() < xs[j].getStartKey()
}

// NewTableWithKeyRange constructs TableInfoWithKeyRange for given table, it is exported only for test.
func NewTableWithKeyRange(db *model.DBInfo, table *model.TableInfo) TableInfoWithKeyRange {
	return newTableWithKeyRange(db, table)
}

func newTableWithKeyRange(db *model.DBInfo, table *model.TableInfo) TableInfoWithKeyRange {
	sk, ek := tablecodec.GetTableHandleKeyRange(table.ID)
	startKey := bytesKeyToHex(codec.EncodeBytes(nil, sk))
	endKey := bytesKeyToHex(codec.EncodeBytes(nil, ek))
	return TableInfoWithKeyRange{
		&TableInfo{
			DB:      db,
			Table:   table,
			IsIndex: false,
			Index:   nil,
		},
		startKey,
		endKey,
	}
}

// NewIndexWithKeyRange constructs TableInfoWithKeyRange for given index, it is exported only for test.
func NewIndexWithKeyRange(db *model.DBInfo, table *model.TableInfo, index *model.IndexInfo) TableInfoWithKeyRange {
	return newIndexWithKeyRange(db, table, index)
}

func newIndexWithKeyRange(db *model.DBInfo, table *model.TableInfo, index *model.IndexInfo) TableInfoWithKeyRange {
	sk, ek := tablecodec.GetTableIndexKeyRange(table.ID, index.ID)
	startKey := bytesKeyToHex(codec.EncodeBytes(nil, sk))
	endKey := bytesKeyToHex(codec.EncodeBytes(nil, ek))
	return TableInfoWithKeyRange{
		&TableInfo{
			DB:      db,
			Table:   table,
			IsIndex: true,
			Index:   index,
		},
		startKey,
		endKey,
	}
}

func newPartitionTableWithKeyRange(db *model.DBInfo, table *model.TableInfo, partitionID int64) TableInfoWithKeyRange {
	sk, ek := tablecodec.GetTableHandleKeyRange(partitionID)
	startKey := bytesKeyToHex(codec.EncodeBytes(nil, sk))
	endKey := bytesKeyToHex(codec.EncodeBytes(nil, ek))
	return TableInfoWithKeyRange{
		&TableInfo{
			DB:      db,
			Table:   table,
			IsIndex: false,
			Index:   nil,
		},
		startKey,
		endKey,
	}
}

// FilterMemDBs filters memory databases in the input schemas.
func (h *Helper) FilterMemDBs(oldSchemas []*model.DBInfo) (schemas []*model.DBInfo) {
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
func (h *Helper) GetRegionsTableInfo(regionsInfo *RegionsInfo, schemas []*model.DBInfo) map[int64][]TableInfo {
	tables := h.GetTablesInfoWithKeyRange(schemas)

	regions := make([]*RegionInfo, 0, len(regionsInfo.Regions))
	for i := 0; i < len(regionsInfo.Regions); i++ {
		regions = append(regions, &regionsInfo.Regions[i])
	}

	tableInfos := h.ParseRegionsTableInfos(regions, tables)
	return tableInfos
}

// GetTablesInfoWithKeyRange returns a slice containing tableInfos with key ranges of all tables in schemas.
func (h *Helper) GetTablesInfoWithKeyRange(schemas []*model.DBInfo) []TableInfoWithKeyRange {
	tables := []TableInfoWithKeyRange{}
	for _, db := range schemas {
		for _, table := range db.Tables {
			if table.Partition != nil {
				for _, partition := range table.Partition.Definitions {
					tables = append(tables, newPartitionTableWithKeyRange(db, table, partition.ID))
				}
			} else {
				tables = append(tables, newTableWithKeyRange(db, table))
			}
			for _, index := range table.Indices {
				tables = append(tables, newIndexWithKeyRange(db, table, index))
			}
		}
	}
	sort.Sort(byTableStartKey(tables))
	return tables
}

// ParseRegionsTableInfos parses the tables or indices in regions according to key range.
func (h *Helper) ParseRegionsTableInfos(regionsInfo []*RegionInfo, tables []TableInfoWithKeyRange) map[int64][]TableInfo {
	tableInfos := make(map[int64][]TableInfo, len(regionsInfo))

	if len(tables) == 0 || len(regionsInfo) == 0 {
		return tableInfos
	}
	// tables is sorted in GetTablesInfoWithKeyRange func
	sort.Sort(byRegionStartKey(regionsInfo))

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

// BytesKeyToHex converts bytes key to hex key, it is exported only for test.
func BytesKeyToHex(key []byte) string {
	return bytesKeyToHex(key)
}

func bytesKeyToHex(key []byte) string {
	return strings.ToUpper(hex.EncodeToString(key))
}

// GetRegionsInfo gets the region information of current store by using PD's api.
func (h *Helper) GetRegionsInfo() (*RegionsInfo, error) {
	var regionsInfo RegionsInfo
	err := h.requestPD("GetRegions", "GET", pdapi.Regions, nil, &regionsInfo)
	return &regionsInfo, err
}

// GetStoreRegionsInfo gets the region in given store.
func (h *Helper) GetStoreRegionsInfo(storeID uint64) (*RegionsInfo, error) {
	var regionsInfo RegionsInfo
	err := h.requestPD("GetStoreRegions", "GET", pdapi.StoreRegions+"/"+strconv.FormatUint(storeID, 10), nil, &regionsInfo)
	return &regionsInfo, err
}

// GetRegionInfoByID gets the region information of the region ID by using PD's api.
func (h *Helper) GetRegionInfoByID(regionID uint64) (*RegionInfo, error) {
	var regionInfo RegionInfo
	err := h.requestPD("GetRegionByID", "GET", pdapi.RegionByID+"/"+strconv.FormatUint(regionID, 10), nil, &regionInfo)
	return &regionInfo, err
}

// GetRegionsInfoByRange scans region by key range
func (h *Helper) GetRegionsInfoByRange(sk, ek []byte) (*RegionsInfo, error) {
	var regionsInfo RegionsInfo
	err := h.requestPD("GetRegionByRange", "GET", fmt.Sprintf("%v?key=%s&end_key=%s", pdapi.ScanRegions,
		url.QueryEscape(string(sk)), url.QueryEscape(string(ek))), nil, &regionsInfo)
	return &regionsInfo, err
}

// GetRegionByKey gets regioninfo by key
func (h *Helper) GetRegionByKey(k []byte) (*RegionInfo, error) {
	var regionInfo RegionInfo
	err := h.requestPD("GetRegionByKey", "GET", fmt.Sprintf("%v/%v", pdapi.RegionKey, url.QueryEscape(string(k))), nil, &regionInfo)
	return &regionInfo, err
}

// request PD API, decode the response body into res
func (h *Helper) requestPD(apiName, method, uri string, body io.Reader, res interface{}) error {
	etcd, ok := h.Store.(kv.EtcdBackend)
	if !ok {
		return errors.WithStack(errors.New("not implemented"))
	}
	pdHosts, err := etcd.EtcdAddrs()
	if err != nil {
		return err
	}
	if len(pdHosts) == 0 {
		return errors.New("pd unavailable")
	}
	logutil.BgLogger().Debug("RequestPD URL", zap.String("url", util.InternalHTTPSchema()+"://"+pdHosts[0]+uri))
	req := new(http.Request)
	for _, host := range pdHosts {
		req, err = http.NewRequest(method, util.InternalHTTPSchema()+"://"+host+uri, body)
		if err != nil {
			// Try to request from another PD node when some nodes may down.
			if strings.Contains(err.Error(), "connection refused") {
				continue
			}
			return errors.Trace(err)
		}
	}
	if err != nil {
		return err
	}
	start := time.Now()
	resp, err := util.InternalHTTPClient().Do(req)
	if err != nil {
		metrics.PDAPIRequestCounter.WithLabelValues(apiName, "network error").Inc()
		return errors.Trace(err)
	}
	metrics.PDAPIExecutionHistogram.WithLabelValues(apiName).Observe(time.Since(start).Seconds())
	metrics.PDAPIRequestCounter.WithLabelValues(apiName, resp.Status).Inc()

	defer func() {
		err = resp.Body.Close()
		if err != nil {
			logutil.BgLogger().Error("close body failed", zap.Error(err))
		}
	}()

	err = json.NewDecoder(resp.Body).Decode(res)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// StoresStat stores all information get from PD's api.
type StoresStat struct {
	Count  int         `json:"count"`
	Stores []StoreStat `json:"stores"`
}

// StoreStat stores information of one store.
type StoreStat struct {
	Store  StoreBaseStat   `json:"store"`
	Status StoreDetailStat `json:"status"`
}

// StoreBaseStat stores the basic information of one store.
type StoreBaseStat struct {
	ID             int64        `json:"id"`
	Address        string       `json:"address"`
	State          int64        `json:"state"`
	StateName      string       `json:"state_name"`
	Version        string       `json:"version"`
	Labels         []StoreLabel `json:"labels"`
	StatusAddress  string       `json:"status_address"`
	GitHash        string       `json:"git_hash"`
	StartTimestamp int64        `json:"start_timestamp"`
}

// StoreLabel stores the information of one store label.
type StoreLabel struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// StoreDetailStat stores the detail information of one store.
type StoreDetailStat struct {
	Capacity        string    `json:"capacity"`
	Available       string    `json:"available"`
	LeaderCount     int64     `json:"leader_count"`
	LeaderWeight    float64   `json:"leader_weight"`
	LeaderScore     float64   `json:"leader_score"`
	LeaderSize      int64     `json:"leader_size"`
	RegionCount     int64     `json:"region_count"`
	RegionWeight    float64   `json:"region_weight"`
	RegionScore     float64   `json:"region_score"`
	RegionSize      int64     `json:"region_size"`
	StartTs         time.Time `json:"start_ts"`
	LastHeartbeatTs time.Time `json:"last_heartbeat_ts"`
	Uptime          string    `json:"uptime"`
}

// GetStoresStat gets the TiKV store information by accessing PD's api.
func (h *Helper) GetStoresStat() (*StoresStat, error) {
	var storesStat StoresStat
	err := h.requestPD("GetStoresStat", "GET", pdapi.Stores, nil, &storesStat)
	return &storesStat, err
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

// PDRegionStats is the json response from PD.
type PDRegionStats struct {
	Count            int            `json:"count"`
	EmptyCount       int            `json:"empty_count"`
	StorageSize      int64          `json:"storage_size"`
	StorageKeys      int64          `json:"storage_keys"`
	StoreLeaderCount map[uint64]int `json:"store_leader_count"`
	StorePeerCount   map[uint64]int `json:"store_peer_count"`
}

// GetPDRegionStats get the RegionStats by tableID.
func (h *Helper) GetPDRegionStats(tableID int64, stats *PDRegionStats, noIndexStats bool) error {
	pdAddrs, err := h.GetPDAddr()
	if err != nil {
		return errors.Trace(err)
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

	statURL := fmt.Sprintf("%s://%s/pd/api/v1/stats/region?start_key=%s&end_key=%s",
		util.InternalHTTPSchema(),
		pdAddrs[0],
		url.QueryEscape(string(startKey)),
		url.QueryEscape(string(endKey)))

	resp, err := util.InternalHTTPClient().Get(statURL)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if err = resp.Body.Close(); err != nil {
			logutil.BgLogger().Error("err", zap.Error(err))
		}
	}()

	dec := json.NewDecoder(resp.Body)

	return dec.Decode(stats)
}

// DeletePlacementRule is to delete placement rule for certain group.
func (h *Helper) DeletePlacementRule(group string, ruleID string) error {
	pdAddrs, err := h.GetPDAddr()
	if err != nil {
		return errors.Trace(err)
	}

	deleteURL := fmt.Sprintf("%s://%s/pd/api/v1/config/rule/%v/%v",
		util.InternalHTTPSchema(),
		pdAddrs[0],
		group,
		ruleID,
	)

	req, err := http.NewRequest("DELETE", deleteURL, nil)
	if err != nil {
		return errors.Trace(err)
	}

	resp, err := util.InternalHTTPClient().Do(req)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if err = resp.Body.Close(); err != nil {
			logutil.BgLogger().Error("err", zap.Error(err))
		}
	}()
	if resp.StatusCode != http.StatusOK {
		return errors.New("DeletePlacementRule returns error")
	}
	return nil
}

// SetPlacementRule is a helper function to set placement rule.
func (h *Helper) SetPlacementRule(rule placement.Rule) error {
	pdAddrs, err := h.GetPDAddr()
	if err != nil {
		return errors.Trace(err)
	}
	m, _ := json.Marshal(rule)

	postURL := fmt.Sprintf("%s://%s/pd/api/v1/config/rule",
		util.InternalHTTPSchema(),
		pdAddrs[0],
	)
	buf := bytes.NewBuffer(m)
	resp, err := util.InternalHTTPClient().Post(postURL, "application/json", buf)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if err = resp.Body.Close(); err != nil {
			logutil.BgLogger().Error("err", zap.Error(err))
		}
	}()
	if resp.StatusCode != http.StatusOK {
		return errors.New("SetPlacementRule returns error")
	}
	return nil
}

// GetGroupRules to get all placement rule in a certain group.
func (h *Helper) GetGroupRules(group string) ([]placement.Rule, error) {
	pdAddrs, err := h.GetPDAddr()
	if err != nil {
		return nil, errors.Trace(err)
	}

	getURL := fmt.Sprintf("%s://%s/pd/api/v1/config/rules/group/%s",
		util.InternalHTTPSchema(),
		pdAddrs[0],
		group,
	)

	resp, err := util.InternalHTTPClient().Get(getURL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		if err = resp.Body.Close(); err != nil {
			logutil.BgLogger().Error("err", zap.Error(err))
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("GetGroupRules returns error")
	}

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(resp.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var rules []placement.Rule
	err = json.Unmarshal(buf.Bytes(), &rules)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return rules, nil
}

// PostAccelerateSchedule sends `regions/accelerate-schedule` request.
func (h *Helper) PostAccelerateSchedule(tableID int64) error {
	pdAddrs, err := h.GetPDAddr()
	if err != nil {
		return errors.Trace(err)
	}
	startKey := tablecodec.GenTableRecordPrefix(tableID)
	endKey := tablecodec.EncodeTablePrefix(tableID + 1)
	startKey = codec.EncodeBytes([]byte{}, startKey)
	endKey = codec.EncodeBytes([]byte{}, endKey)

	postURL := fmt.Sprintf("%s://%s/pd/api/v1/regions/accelerate-schedule",
		util.InternalHTTPSchema(),
		pdAddrs[0])

	input := map[string]string{
		"start_key": url.QueryEscape(string(startKey)),
		"end_key":   url.QueryEscape(string(endKey)),
	}
	v, err := json.Marshal(input)
	if err != nil {
		return errors.Trace(err)
	}
	resp, err := util.InternalHTTPClient().Post(postURL, "application/json", bytes.NewBuffer(v))
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if err = resp.Body.Close(); err != nil {
			logutil.BgLogger().Error("err", zap.Error(err))
		}
	}()
	return nil
}

// GetPDRegionRecordStats is a helper function calling `/stats/region`.
func (h *Helper) GetPDRegionRecordStats(tableID int64, stats *PDRegionStats) error {
	pdAddrs, err := h.GetPDAddr()
	if err != nil {
		return errors.Trace(err)
	}

	startKey := tablecodec.GenTableRecordPrefix(tableID)
	endKey := tablecodec.EncodeTablePrefix(tableID + 1)
	startKey = codec.EncodeBytes([]byte{}, startKey)
	endKey = codec.EncodeBytes([]byte{}, endKey)

	statURL := fmt.Sprintf("%s://%s/pd/api/v1/stats/region?start_key=%s&end_key=%s",
		util.InternalHTTPSchema(),
		pdAddrs[0],
		url.QueryEscape(string(startKey)),
		url.QueryEscape(string(endKey)))

	resp, err := util.InternalHTTPClient().Get(statURL)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if err = resp.Body.Close(); err != nil {
			logutil.BgLogger().Error("err", zap.Error(err))
		}
	}()

	dec := json.NewDecoder(resp.Body)

	return dec.Decode(stats)
}

// GetTiFlashTableIDFromEndKey computes tableID from pd rule's endKey.
func GetTiFlashTableIDFromEndKey(endKey string) int64 {
	e, _ := hex.DecodeString(endKey)
	_, decodedEndKey, _ := codec.DecodeBytes(e, []byte{})
	tableID := tablecodec.DecodeTableID(decodedEndKey)
	tableID -= 1
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
		realN += 1
		r, err := strconv.ParseInt(s, 10, 32)
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
func CollectTiFlashStatus(statusAddress string, tableID int64, regionReplica *map[int64]int) error {
	statURL := fmt.Sprintf("%s://%s/tiflash/sync-status/%d",
		util.InternalHTTPSchema(),
		statusAddress,
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
