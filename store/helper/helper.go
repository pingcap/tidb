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
// See the License for the specific language governing permissions and
// limitations under the License.

package helper

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/pdapi"
	"go.uber.org/zap"
)

const (
	protocol = "http://"
)

// Helper is a middleware to get some information from tikv/pd. It can be used for TiDB's http api or mem table.
type Helper struct {
	Store       tikv.Storage
	RegionCache *tikv.RegionCache
}

// NewHelper get a Helper from Storage
func NewHelper(store tikv.Storage) *Helper {
	return &Helper{
		Store:       store,
		RegionCache: store.GetRegionCache(),
	}
}

// GetMvccByEncodedKey get the MVCC value by the specific encoded key.
func (h *Helper) GetMvccByEncodedKey(encodedKey kv.Key) (*kvrpcpb.MvccGetByKeyResponse, error) {
	keyLocation, err := h.RegionCache.LocateKey(tikv.NewBackoffer(context.Background(), 500), encodedKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tikvReq := tikvrpc.NewRequest(tikvrpc.CmdMvccGetByKey, &kvrpcpb.MvccGetByKeyRequest{Key: encodedKey})
	kvResp, err := h.Store.SendReq(tikv.NewBackoffer(context.Background(), 500), tikvReq, keyLocation.Region, time.Minute)
	if err != nil {
		logutil.BgLogger().Info("get MVCC by encoded key failed",
			zap.Stringer("encodeKey", encodedKey),
			zap.Reflect("region", keyLocation.Region),
			zap.Stringer("startKey", keyLocation.StartKey),
			zap.Stringer("endKey", keyLocation.EndKey),
			zap.Reflect("kvResp", kvResp),
			zap.Error(err))
		return nil, errors.Trace(err)
	}
	return kvResp.Resp.(*kvrpcpb.MvccGetByKeyResponse), nil
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
	etcd, ok := h.Store.(tikv.EtcdBackend)
	if !ok {
		return nil, errors.WithStack(errors.New("not implemented"))
	}
	pdHosts := etcd.EtcdAddrs()
	if len(pdHosts) == 0 {
		return nil, errors.New("pd unavailable")
	}
	req, err := http.NewRequest("GET", protocol+pdHosts[0]+rw, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		err = resp.Body.Close()
		if err != nil {
			logutil.BgLogger().Error("close body failed", zap.Error(err))
		}
	}()
	var regionResp StoreHotRegionInfos
	err = json.NewDecoder(resp.Body).Decode(&regionResp)
	if err != nil {
		return nil, errors.Trace(err)
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
		t := HotTableIndex{RegionID: regionID, RegionMetric: &regionMetric}
		region, err := h.RegionCache.LocateRegionByID(tikv.NewBackoffer(context.Background(), 500), regionID)
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
		return findRangeInPhysicalTable(hotRange, tbl.ID, db.Name.O, tbl.Name.O, tbl.Indices)
	}

	for _, def := range pi.Definitions {
		tablePartition := fmt.Sprintf("%s(%s)", tbl.Name.O, def.Name)
		if f := findRangeInPhysicalTable(hotRange, def.ID, db.Name.O, tablePartition, tbl.Indices); f != nil {
			return f
		}
	}
	return nil
}

func findRangeInPhysicalTable(hotRange *RegionFrameRange, physicalID int64, dbName, tblName string, indices []*model.IndexInfo) *FrameItem {
	if f := hotRange.GetRecordFrame(physicalID, dbName, tblName); f != nil {
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
			_, frame.RecordID, err = tablecodec.DecodeRecordKey(key)
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
func (r *RegionFrameRange) GetRecordFrame(tableID int64, dbName, tableName string) *FrameItem {
	if tableID == r.First.TableID && r.First.IsRecord {
		r.First.DBName, r.First.TableName = dbName, tableName
		return r.First
	}
	if tableID == r.Last.TableID && r.Last.IsRecord {
		r.Last.DBName, r.Last.TableName = dbName, tableName
		return r.Last
	}

	if tableID >= r.First.TableID && tableID < r.Last.TableID {
		return &FrameItem{
			DBName:    dbName,
			TableName: tableName,
			TableID:   tableID,
			IsRecord:  true,
		}
	}
	return nil
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
	RegionPeer
	DownSec int64 `json:"down_seconds"`
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
	WrittenBytes    int64            `json:"written_bytes"`
	ReadBytes       int64            `json:"read_bytes"`
	ApproximateSize int64            `json:"approximate_size"`
	ApproximateKeys int64            `json:"approximate_keys"`
}

// RegionsInfo stores the information of regions.
type RegionsInfo struct {
	Count   int64        `json:"count"`
	Regions []RegionInfo `json:"regions"`
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

// tableInfoWithKeyRange stores table or index informations with its key range.
type tableInfoWithKeyRange struct {
	*TableInfo
	StartKey string
	EndKey   string
}

func (t tableInfoWithKeyRange) getStartKey() string { return t.StartKey }
func (t tableInfoWithKeyRange) getEndKey() string   { return t.EndKey }

// for sorting
type byTableStartKey []tableInfoWithKeyRange

func (xs byTableStartKey) Len() int      { return len(xs) }
func (xs byTableStartKey) Swap(i, j int) { xs[i], xs[j] = xs[j], xs[i] }
func (xs byTableStartKey) Less(i, j int) bool {
	return xs[i].getStartKey() < xs[j].getStartKey()
}

func newTableWithKeyRange(db *model.DBInfo, table *model.TableInfo) tableInfoWithKeyRange {
	sk, ek := tablecodec.GetTableHandleKeyRange(table.ID)
	startKey := bytesKeyToHex(codec.EncodeBytes(nil, sk))
	endKey := bytesKeyToHex(codec.EncodeBytes(nil, ek))
	return tableInfoWithKeyRange{
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

func newIndexWithKeyRange(db *model.DBInfo, table *model.TableInfo, index *model.IndexInfo) tableInfoWithKeyRange {
	sk, ek := tablecodec.GetTableIndexKeyRange(table.ID, index.ID)
	startKey := bytesKeyToHex(codec.EncodeBytes(nil, sk))
	endKey := bytesKeyToHex(codec.EncodeBytes(nil, ek))
	return tableInfoWithKeyRange{
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

// GetRegionsTableInfo returns a map maps region id to its tables or indices.
// Assuming tables or indices key ranges never intersect.
// Regions key ranges can intersect.
func (h *Helper) GetRegionsTableInfo(regionsInfo *RegionsInfo, schemas []*model.DBInfo) map[int64][]TableInfo {
	tableInfos := make(map[int64][]TableInfo, len(regionsInfo.Regions))

	regions := make([]*RegionInfo, 0, len(regionsInfo.Regions))
	for i := 0; i < len(regionsInfo.Regions); i++ {
		tableInfos[regionsInfo.Regions[i].ID] = []TableInfo{}
		regions = append(regions, &regionsInfo.Regions[i])
	}

	tables := []tableInfoWithKeyRange{}
	for _, db := range schemas {
		for _, table := range db.Tables {
			tables = append(tables, newTableWithKeyRange(db, table))
			for _, index := range table.Indices {
				tables = append(tables, newIndexWithKeyRange(db, table, index))
			}
		}
	}

	if len(tables) == 0 || len(regions) == 0 {
		return tableInfos
	}

	sort.Sort(byRegionStartKey(regions))
	sort.Sort(byTableStartKey(tables))

	idx := 0
OutLoop:
	for _, region := range regions {
		id := region.ID
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

// GetRegionsInfo gets the region information of current store by using PD's api.
func (h *Helper) GetRegionsInfo() (*RegionsInfo, error) {
	var regionsInfo RegionsInfo
	err := h.requestPD("GET", pdapi.Regions, nil, &regionsInfo)
	return &regionsInfo, err
}

// GetRegionInfoByID gets the region information of the region ID by using PD's api.
func (h *Helper) GetRegionInfoByID(regionID uint64) (*RegionInfo, error) {
	var regionInfo RegionInfo
	err := h.requestPD("GET", pdapi.RegionByID+strconv.FormatUint(regionID, 10), nil, &regionInfo)
	return &regionInfo, err
}

// request PD API, decode the response body into res
func (h *Helper) requestPD(method, uri string, body io.Reader, res interface{}) error {
	etcd, ok := h.Store.(tikv.EtcdBackend)
	if !ok {
		return errors.WithStack(errors.New("not implemented"))
	}
	pdHosts := etcd.EtcdAddrs()
	if len(pdHosts) == 0 {
		return errors.New("pd unavailable")
	}

	logutil.BgLogger().Debug("RequestPD URL", zap.String("url", protocol+pdHosts[0]+uri))
	req, err := http.NewRequest(method, protocol+pdHosts[0]+uri, body)
	if err != nil {
		return errors.Trace(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.Trace(err)
	}

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
	etcd, ok := h.Store.(tikv.EtcdBackend)
	if !ok {
		return nil, errors.WithStack(errors.New("not implemented"))
	}
	pdHosts := etcd.EtcdAddrs()
	if len(pdHosts) == 0 {
		return nil, errors.New("pd unavailable")
	}
	req, err := http.NewRequest("GET", protocol+pdHosts[0]+pdapi.Stores, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		err = resp.Body.Close()
		if err != nil {
			logutil.BgLogger().Error("close body failed", zap.Error(err))
		}
	}()
	var storesStat StoresStat
	err = json.NewDecoder(resp.Body).Decode(&storesStat)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &storesStat, nil
}
