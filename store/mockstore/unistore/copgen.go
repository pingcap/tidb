package unistore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/store/mockstore/mockstorage"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type KvPair struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

type Request struct {
	Type     tikvrpc.CmdType `json:"type"`
	Plan     string          `json:"plan"`
	Request  []byte          `json:"request"`
	Response []byte          `json:"response"`
}

type RegionInfo struct {
	ID      uint64   `json:"id"`
	Version uint64   `json:"version"`
	ConfVer uint64   `json:"conf_ver"`
	Start   []byte   `json:"start"`
	End     []byte   `json:"end"`
	Pairs   []KvPair `json:"pairs"`
}

type TableInfo struct {
	ID      int64            `json:"id"`
	Regions []RegionInfo     `json:"regions"`
	Meta    *model.TableInfo `json:"meta"`
}

type TestGenConfig struct {
	TableOfInterest []int64 `json:"table_of_interest"`
	store           kv.Storage
	dom             *domain.Domain
	Cluster         *Cluster
	TableData       map[int64]*TableInfo `json:"table_data"`
	RequestData     []Request            `json:"request_data"`
}

func (c *TestGenConfig) AddRequest(reqType tikvrpc.CmdType, req []byte, plan string, resp []byte) {
	c.RequestData = append(c.RequestData, Request{
		Type:     reqType,
		Request:  req,
		Plan:     plan,
		Response: resp,
	})
}

func (c *TestGenConfig) Serialize() ([]byte, error) {
	return json.MarshalIndent(c, "", "  ")
}

func (c *TestGenConfig) getMockedRegionInfo(store kv.Storage, tblInfo *model.TableInfo) ([]RegionInfo, error) {
	mockStorage, ok := store.(*mockstorage.MockStorage)
	if !ok {
		return nil, fmt.Errorf("the store is not a mockStorage")
	}
	pdClient := mockStorage.GetPDClient()
	start, end := tablecodec.GetTableHandleKeyRange(tblInfo.ID)
	ctx := context.Background()
	regions, err := pdClient.ScanRegions(ctx, start, end, -1)
	if err != nil {
		return nil, err
	}
	kvClient := mockStorage.GetTiKVClient()
	regionInfos := make([]RegionInfo, 0, len(regions))
	// get region data from kv using scan
	for _, region := range regions {
		start := region.Meta.StartKey
		end := region.Meta.EndKey
		regionInfo := RegionInfo{
			ID:      region.Meta.Id,
			Version: region.Meta.RegionEpoch.Version,
			ConfVer: region.Meta.RegionEpoch.ConfVer,
			Start:   start,
			End:     end,
			Pairs:   make([]KvPair, 0),
		}
		scanReq := &kvrpcpb.ScanRequest{
			StartKey: start,
			EndKey:   end,
			Context:  &kvrpcpb.Context{RegionId: region.Meta.Id, RegionEpoch: region.Meta.RegionEpoch},
			Version:  math.MaxUint64,
			Limit:    math.MaxUint32}
		req := tikvrpc.NewRequest(tikvrpc.CmdScan, scanReq)

		ctx := context.Background()
		resp, err := kvClient.SendRequest(ctx, c.Cluster.GetAllStores()[0].Address, req, 10*time.Second)
		if err != nil {
			return nil, err
		}
		if resp.Resp == nil {
			return nil, fmt.Errorf("resp is nil")
		}
		scanResp := resp.Resp.(*kvrpcpb.ScanResponse)
		for _, pair := range scanResp.Pairs {
			if tablecodec.DecodeTableID(pair.Key) == tblInfo.ID {
				regionInfo.Pairs = append(regionInfo.Pairs, KvPair{Key: pair.Key, Value: pair.Value})
			}
		}
		regionInfos = append(regionInfos, regionInfo)
	}
	return regionInfos, err
}

// Init initializes the configuration with core facilities. Must call before calling other functions.
func (c *TestGenConfig) Init(store kv.Storage, dom *domain.Domain) {
	c.store = store
	c.dom = dom
	c.TableData = make(map[int64]*TableInfo)
}

// AddTable adds a table into the table of interest.
func (c *TestGenConfig) AddTable(dbName, tblName string) error {
	tbl, err := c.dom.InfoSchema().TableByName(model.NewCIStr(dbName), model.NewCIStr(tblName))
	if err != nil {
		return err
	}
	c.TableOfInterest = append(c.TableOfInterest, tbl.Meta().ID)
	return nil
}

// RemoveTable removes the table from the table of interest.
func (c *TestGenConfig) RemoveTable(dbName, tblName string) error {
	tbl, err := c.dom.InfoSchema().TableByName(model.NewCIStr(dbName), model.NewCIStr(tblName))
	if err != nil {
		return err
	}
	for i, id := range c.TableOfInterest {
		if id == tbl.Meta().ID {
			c.TableOfInterest = append(c.TableOfInterest[:i], c.TableOfInterest[i+1:]...)
			break
		}
	}
	return nil
}

func (c *TestGenConfig) split(key []byte, splitKey []byte) {
	region, peers, _ := c.Cluster.GetRegionByKey(key)
	if bytes.Compare(region.StartKey, key) != 0 {
		newRegionID := c.Cluster.AllocID()
		peersID := make([]uint64, 0, len(region.Peers))
		for _, peer := range region.Peers {
			peersID = append(peersID, peer.Id)
		}
		c.Cluster.Split(region.Id, newRegionID, splitKey, peersID, peers.Id)
	}
}

func (c *TestGenConfig) dumpTable(tblID int64) error {
	tbl, exists := c.dom.InfoSchema().TableByID(tblID)
	if !exists {
		return fmt.Errorf("table %d does not exist", tblID)
	}
	var tableStart, tableEnd []byte
	tableStart = tablecodec.GenTableRecordPrefix(tbl.Meta().ID)
	tableEnd = tablecodec.GenTableRecordPrefix(tbl.Meta().ID).PrefixNext()
	rawStartKey := codec.EncodeBytes(nil, tableStart)
	rawEndKey := codec.EncodeBytes(nil, tableEnd)

	c.split(rawStartKey, tableStart)
	c.split(rawEndKey, tableEnd)

	regions, err := c.getMockedRegionInfo(c.store, tbl.Meta())
	if err != nil {
		return err
	}
	tableInfo := &TableInfo{
		ID:      tbl.Meta().ID,
		Regions: regions,
		Meta:    tbl.Meta(),
	}
	c.TableData[tbl.Meta().ID] = tableInfo
	return nil
}

// Prepare prepares the data of table of interests, and afterward DAG requests on tables of interest will be recorded for replaying with the dumped data.
func (c *TestGenConfig) Prepare() error {
	for _, tblID := range c.TableOfInterest {
		err := c.dumpTable(tblID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *TestGenConfig) Dump(path string) error {
	// create file if not exists
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	// serialize datat to JSON and write to file
	data, err := c.Serialize()
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func (c *TestGenConfig) IsTableInterested(tid int64) bool {
	for _, v := range c.TableOfInterest {
		if tid == v {
			return true
		}
	}
	return false
}
