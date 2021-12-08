package unistore

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/store/mockstore/mockstorage"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type KvPair struct {
	Key   []byte
	Value []byte
}

type RegionInfo struct {
	ID      uint64
	Version uint64
	ConfVer uint64
	Start   []byte
	End     []byte
	Pairs   []KvPair
}

type TableInfo struct {
	ID      int64
	Regions []RegionInfo
}

type TestGenConfig struct {
	TableOfInterest []int64
	store           kv.Storage
	dom             *domain.Domain
	TableData       map[int64]*TableInfo
}

func getMockedRegionInfo(store kv.Storage, tblInfo *model.TableInfo) ([]RegionInfo, error) {
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
		scanReq := &kvrpcpb.ScanRequest{StartKey: start, EndKey: end, Context: &kvrpcpb.Context{RegionId: region.Meta.Id, RegionEpoch: region.Meta.RegionEpoch},
			Version: math.MaxUint64,
			Limit:   math.MaxUint32}
		req := tikvrpc.NewRequest(tikvrpc.CmdScan, scanReq)

		ctx := context.Background()
		// TODO(zhifeng): determin the correct store name
		resp, err := kvClient.SendRequest(ctx, "store1", req, 10*time.Second)
		if err != nil {
			return nil, err
		}
		if resp.Resp == nil {
			return nil, fmt.Errorf("resp is nil")
		}
		scanResp := resp.Resp.(*kvrpcpb.ScanResponse)
		fmt.Printf("number of KV in region %d is %d\n", region.Meta.Id, len(scanResp.Pairs))
		for _, pair := range scanResp.Pairs {
			if tablecodec.DecodeTableID(pair.Key) == tblInfo.ID {
				fmt.Printf("key: %v, value: %v\n", pair.Key, pair.Value)
				regionInfo.Pairs = append(regionInfo.Pairs, KvPair{Key: pair.Key, Value: pair.Value})
			}
		}
		regionInfos = append(regionInfos, regionInfo)
	}

	return regionInfos, err
}

// Init initializes the configuration with core facilities. Must call before calling other functions.
func (c *TestGenConfig) Init(store kv.Storage, dom *domain.Domain) error {
	c.store = store
	c.dom = dom
	c.TableData = make(map[int64]*TableInfo)
	return nil
}

// AddTable adds a table into the table of interest. Table data is dumped at the same time. All DAG Reqeusts on this table will be recorded for replaying with the dumped data.
func (c *TestGenConfig) AddTable(dbName, tblName string) error {
	tbl, err := c.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	fmt.Println(tbl.Meta().ID)
	c.TableOfInterest = append(c.TableOfInterest, tbl.Meta().ID)
	regions, err := getMockedRegionInfo(c.store, tbl.Meta())
	if err != nil {
		return err
	}
	fmt.Printf("number of regions of table %s.%s is %d\n", dbName, tblName, len(regions))
	tableInfo := &TableInfo{
		ID:      tbl.Meta().ID,
		Regions: make([]RegionInfo, 0, len(regions)),
	}
	c.TableData[tbl.Meta().ID] = tableInfo

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
