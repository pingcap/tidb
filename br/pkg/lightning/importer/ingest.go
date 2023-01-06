// Copyright 2022 PingCAP, Inc.
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

package importer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	sstpb "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/br/pkg/lightning/importer/kv"
	"github.com/pingcap/tidb/util/codec"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	maxGRPCMsgSize  = 128 << 20
	regionSplitSize = 96 << 20
	regionSplitKeys = 1_280_000
	ingestWorkerNum = 16
)

type Ingestor struct {
	kvStore          *tikv.KVStore
	importClientPool *importClientPool
}

func NewIngestor(pdAddr string) (*Ingestor, error) {
	maxCallMsgSize := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxGRPCMsgSize)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxGRPCMsgSize)),
	}
	pdCli, err := pd.NewClientWithContext(
		context.Background(), []string{pdAddr}, pd.SecurityOption{},
		pd.WithGRPCDialOptions(maxCallMsgSize...),
		// If the time too short, we may scatter a region many times, because
		// the interface `ScatterRegions` may time out.
		pd.WithCustomTimeoutOption(60*time.Second),
		pd.WithMaxErrorRetry(3),
	)
	if err != nil {
		return nil, err
	}
	pdClient := tikv.CodecPDClient{Client: pdCli}
	spkv, err := tikv.NewEtcdSafePointKV([]string{pdAddr}, nil)
	if err != nil {
		pdClient.Close()
		return nil, err
	}
	kvStore, err := tikv.NewKVStore("tikv-importer", &pdClient, spkv, tikv.NewRPCClient())
	if err != nil {
		pdClient.Close()
		_ = spkv.Close()
		return nil, err
	}
	pool := &importClientPool{
		pdCli: kvStore.GetPDClient(),
		g:     &singleflight.Group{},
	}
	return &Ingestor{kvStore: kvStore, importClientPool: pool}, nil
}

func (in *Ingestor) Ingest(ctx context.Context, kr kv.KeyRange, endpoints []string) error {
	var clients []*Client
	for _, endpoint := range endpoints {
		client, err := NewClient(endpoint)
		if err != nil {
			return err
		}
		clients = append(clients, client)
	}

	var allProps []kv.Properties
	for _, client := range clients {
		props, err := client.Properties(ctx)
		if err != nil {
			return err
		}
		allProps = append(allProps, props)
	}
	props := kv.MergeProperties(allProps...)
	kr = kr.Intersect(props.Range)

	if kr.IsEmpty() {
		return nil
	}
	if len(kr.StartKey) == 0 || len(kr.EndKey) == 0 {
		panic("unbounded range")
	}
	if err := in.splitRegions(ctx, kr, props.RangeProps); err != nil {
		return err
	}

	physical, logical, err := in.kvStore.GetPDClient().GetTS(ctx)
	if err != nil {
		return err
	}
	commitTS := oracle.ComposeTS(physical, logical)

	taskCh := make(chan *ingestTask, ingestWorkerNum)

	var wg sync.WaitGroup
	wg.Add(ingestWorkerNum)
	for i := 0; i < ingestWorkerNum; i++ {
		w := &ingestWorker{
			clients:          clients,
			taskCh:           taskCh,
			commitTS:         commitTS,
			importClientPool: in.importClientPool,
		}
		go func() {
			defer wg.Done()
			w.run(ctx)
		}()
	}

	defer func() {
		close(taskCh)
		wg.Wait()
	}()

	doneRanges := &kv.KeyRanges{}
	for {
		tasks, err := in.makeTasks(ctx, kr, doneRanges)
		if err != nil {
			return err
		}
		if len(tasks) == 0 {
			return nil
		}
		for _, task := range tasks {
			select {
			case taskCh <- task:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		for _, task := range tasks {
			if err := <-task.done; err != nil {
				log.Printf("failed to ingest region %d: %v", task.region.Meta.Id, err)
			} else {
				doneRanges.Add(task.kr)
			}
		}
	}
}

func (in *Ingestor) makeTasks(ctx context.Context, kr kv.KeyRange, doneRanges *kv.KeyRanges) ([]*ingestTask, error) {
	regions, err := in.scanRegions(ctx, kr)
	if err != nil {
		return nil, err
	}
	var tasks []*ingestTask
	for _, region := range regions {
		regionRange := kv.KeyRange{
			StartKey: region.Meta.StartKey,
			EndKey:   region.Meta.EndKey,
		}

		for _, undone := range doneRanges.AbsentFromSelf(regionRange) {
			tasks = append(tasks, &ingestTask{
				kr:     undone,
				region: region,
				done:   make(chan error, 1),
			})
		}
	}
	return tasks, nil
}

func (in *Ingestor) scanRegions(ctx context.Context, kr kv.KeyRange) ([]*pd.Region, error) {
	pdCli := in.kvStore.GetPDClient()

	var result []*pd.Region
	for !kr.IsEmpty() {
		regions, err := pdCli.ScanRegions(ctx, kr.StartKey, kr.EndKey, 1000)
		if err != nil {
			return nil, err
		}
		if len(regions) == 0 {
			break
		}
		result = append(result, regions...)
		kr.StartKey = regions[len(regions)-1].Meta.EndKey
	}
	return result, nil
}

func (in *Ingestor) splitRegions(ctx context.Context, full kv.KeyRange, rangeProps kv.RangeProperties) error {
	splitKeys := in.estimateSplitKeys(full, rangeProps)
	regionIDs, err := in.kvStore.SplitRegions(ctx, splitKeys, true, nil)
	if err != nil {
		return err
	}
	for _, regionID := range regionIDs {
		if err := in.kvStore.WaitScatterRegionFinish(ctx, regionID, 0); err != nil {
			return err
		}
	}
	return err
}

func (in *Ingestor) estimateSplitKeys(full kv.KeyRange, rangeProps kv.RangeProperties) [][]byte {
	splitKeys := [][]byte{full.StartKey}
	lastSize := int64(0)
	lastKeys := 0
	for _, rangeOff := range rangeProps.Offsets {
		if rangeOff.Size-lastSize >= regionSplitSize || rangeOff.Keys-lastKeys >= regionSplitKeys {
			splitKeys = append(splitKeys, rangeOff.Key)
			lastSize = rangeOff.Size
			lastKeys = rangeOff.Keys
		}
	}
	splitKeys = append(splitKeys, full.EndKey)
	return splitKeys
}

func (in *Ingestor) Close() error {
	in.importClientPool.close()
	return in.kvStore.Close()
}

type ingestTask struct {
	kr     kv.KeyRange
	region *pd.Region
	done   chan error
}

type ingestWorker struct {
	clients          []*Client
	taskCh           <-chan *ingestTask
	commitTS         uint64
	importClientPool *importClientPool
}

func (w *ingestWorker) run(ctx context.Context) {
	for {
		select {
		case task, ok := <-w.taskCh:
			if !ok {
				return
			}
			task.done <- w.ingest(ctx, task.region, task.kr)
		case <-ctx.Done():
			return
		}
	}
}

func (w *ingestWorker) ingest(ctx context.Context, region *pd.Region, kr kv.KeyRange) error {
	log.Printf("ingesting region %d", region.Meta.Id)

	reader, err := w.newKVReader(ctx, kr)
	if err != nil {
		return err
	}
	defer reader.Close()

	var (
		leaderIdx    = -1
		leaderClinet sstpb.ImportSSTClient
		writeClients []sstpb.ImportSST_WriteClient
	)
	closeAll := func() {
		for _, c := range writeClients {
			_ = c.CloseSend()
		}
	}

	u := uuid.New()
	meta := &sstpb.SSTMeta{
		Uuid:        u[:],
		RegionId:    region.Meta.GetId(),
		RegionEpoch: region.Meta.GetRegionEpoch(),
		Range: &sstpb.Range{
			Start: codec.EncodeBytes(nil, kr.StartKey),
			// It's not easy to get the last key of the scan range,
			// so we use the start key to skip check_key_in_region.
			End: codec.EncodeBytes(nil, kr.StartKey),
		},
	}

	for i := 0; i < len(region.Meta.Peers); i++ {
		peer := region.Meta.Peers[i]
		importClient, err := w.importClientPool.get(ctx, peer.StoreId)
		if err != nil {
			closeAll()
			return err
		}
		if peer.Id == region.Leader.GetId() {
			leaderIdx = i
			leaderClinet = importClient
		}
		writeClient, err := importClient.Write(ctx)
		if err != nil {
			closeAll()
			return err
		}
		writeClients = append(writeClients, writeClient)

		if err := writeClient.Send(&sstpb.WriteRequest{
			Chunk: &sstpb.WriteRequest_Meta{
				Meta: meta,
			},
		}); err != nil {
			closeAll()
			return err
		}
	}
	if leaderIdx == -1 {
		closeAll()
		return fmt.Errorf("region %d has no leader", region.Meta.Id)
	}

	writeBatch := &sstpb.WriteBatch{
		CommitTs: w.commitTS,
	}

	req := &sstpb.WriteRequest{
		Chunk: &sstpb.WriteRequest_Batch{
			Batch: writeBatch,
		},
	}

	const writeBatchSize = 1024

	for {
		key, val, err := reader.Read()
		if err != nil {
			closeAll()
			return err
		}
		if key != nil {
			writeBatch.Pairs = append(writeBatch.Pairs, &sstpb.Pair{
				Key:   append([]byte(nil), key...),
				Value: append([]byte(nil), val...),
			})
		}
		if key == nil || len(writeBatch.Pairs) >= writeBatchSize {
			for _, wc := range writeClients {
				if err := wc.Send(req); err != nil {
					closeAll()
					return err
				}
			}
			writeBatch.Pairs = writeBatch.Pairs[:0]
		}
		if key == nil {
			break
		}
	}

	var sstMetas []*sstpb.SSTMeta
	for i, wc := range writeClients {
		resp, err := wc.CloseAndRecv()
		if err != nil {
			closeAll()
			return err
		}
		if i == leaderIdx {
			sstMetas = resp.Metas
		}
	}
	if len(sstMetas) == 0 {
		return errors.New("no sst meta received from leader")
	}

	reqCtx := &kvrpcpb.Context{
		RegionId:    region.Meta.GetId(),
		RegionEpoch: region.Meta.GetRegionEpoch(),
		Peer:        region.Leader,
	}

	resp, err := leaderClinet.MultiIngest(ctx, &sstpb.MultiIngestRequest{
		Context: reqCtx,
		Ssts:    sstMetas,
	})
	if err != nil {
		return err
	}
	if resp.Error != nil {
		return errors.New(resp.Error.String())
	}
	return nil
}

func (w *ingestWorker) newKVReader(ctx context.Context, kr kv.KeyRange) (kv.Reader, error) {
	var readers []kv.Reader
	for _, client := range w.clients {
		reader, err := client.NewReader(ctx, kr.StartKey, kr.EndKey)
		if err != nil {
			return nil, err
		}
		readers = append(readers, reader)
	}
	return kv.MergeReaders(readers...)
}

type importClientPool struct {
	pdCli pd.Client
	conns sync.Map // storeID -> *grpc.ClientConn
	g     *singleflight.Group
}

func (p *importClientPool) get(ctx context.Context, storeID uint64) (sstpb.ImportSSTClient, error) {
	if v, ok := p.conns.Load(storeID); ok {
		return sstpb.NewImportSSTClient(v.(*grpc.ClientConn)), nil
	}
	v, err, _ := p.g.Do(strconv.FormatUint(storeID, 10), func() (interface{}, error) {
		store, err := p.pdCli.GetStore(ctx, storeID)
		if err != nil {
			return nil, err
		}
		conn, err := grpc.Dial(store.GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		p.conns.Store(storeID, conn)
		return conn, nil
	})
	if err != nil {
		return nil, err
	}
	conn := v.(*grpc.ClientConn)
	return sstpb.NewImportSSTClient(conn), nil
}

func (p *importClientPool) close() {
	p.conns.Range(func(key, value interface{}) bool {
		_ = value.(*grpc.ClientConn).Close()
		p.conns.Delete(key)
		return true
	})
}
