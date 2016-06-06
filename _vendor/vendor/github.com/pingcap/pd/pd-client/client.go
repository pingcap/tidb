package pd

import (
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"golang.org/x/net/context"
)

const (
	requestTimeout    = 3 * time.Second
	maxRetryGetLeader = 100
)

// Client is a PD (Placement Driver) client.
// It should not be used after calling Close().
type Client interface {
	// GetTS gets a timestamp from PD.
	GetTS() (int64, int64, error)
	// GetRegion gets a region from PD by key.
	// The region may expire after split. Caller is responsible for caching and
	// taking care of region change.
	GetRegion(key []byte) (*metapb.Region, error)
	// GetStore gets a store from PD by store id.
	// The store may expire later. Caller is responsible for caching and taking care
	// of store change.
	GetStore(storeID uint64) (*metapb.Store, error)
	// Close closes the client.
	Close()
}

type client struct {
	clusterID   uint64
	etcdClient  *clientv3.Client
	workerMutex sync.RWMutex
	worker      *rpcWorker
	wg          sync.WaitGroup
	quit        chan struct{}
}

func getLeaderPath(clusterID uint64, rootPath string) string {
	return path.Join(rootPath, strconv.FormatUint(clusterID, 10), "leader")
}

// NewClient creates a PD client.
func NewClient(etcdAddrs []string, rootPath string, clusterID uint64) (Client, error) {
	log.Infof("[pd] create etcd client with endpoints %v", etcdAddrs)
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdAddrs,
		DialTimeout: requestTimeout,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	leaderPath := getLeaderPath(clusterID, rootPath)

	var (
		leaderAddr string
		revision   int64
	)

	for i := 0; i < maxRetryGetLeader; i++ {
		leaderAddr, revision, err = getLeader(etcdClient, leaderPath)
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if err != nil {
		return nil, errors.Trace(err)
	}

	client := &client{
		clusterID:  clusterID,
		etcdClient: etcdClient,
		worker:     newRPCWorker(leaderAddr, clusterID),
		quit:       make(chan struct{}),
	}

	client.wg.Add(1)
	go client.watchLeader(leaderPath, revision)

	return client, nil
}

func (c *client) Close() {
	c.etcdClient.Close()

	close(c.quit)
	// Must wait watchLeader done.
	c.wg.Wait()
	c.worker.stop(errors.New("[pd] pd-client closing"))
}

func (c *client) GetTS() (int64, int64, error) {
	req := &tsoRequest{
		done: make(chan error),
	}
	c.workerMutex.RLock()
	c.worker.requests <- req
	c.workerMutex.RUnlock()
	err := <-req.done
	return req.physical, req.logical, err
}

func (c *client) GetRegion(key []byte) (*metapb.Region, error) {
	req := &regionRequest{
		pbReq: &pdpb.GetRegionRequest{
			RegionKey: key,
		},
		done: make(chan error),
	}
	c.workerMutex.RLock()
	c.worker.requests <- req
	c.workerMutex.RUnlock()
	err := <-req.done
	if err != nil {
		return nil, errors.Trace(err)
	}
	region := req.pbResp.GetRegion()
	if region == nil {
		return nil, errors.New("[pd] region field in rpc response not set")
	}
	return region, nil
}

func (c *client) GetStore(storeID uint64) (*metapb.Store, error) {
	req := &storeRequest{
		pbReq: &pdpb.GetStoreRequest{
			StoreId: proto.Uint64(storeID),
		},
		done: make(chan error),
	}
	c.workerMutex.RLock()
	c.worker.requests <- req
	c.workerMutex.RUnlock()
	err := <-req.done
	if err != nil {
		return nil, errors.Trace(err)
	}
	store := req.pbResp.GetStore()
	if store == nil {
		return nil, errors.New("[pd] store field in rpc response not set")
	}
	return store, nil
}

func (c *client) watchLeader(leaderPath string, revision int64) {
	defer c.wg.Done()
WATCH:
	for {
		log.Infof("[pd] start watch pd leader on path %v, revision %v", leaderPath, revision)
		rch := c.etcdClient.Watch(context.Background(), leaderPath, clientv3.WithRev(revision))
		select {
		case resp := <-rch:
			if resp.Canceled {
				log.Warn("[pd] leader watcher canceled")
				continue WATCH
			}
			leaderAddr, rev, err := getLeader(c.etcdClient, leaderPath)
			if err != nil {
				log.Warn(err)
				continue WATCH
			}
			log.Infof("[pd] found new pd-server leader addr: %v", leaderAddr)
			c.workerMutex.Lock()
			c.worker.stop(errors.New("[pd] leader change"))
			c.worker = newRPCWorker(leaderAddr, c.clusterID)
			c.workerMutex.Unlock()
			revision = rev
		case <-c.quit:
			return
		}
	}
}

func getLeader(etcdClient *clientv3.Client, path string) (string, int64, error) {
	kv := clientv3.NewKV(etcdClient)
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	resp, err := kv.Get(ctx, path)
	cancel()
	if err != nil {
		return "", 0, errors.Trace(err)
	}
	if len(resp.Kvs) != 1 {
		return "", 0, errors.Errorf("invalid getLeader resp: %v", resp)
	}

	var leader pdpb.Leader
	if err = proto.Unmarshal(resp.Kvs[0].Value, &leader); err != nil {
		return "", 0, errors.Trace(err)
	}
	return leader.GetAddr(), resp.Header.Revision, nil
}
