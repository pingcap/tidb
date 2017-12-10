// Copyright 2016 PingCAP, Inc.
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

package server

import (
	"fmt"
	"path"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/coreos/etcd/clientv3"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
)

const (
	backgroundJobInterval = time.Minute
)

// Error instances
var (
	ErrNotBootstrapped = errors.New("TiKV cluster is not bootstrapped, please start TiKV first")
)

// RaftCluster is used for cluster config management.
// Raft cluster key format:
// cluster 1 -> /1/raft, value is metapb.Cluster
// cluster 2 -> /2/raft
// For cluster 1
// store 1 -> /1/raft/s/1, value is metapb.Store
// region 1 -> /1/raft/r/1, value is metapb.Region
type RaftCluster struct {
	sync.RWMutex

	s *Server

	running bool

	clusterID   uint64
	clusterRoot string

	// cached cluster info
	cachedCluster *clusterInfo

	coordinator *coordinator

	wg   sync.WaitGroup
	quit chan struct{}

	status *ClusterStatus
}

// ClusterStatus saves some state information
type ClusterStatus struct {
	RaftBootstrapTime time.Time `json:"raft_bootstrap_time,omitempty"`
}

func newRaftCluster(s *Server, clusterID uint64) *RaftCluster {
	return &RaftCluster{
		s:           s,
		running:     false,
		clusterID:   clusterID,
		clusterRoot: s.getClusterRootPath(),
	}
}

func (c *RaftCluster) loadClusterStatus() error {
	data, err := c.s.kv.Load((c.s.kv.ClusterStatePath("raft_bootstrap_time")))
	if err != nil {
		return errors.Trace(err)
	}
	if len(data) == 0 {
		return nil
	}
	t, err := parseTimestamp([]byte(data))
	if err != nil {
		return errors.Trace(err)
	}
	c.status = &ClusterStatus{
		RaftBootstrapTime: t,
	}
	return nil
}

func (c *RaftCluster) start() error {
	c.Lock()
	defer c.Unlock()

	if c.running {
		log.Warn("raft cluster has already been started")
		return nil
	}

	cluster, err := loadClusterInfo(c.s.idAlloc, c.s.kv)
	if err != nil {
		return errors.Trace(err)
	}
	if cluster == nil {
		return nil
	}
	c.cachedCluster = cluster
	c.coordinator = newCoordinator(c.cachedCluster, c.s.scheduleOpt, c.s.hbStreams, c.s.classifier)
	c.quit = make(chan struct{})

	c.wg.Add(2)
	go c.runCoordinator()
	go c.runBackgroundJobs(backgroundJobInterval)

	c.running = true

	return nil
}

func (c *RaftCluster) runCoordinator() {
	c.coordinator.run()
	c.wg.Done()
}

func (c *RaftCluster) stop() {
	c.Lock()
	defer c.Unlock()

	if !c.running {
		return
	}

	c.running = false

	close(c.quit)
	c.coordinator.stop()
	c.wg.Wait()
}

func (c *RaftCluster) isRunning() bool {
	c.RLock()
	defer c.RUnlock()

	return c.running
}

// GetConfig gets the config information.
func (s *Server) GetConfig() *Config {
	cfg := s.cfg.clone()
	cfg.Schedule = *s.scheduleOpt.load()
	cfg.Replication = *s.scheduleOpt.rep.load()
	return cfg
}

// GetScheduleConfig gets the balance config information.
func (s *Server) GetScheduleConfig() *ScheduleConfig {
	cfg := &ScheduleConfig{}
	*cfg = *s.scheduleOpt.load()
	return cfg
}

// SetScheduleConfig sets the balance config information.
func (s *Server) SetScheduleConfig(cfg ScheduleConfig) {
	s.scheduleOpt.store(&cfg)
	s.scheduleOpt.persist(s.kv)
	s.cfg.Schedule = cfg
	log.Infof("schedule config is updated: %+v, old: %+v", cfg, s.cfg.Schedule)
}

// GetReplicationConfig get the replication config
func (s *Server) GetReplicationConfig() *ReplicationConfig {
	cfg := &ReplicationConfig{}
	*cfg = *s.scheduleOpt.rep.load()
	return cfg
}

// SetReplicationConfig sets the replication config
func (s *Server) SetReplicationConfig(cfg ReplicationConfig) {
	s.scheduleOpt.rep.store(&cfg)
	s.scheduleOpt.persist(s.kv)
	s.cfg.Replication = cfg
	log.Infof("replication is updated: %+v, old: %+v", cfg, s.cfg.Replication)
}

func (s *Server) getClusterRootPath() string {
	return path.Join(s.rootPath, "raft")
}

// GetRaftCluster gets raft cluster.
// If cluster has not been bootstrapped, return nil.
func (s *Server) GetRaftCluster() *RaftCluster {
	if s.isClosed() || !s.cluster.isRunning() {
		return nil
	}
	return s.cluster
}

// GetCluster gets cluster
func (s *Server) GetCluster() *metapb.Cluster {
	return &metapb.Cluster{
		Id:           s.clusterID,
		MaxPeerCount: uint32(s.cfg.Replication.MaxReplicas),
	}
}

// GetClusterStatus gets cluster status
func (s *Server) GetClusterStatus() (*ClusterStatus, error) {
	s.cluster.Lock()
	defer s.cluster.Unlock()
	err := s.cluster.loadClusterStatus()
	if err != nil {
		return nil, errors.Trace(err)
	}
	clone := &ClusterStatus{}
	*clone = *s.cluster.status
	return clone, nil
}

func (s *Server) createRaftCluster() error {
	if s.cluster.isRunning() {
		return nil
	}

	return s.cluster.start()
}

func (s *Server) stopRaftCluster() {
	s.cluster.stop()
}

func makeStoreKey(clusterRootPath string, storeID uint64) string {
	return path.Join(clusterRootPath, "s", fmt.Sprintf("%020d", storeID))
}

func makeRegionKey(clusterRootPath string, regionID uint64) string {
	return path.Join(clusterRootPath, "r", fmt.Sprintf("%020d", regionID))
}

func makeRaftClusterStatusPrefix(clusterRootPath string) string {
	return path.Join(clusterRootPath, "status")
}

func makeBootstrapTimeKey(clusterRootPath string) string {
	return path.Join(makeRaftClusterStatusPrefix(clusterRootPath), "raft_bootstrap_time")
}

func checkBootstrapRequest(clusterID uint64, req *pdpb.BootstrapRequest) error {
	// TODO: do more check for request fields validation.

	storeMeta := req.GetStore()
	if storeMeta == nil {
		return errors.Errorf("missing store meta for bootstrap %d", clusterID)
	} else if storeMeta.GetId() == 0 {
		return errors.New("invalid zero store id")
	}

	regionMeta := req.GetRegion()
	if regionMeta == nil {
		return errors.Errorf("missing region meta for bootstrap %d", clusterID)
	} else if len(regionMeta.GetStartKey()) > 0 || len(regionMeta.GetEndKey()) > 0 {
		// first region start/end key must be empty
		return errors.Errorf("invalid first region key range, must all be empty for bootstrap %d", clusterID)
	} else if regionMeta.GetId() == 0 {
		return errors.New("invalid zero region id")
	}

	peers := regionMeta.GetPeers()
	if len(peers) != 1 {
		return errors.Errorf("invalid first region peer count %d, must be 1 for bootstrap %d", len(peers), clusterID)
	}

	peer := peers[0]
	if peer.GetStoreId() != storeMeta.GetId() {
		return errors.Errorf("invalid peer store id %d != %d for bootstrap %d", peer.GetStoreId(), storeMeta.GetId(), clusterID)
	}
	if peer.GetId() == 0 {
		return errors.New("invalid zero peer id")
	}

	return nil
}

func (s *Server) bootstrapCluster(req *pdpb.BootstrapRequest) (*pdpb.BootstrapResponse, error) {
	clusterID := s.clusterID

	log.Infof("try to bootstrap raft cluster %d with %v", clusterID, req)

	if err := checkBootstrapRequest(clusterID, req); err != nil {
		return nil, errors.Trace(err)
	}

	clusterMeta := metapb.Cluster{
		Id:           clusterID,
		MaxPeerCount: uint32(s.cfg.Replication.MaxReplicas),
	}

	// Set cluster meta
	clusterValue, err := clusterMeta.Marshal()
	if err != nil {
		return nil, errors.Trace(err)
	}
	clusterRootPath := s.getClusterRootPath()

	var ops []clientv3.Op
	ops = append(ops, clientv3.OpPut(clusterRootPath, string(clusterValue)))

	// Set bootstrap time
	bootstrapKey := makeBootstrapTimeKey(clusterRootPath)
	nano := time.Now().UnixNano()

	timeData := uint64ToBytes(uint64(nano))
	ops = append(ops, clientv3.OpPut(bootstrapKey, string(timeData)))

	// Set store meta
	storeMeta := req.GetStore()
	storePath := makeStoreKey(clusterRootPath, storeMeta.GetId())
	storeValue, err := storeMeta.Marshal()
	if err != nil {
		return nil, errors.Trace(err)
	}
	ops = append(ops, clientv3.OpPut(storePath, string(storeValue)))

	regionValue, err := req.GetRegion().Marshal()
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Set region meta with region id.
	regionPath := makeRegionKey(clusterRootPath, req.GetRegion().GetId())
	ops = append(ops, clientv3.OpPut(regionPath, string(regionValue)))

	// TODO: we must figure out a better way to handle bootstrap failed, maybe intervene manually.
	bootstrapCmp := clientv3.Compare(clientv3.CreateRevision(clusterRootPath), "=", 0)
	resp, err := s.txn().If(bootstrapCmp).Then(ops...).Commit()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !resp.Succeeded {
		log.Warnf("cluster %d already bootstrapped", clusterID)
		return nil, errors.Errorf("cluster %d already bootstrapped", clusterID)
	}

	log.Infof("bootstrap cluster %d ok", clusterID)

	if err := s.cluster.start(); err != nil {
		return nil, errors.Trace(err)
	}

	return &pdpb.BootstrapResponse{}, nil
}

// GetRegionByKey gets region and leader peer by region key from cluster.
func (c *RaftCluster) GetRegionByKey(regionKey []byte) (*metapb.Region, *metapb.Peer) {
	region := c.cachedCluster.searchRegion(regionKey)
	if region == nil {
		return nil, nil
	}
	return region.Region, region.Leader
}

// GetRegionInfoByKey gets regionInfo by region key from cluster.
func (c *RaftCluster) GetRegionInfoByKey(regionKey []byte) *core.RegionInfo {
	return c.cachedCluster.searchRegion(regionKey)
}

// GetRegionByID gets region and leader peer by regionID from cluster.
func (c *RaftCluster) GetRegionByID(regionID uint64) (*metapb.Region, *metapb.Peer) {
	region := c.cachedCluster.GetRegion(regionID)
	if region == nil {
		return nil, nil
	}
	return region.Region, region.Leader
}

// GetRegionInfoByID gets regionInfo by regionID from cluster.
func (c *RaftCluster) GetRegionInfoByID(regionID uint64) *core.RegionInfo {
	return c.cachedCluster.GetRegion(regionID)
}

// GetRegions gets regions from cluster.
func (c *RaftCluster) GetRegions() []*metapb.Region {
	return c.cachedCluster.getMetaRegions()
}

// GetStores gets stores from cluster.
func (c *RaftCluster) GetStores() []*metapb.Store {
	return c.cachedCluster.getMetaStores()
}

// GetStore gets store from cluster.
func (c *RaftCluster) GetStore(storeID uint64) (*core.StoreInfo, error) {
	if storeID == 0 {
		return nil, errors.New("invalid zero store id")
	}

	store := c.cachedCluster.GetStore(storeID)
	if store == nil {
		return nil, errors.Errorf("invalid store ID %d, not found", storeID)
	}
	return store, nil
}

// UpdateStoreLabels updates a store's location labels.
func (c *RaftCluster) UpdateStoreLabels(storeID uint64, labels []*metapb.StoreLabel) error {
	store := c.cachedCluster.GetStore(storeID)
	if store == nil {
		return errors.Errorf("invalid store ID %d, not found", storeID)
	}
	storeMeta := store.Store
	storeMeta.Labels = labels
	// putStore will perform label merge.
	err := c.putStore(storeMeta)
	return errors.Trace(err)
}

func (c *RaftCluster) putStore(store *metapb.Store) error {
	c.Lock()
	defer c.Unlock()

	if store.GetId() == 0 {
		return errors.Errorf("invalid put store %v", store)
	}

	cluster := c.cachedCluster

	// Store address can not be the same as other stores.
	for _, s := range cluster.GetStores() {
		// It's OK to start a new store on the same address if the old store has been removed.
		if s.IsTombstone() {
			continue
		}
		if s.GetId() != store.GetId() && s.GetAddress() == store.GetAddress() {
			return errors.Errorf("duplicated store address: %v, already registered by %v", store, s.Store)
		}
	}

	s := cluster.GetStore(store.GetId())
	if s == nil {
		// Add a new store.
		s = core.NewStoreInfo(store)
	} else {
		// Update an existed store.
		s.Address = store.Address
		s.MergeLabels(store.Labels)
	}

	// Check location labels.
	for _, k := range c.s.cfg.Replication.LocationLabels {
		if v := s.GetLabelValue(k); len(v) == 0 {
			log.Warnf("missing location label %q in store %v", k, s)
		}
	}

	return cluster.putStore(s)
}

// RemoveStore marks a store as offline in cluster.
// State transition: Up -> Offline.
func (c *RaftCluster) RemoveStore(storeID uint64) error {
	c.Lock()
	defer c.Unlock()

	cluster := c.cachedCluster

	store := cluster.GetStore(storeID)
	if store == nil {
		return errors.Trace(core.ErrStoreNotFound(storeID))
	}

	// Remove an offline store should be OK, nothing to do.
	if store.IsOffline() {
		return nil
	}

	if store.IsTombstone() {
		return errors.New("store has been removed")
	}

	store.State = metapb.StoreState_Offline
	log.Warnf("[store %d] store %s has been Offline", store.GetId(), store.GetAddress())
	return cluster.putStore(store)
}

// BuryStore marks a store as tombstone in cluster.
// State transition:
// Case 1: Up -> Tombstone (if force is true);
// Case 2: Offline -> Tombstone.
func (c *RaftCluster) BuryStore(storeID uint64, force bool) error {
	c.Lock()
	defer c.Unlock()

	cluster := c.cachedCluster

	store := cluster.GetStore(storeID)
	if store == nil {
		return errors.Trace(core.ErrStoreNotFound(storeID))
	}

	// Bury a tombstone store should be OK, nothing to do.
	if store.IsTombstone() {
		return nil
	}

	if store.IsUp() {
		if !force {
			return errors.New("store is still up, please remove store gracefully")
		}
		log.Warnf("forcedly bury store %v", store)
	}

	store.State = metapb.StoreState_Tombstone
	log.Warnf("[store %d] store %s has been Tombstone", store.GetId(), store.GetAddress())
	return cluster.putStore(store)
}

// SetStoreState sets up a store's state.
func (c *RaftCluster) SetStoreState(storeID uint64, state metapb.StoreState) error {
	c.Lock()
	defer c.Unlock()

	cluster := c.cachedCluster

	store := cluster.GetStore(storeID)
	if store == nil {
		return errors.Trace(core.ErrStoreNotFound(storeID))
	}

	store.State = state
	log.Warnf("[store %d] set state to %v", storeID, state.String())
	return cluster.putStore(store)
}

// SetStoreWeight sets up a store's leader/region balance weight.
func (c *RaftCluster) SetStoreWeight(storeID uint64, leader, region float64) error {
	c.Lock()
	defer c.Unlock()

	store := c.cachedCluster.GetStore(storeID)
	if store == nil {
		return errors.Trace(core.ErrStoreNotFound(storeID))
	}

	if err := c.s.kv.SaveStoreWeight(storeID, leader, region); err != nil {
		return errors.Trace(err)
	}

	store.LeaderWeight, store.RegionWeight = leader, region
	return c.cachedCluster.putStore(store)
}

func (c *RaftCluster) checkStores() {
	cluster := c.cachedCluster
	for _, store := range cluster.getMetaStores() {
		if store.GetState() != metapb.StoreState_Offline {
			continue
		}
		if c.storeIsEmpty(store.GetId()) {
			err := c.BuryStore(store.GetId(), false)
			if err != nil {
				log.Errorf("bury store %v failed: %v", store, err)
			} else {
				log.Infof("buried store %v", store)
			}
		}
	}
}

func (c *RaftCluster) storeIsEmpty(storeID uint64) bool {
	cluster := c.cachedCluster
	if cluster.getStoreRegionCount(storeID) > 0 {
		return false
	}
	// If pd-server is started recently, or becomes leader recently, the check may
	// happen before any heartbeat from tikv. So we need to check region metas to
	// verify no region's peer is on the store.
	regions := cluster.getMetaRegions()
	for _, region := range regions {
		for _, p := range region.GetPeers() {
			if p.GetStoreId() == storeID {
				return false
			}
		}
	}
	return true
}

func (c *RaftCluster) collectMetrics() {
	cluster := c.cachedCluster
	statsMap := newStoreStatisticsMap(c.coordinator.opt, c.GetNamespaceClassifier())
	for _, s := range cluster.GetStores() {
		statsMap.Observe(s)
	}
	statsMap.Collect()

	c.coordinator.collectSchedulerMetrics()
	c.coordinator.collectHotSpotMetrics()
}

func (c *RaftCluster) runBackgroundJobs(interval time.Duration) {
	defer c.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.quit:
			return
		case <-ticker.C:
			c.checkStores()
			c.collectMetrics()
		}
	}
}

// GetConfig gets config from cluster.
func (c *RaftCluster) GetConfig() *metapb.Cluster {
	return c.cachedCluster.getMeta()
}

func (c *RaftCluster) putConfig(meta *metapb.Cluster) error {
	if meta.GetId() != c.clusterID {
		return errors.Errorf("invalid cluster %v, mismatch cluster id %d", meta, c.clusterID)
	}
	return c.cachedCluster.putMeta(meta)
}

// GetNamespaceClassifier returns current namespace classifier.
func (c *RaftCluster) GetNamespaceClassifier() namespace.Classifier {
	return c.s.classifier
}
