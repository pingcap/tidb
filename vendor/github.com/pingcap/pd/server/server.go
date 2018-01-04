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
	"crypto/tls"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/etcd/pkg/transport"
	"github.com/coreos/etcd/pkg/types"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/pkg/etcdutil"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

const (
	etcdTimeout = time.Second * 3
	// pdRootPath for all pd servers.
	pdRootPath      = "/pd"
	pdAPIPrefix     = "/pd/"
	pdClusterIDPath = "/pd/cluster_id"
)

// Server is the pd server.
type Server struct {
	// Server state.
	isServing int64
	isLeader  int64

	// Configs and initial fields.
	cfg         *Config
	etcdCfg     *embed.Config
	scheduleOpt *scheduleOption
	handler     *Handler

	wg sync.WaitGroup

	// Etcd and cluster informations.
	etcd        *embed.Etcd
	client      *clientv3.Client
	id          uint64 // etcd server id.
	clusterID   uint64 // pd cluster id.
	rootPath    string
	leaderValue string // leader value saved in etcd leader key.  Every write will use this to check leader validation.

	// Server services.
	// for id allocator, we can use one allocator for
	// store, region and peer, because we just need
	// a unique ID.
	idAlloc *idAllocator
	// for kv operation.
	kv *core.KV
	// for namespace.
	classifier namespace.Classifier
	// for raft cluster
	cluster *RaftCluster
	// For tso, set after pd becomes leader.
	ts            atomic.Value
	lastSavedTime time.Time
	// For resign notify.
	resignCh chan struct{}
	// For async region heartbeat.
	hbStreams *heartbeatStreams
}

// CreateServer creates the UNINITIALIZED pd server with given configuration.
func CreateServer(cfg *Config, apiRegister func(*Server) http.Handler) (*Server, error) {
	log.Infof("PD config - %v", cfg)
	rand.Seed(time.Now().UnixNano())

	s := &Server{
		cfg:         cfg,
		scheduleOpt: newScheduleOption(cfg),
		resignCh:    make(chan struct{}),
	}
	s.handler = newHandler(s)

	// Adjust etcd config.
	etcdCfg, err := s.cfg.genEmbedEtcdConfig()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if apiRegister != nil {
		etcdCfg.UserHandlers = map[string]http.Handler{
			pdAPIPrefix: apiRegister(s),
		}
	}
	etcdCfg.ServiceRegister = func(gs *grpc.Server) { pdpb.RegisterPDServer(gs, s) }
	s.etcdCfg = etcdCfg

	return s, nil
}

func (s *Server) startEtcd() error {
	log.Info("start embed etcd")
	etcd, err := embed.StartEtcd(s.etcdCfg)
	if err != nil {
		return errors.Trace(err)
	}

	// Check cluster ID
	urlmap, err := types.NewURLsMap(s.cfg.InitialCluster)
	if err != nil {
		return errors.Trace(err)
	}
	tlsConfig, err := s.GetTLSConfig()
	if err != nil {
		return errors.Trace(err)
	}
	if err = etcdutil.CheckClusterID(etcd.Server.Cluster().ID(), urlmap, tlsConfig); err != nil {
		return errors.Trace(err)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	select {
	// Wait etcd until it is ready to use
	case <-etcd.Server.ReadyNotify():
	case sig := <-sc:
		return errors.Errorf("receive signal %v when waiting embed etcd to be ready", sig)
	}

	endpoints := []string{s.etcdCfg.ACUrls[0].String()}
	log.Infof("create etcd v3 client with endpoints %v", endpoints)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: etcdTimeout,
		TLS:         tlsConfig,
	})
	if err != nil {
		return errors.Trace(err)
	}

	etcdServerID := uint64(etcd.Server.ID())

	// update advertise peer urls.
	etcdMembers, err := etcdutil.ListEtcdMembers(client)
	if err != nil {
		return errors.Trace(err)
	}
	for _, m := range etcdMembers.Members {
		if etcdServerID == m.ID {
			etcdPeerURLs := strings.Join(m.PeerURLs, ",")
			if s.cfg.AdvertisePeerUrls != etcdPeerURLs {
				log.Infof("update advertise peer urls from %s to %s", s.cfg.AdvertisePeerUrls, etcdPeerURLs)
				s.cfg.AdvertisePeerUrls = etcdPeerURLs
			}
		}
	}

	s.etcd = etcd
	s.client = client
	s.id = etcdServerID
	return nil
}

func (s *Server) startServer() error {
	var err error
	if err = s.initClusterID(); err != nil {
		return errors.Trace(err)
	}
	log.Infof("init cluster id %v", s.clusterID)

	s.rootPath = path.Join(pdRootPath, strconv.FormatUint(s.clusterID, 10))
	s.leaderValue = s.marshalLeader()

	s.idAlloc = &idAllocator{s: s}
	kvBase := newEtcdKVBase(s)
	s.kv = core.NewKV(kvBase)
	s.cluster = newRaftCluster(s, s.clusterID)
	s.hbStreams = newHeartbeatStreams(s.clusterID)
	if s.classifier, err = namespace.CreateClassifier(s.cfg.NamespaceClassifier, s.kv, s.idAlloc); err != nil {
		return errors.Trace(err)
	}

	// Server has started.
	atomic.StoreInt64(&s.isServing, 1)
	return nil
}

func (s *Server) initClusterID() error {
	// Get any cluster key to parse the cluster ID.
	resp, err := kvGet(s.client, pdRootPath, clientv3.WithFirstCreate()...)
	if err != nil {
		return errors.Trace(err)
	}

	// If no key exist, generate a random cluster ID.
	if len(resp.Kvs) == 0 {
		s.clusterID, err = initOrGetClusterID(s.client, pdClusterIDPath)
		return errors.Trace(err)
	}

	key := string(resp.Kvs[0].Key)

	// If the key is "pdClusterIDPath", parse the cluster ID from it.
	if key == pdClusterIDPath {
		s.clusterID, err = bytesToUint64(resp.Kvs[0].Value)
		return errors.Trace(err)
	}

	// Parse the cluster ID from any other keys for compatibility.
	elems := strings.Split(key, "/")
	if len(elems) < 3 {
		return errors.Errorf("invalid cluster key %v", key)
	}
	s.clusterID, err = strconv.ParseUint(elems[2], 10, 64)

	log.Infof("init and load cluster id: %d", s.clusterID)
	return errors.Trace(err)
}

// Close closes the server.
func (s *Server) Close() {
	if !atomic.CompareAndSwapInt64(&s.isServing, 1, 0) {
		// server is already closed
		return
	}

	log.Info("closing server")

	s.enableLeader(false)

	if s.client != nil {
		s.client.Close()
	}

	if s.etcd != nil {
		s.etcd.Close()
	}

	if s.hbStreams != nil {
		s.hbStreams.Close()
	}

	s.wg.Wait()

	log.Info("close server")
}

// isClosed checks whether server is closed or not.
func (s *Server) isClosed() bool {
	return atomic.LoadInt64(&s.isServing) == 0
}

var timeMonitorOnce sync.Once

// Run runs the pd server.
func (s *Server) Run() error {
	timeMonitorOnce.Do(func() {
		go StartMonitor(time.Now, func() {
			log.Errorf("system time jumps backward")
			timeJumpBackCounter.Inc()
		})
	})

	if err := s.startEtcd(); err != nil {
		return errors.Trace(err)
	}

	if err := s.startServer(); err != nil {
		return errors.Trace(err)
	}

	s.wg.Add(1)
	go s.leaderLoop()
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
		MaxPeerCount: uint32(s.scheduleOpt.rep.GetMaxReplicas()),
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

func (s *Server) createRaftCluster() error {
	if s.cluster.isRunning() {
		return nil
	}

	return s.cluster.start()
}

func (s *Server) stopRaftCluster() {
	s.cluster.stop()
}

// GetAddr returns the server urls for clients.
func (s *Server) GetAddr() string {
	return s.cfg.AdvertiseClientUrls
}

// GetHandler returns the handler for API.
func (s *Server) GetHandler() *Handler {
	return s.handler
}

// GetEndpoints returns the etcd endpoints for outer use.
func (s *Server) GetEndpoints() []string {
	return s.client.Endpoints()
}

// GetClient returns builtin etcd client.
func (s *Server) GetClient() *clientv3.Client {
	return s.client
}

// ID returns the unique etcd ID for this server in etcd cluster.
func (s *Server) ID() uint64 {
	return s.id
}

// Name returns the unique etcd Name for this server in etcd cluster.
func (s *Server) Name() string {
	return s.cfg.Name
}

// ClusterID returns the cluster ID of this server.
func (s *Server) ClusterID() uint64 {
	return s.clusterID
}

// txn returns an etcd client transaction wrapper.
// The wrapper will set a request timeout to the context and log slow transactions.
func (s *Server) txn() clientv3.Txn {
	return newSlowLogTxn(s.client)
}

// leaderTxn returns txn() with a leader comparison to guarantee that
// the transaction can be executed only if the server is leader.
func (s *Server) leaderTxn(cs ...clientv3.Cmp) clientv3.Txn {
	return s.txn().If(append(cs, s.leaderCmp())...)
}

// GetConfig gets the config information.
func (s *Server) GetConfig() *Config {
	cfg := s.cfg.clone()
	cfg.Schedule = *s.scheduleOpt.load()
	cfg.Replication = *s.scheduleOpt.rep.load()
	namespaces := make(map[string]NamespaceConfig)
	for name, opt := range s.scheduleOpt.ns {
		namespaces[name] = *opt.load()
	}
	cfg.Namespace = namespaces
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
	old := s.scheduleOpt.load()
	s.scheduleOpt.store(&cfg)
	s.scheduleOpt.persist(s.kv)
	log.Infof("schedule config is updated: %+v, old: %+v", cfg, old)
}

// GetReplicationConfig get the replication config.
func (s *Server) GetReplicationConfig() *ReplicationConfig {
	cfg := &ReplicationConfig{}
	*cfg = *s.scheduleOpt.rep.load()
	return cfg
}

// SetReplicationConfig sets the replication config.
func (s *Server) SetReplicationConfig(cfg ReplicationConfig) {
	old := s.scheduleOpt.rep.load()
	s.scheduleOpt.rep.store(&cfg)
	s.scheduleOpt.persist(s.kv)
	log.Infof("replication config is updated: %+v, old: %+v", cfg, old)
}

// GetNamespaceConfig get the namespace config.
func (s *Server) GetNamespaceConfig(name string) *NamespaceConfig {
	if _, ok := s.scheduleOpt.ns[name]; !ok {
		return &NamespaceConfig{}
	}

	cfg := &NamespaceConfig{
		LeaderScheduleLimit:  s.scheduleOpt.GetLeaderScheduleLimit(name),
		RegionScheduleLimit:  s.scheduleOpt.GetRegionScheduleLimit(name),
		ReplicaScheduleLimit: s.scheduleOpt.GetReplicaScheduleLimit(name),
		MaxReplicas:          uint64(s.scheduleOpt.GetMaxReplicas(name)),
	}

	return cfg
}

// GetNamespaceConfigWithAdjust get the namespace config that replace zero value with global config value.
func (s *Server) GetNamespaceConfigWithAdjust(name string) *NamespaceConfig {
	cfg := s.GetNamespaceConfig(name)
	cfg.adjust(s.scheduleOpt)
	return cfg
}

// SetNamespaceConfig sets the namespace config.
func (s *Server) SetNamespaceConfig(name string, cfg NamespaceConfig) {
	if n, ok := s.scheduleOpt.ns[name]; ok {
		old := s.scheduleOpt.ns[name].load()
		n.store(&cfg)
		s.scheduleOpt.persist(s.kv)
		log.Infof("namespace:%v config is updated: %+v, old: %+v", name, cfg, old)
	} else {
		s.scheduleOpt.ns[name] = newNamespaceOption(&cfg)
		s.scheduleOpt.persist(s.kv)
		log.Infof("namespace:%v config is added: %+v", name, cfg)
	}
}

// DeleteNamespaceConfig deletes the namespace config.
func (s *Server) DeleteNamespaceConfig(name string) {
	if n, ok := s.scheduleOpt.ns[name]; ok {
		cfg := n.load()
		delete(s.scheduleOpt.ns, name)
		s.scheduleOpt.persist(s.kv)
		log.Infof("namespace:%v config is deleted: %+v", name, *cfg)
	}
}

// IsNamespaceExist returns whether the namespace exists.
func (s *Server) IsNamespaceExist(name string) bool {
	return s.classifier.IsNamespaceExist(name)
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

// GetCluster gets cluster.
func (s *Server) GetCluster() *metapb.Cluster {
	return &metapb.Cluster{
		Id:           s.clusterID,
		MaxPeerCount: uint32(s.scheduleOpt.rep.GetMaxReplicas()),
	}
}

// GetClusterStatus gets cluster status.
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

func (s *Server) getAllocIDPath() string {
	return path.Join(s.rootPath, "alloc_id")
}

// GetTLSConfig gets tls config.
func (s *Server) GetTLSConfig() (*tls.Config, error) {
	tlsInfo := transport.TLSInfo{
		CertFile:      s.cfg.Security.CertPath,
		KeyFile:       s.cfg.Security.KeyPath,
		TrustedCAFile: s.cfg.Security.CAPath,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return tlsConfig, nil
}
