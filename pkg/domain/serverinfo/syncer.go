// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package serverinfo

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/util"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/etcd"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/versioninfo"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// MinStartTSReporter is an interface for reporting the minimum start timestamp
// of all sessions on a server, it's related to GC.
type MinStartTSReporter interface {
	ReportMinStartTS(store tidbkv.Storage, session *concurrency.Session)
}

// Syncer is used to sync server information.
type Syncer struct {
	etcdCli         *clientv3.Client
	reporter        MinStartTSReporter
	info            atomic.Pointer[ServerInfo]
	serverInfoPath  string
	session         *concurrency.Session
	topologySession *concurrency.Session
}

// NewSyncer creates a new Syncer instance.
func NewSyncer(
	uuid string,
	serverIDGetter func() uint64,
	etcdCli *clientv3.Client,
	reporter MinStartTSReporter,
) *Syncer {
	return newSyncer(uuid, serverIDGetter, etcdCli, reporter, "")
}

// NewCrossKSSyncer creates a new Syncer instance for cross keyspace scenarios.
func NewCrossKSSyncer(
	uuid string,
	serverIDGetter func() uint64,
	etcdCli *clientv3.Client,
	reporter MinStartTSReporter,
	targetKS string,
) *Syncer {
	return newSyncer(uuid, serverIDGetter, etcdCli, reporter, targetKS)
}

func newSyncer(
	uuid string,
	serverIDGetter func() uint64,
	etcdCli *clientv3.Client,
	reporter MinStartTSReporter,
	assumedKS string,
) *Syncer {
	is := &Syncer{
		etcdCli:        etcdCli,
		reporter:       reporter,
		serverInfoPath: fmt.Sprintf("%s/%s", ServerInformationPath, uuid),
	}
	is.info.Store(getServerInfo(uuid, serverIDGetter, assumedKS))
	return is
}

// NewSessionAndStoreServerInfo creates a new etcd session and stores server info to etcd.
func (s *Syncer) NewSessionAndStoreServerInfo(ctx context.Context) error {
	if s.etcdCli == nil {
		return nil
	}
	logPrefix := fmt.Sprintf("[Info-syncer] %s", s.serverInfoPath)
	session, err := tidbutil.NewSession(ctx, logPrefix, s.etcdCli, tidbutil.NewSessionDefaultRetryCnt, util.SessionTTL)
	if err != nil {
		return err
	}
	s.session = session
	return s.StoreServerInfo(ctx)
}

// StoreServerInfo stores self server static information to etcd.
func (s *Syncer) StoreServerInfo(ctx context.Context) error {
	if s.etcdCli == nil {
		return nil
	}
	info := s.info.Load()
	infoBuf, err := info.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	str := string(hack.String(infoBuf))
	err = util.PutKVToEtcd(ctx, s.etcdCli, KeyOpDefaultRetryCnt, s.serverInfoPath, str, clientv3.WithLease(s.session.Lease()))
	return err
}

// GetLocalServerInfo returns self server information.
func (s *Syncer) GetLocalServerInfo() *ServerInfo {
	return s.info.Load()
}

// GetServerInfoByID gets server information by ID.
func (s *Syncer) GetServerInfoByID(ctx context.Context, id string) (*ServerInfo, error) {
	localInfo := s.info.Load()
	if s.etcdCli == nil || id == localInfo.ID {
		return localInfo, nil
	}
	key := fmt.Sprintf("%s/%s", ServerInformationPath, id)
	infoMap, err := getInfo(ctx, s.etcdCli, key, KeyOpDefaultRetryCnt, KeyOpDefaultTimeout)
	if err != nil {
		return nil, err
	}
	info, ok := infoMap[id]
	if !ok {
		return nil, errors.Errorf("[info-syncer] get %s failed", key)
	}
	return info, nil
}

// UpdateServerLabel updates the labels of the local server information in etcd.
func (s *Syncer) UpdateServerLabel(ctx context.Context, labels map[string]string) error {
	// when etcdCli is nil, the server infos are generated from the latest config, no need to update.
	if s.etcdCli == nil {
		return nil
	}
	dynamicInfo := s.cloneDynamicServerInfo()
	changed := false
	for k, v := range labels {
		if dynamicInfo.Labels[k] != v {
			changed = true
			dynamicInfo.Labels[k] = v
		}
	}
	if !changed {
		return nil
	}
	info := s.GetLocalServerInfo().Clone()
	info.DynamicInfo = *dynamicInfo
	infoBuf, err := info.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	str := string(hack.String(infoBuf))
	err = util.PutKVToEtcd(ctx, s.etcdCli, KeyOpDefaultRetryCnt, s.serverInfoPath, str, clientv3.WithLease(s.session.Lease()))
	if err != nil {
		return err
	}
	// update the dynamic info in the global info syncer after put etcd success.
	s.setDynamicServerInfo(dynamicInfo)
	return nil
}

// cloneDynamicServerInfo returns a clone of the dynamic server info.
func (s *Syncer) cloneDynamicServerInfo() *DynamicInfo {
	return s.info.Load().DynamicInfo.Clone()
}

// setDynamicServerInfo updates the dynamic server info.
func (s *Syncer) setDynamicServerInfo(ds *DynamicInfo) {
	staticInfo := s.info.Load()
	newInfo := &ServerInfo{
		StaticInfo:  staticInfo.StaticInfo,
		DynamicInfo: *ds,
	}
	s.info.Store(newInfo)
}

// GetAllServerInfo returns all server information from etcd.
func (s *Syncer) GetAllServerInfo(ctx context.Context) (map[string]*ServerInfo, error) {
	if val, _err_ := failpoint.Eval(_curpkg_("mockGetAllServerInfo")); _err_ == nil {
		res := make(map[string]*ServerInfo)
		err := json.Unmarshal([]byte(val.(string)), &res)
		return res, err
	}
	allInfo := make(map[string]*ServerInfo)
	if s.etcdCli == nil {
		info := s.info.Load()
		allInfo[info.ID] = getServerInfo(info.ID, info.ServerIDGetter, "")
		return allInfo, nil
	}
	allInfo, err := getInfo(ctx, s.etcdCli, ServerInformationPath, KeyOpDefaultRetryCnt, KeyOpDefaultTimeout, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	return allInfo, nil
}

// Done returns a channel that closes when the info syncer is no longer being refreshed.
func (s *Syncer) Done() <-chan struct{} {
	if s.etcdCli == nil {
		return make(chan struct{}, 1)
	}
	return s.session.Done()
}

// Restart the info syncer with new session leaseID and store server info to etcd again.
func (s *Syncer) Restart(ctx context.Context) error {
	return s.NewSessionAndStoreServerInfo(ctx)
}

// RemoveServerInfo remove self server static information from etcd.
func (s *Syncer) RemoveServerInfo() {
	if s.etcdCli == nil {
		return
	}
	err := etcd.DeleteKeyFromEtcd(s.serverInfoPath, s.etcdCli, KeyOpDefaultRetryCnt, KeyOpDefaultTimeout)
	if err != nil {
		logutil.BgLogger().Error("remove server info failed", zap.Error(err))
	}
}

// ServerInfoSyncLoop syncs the server information periodically.
func (s *Syncer) ServerInfoSyncLoop(store tidbkv.Storage, exitCh chan struct{}) {
	defer func() {
		logutil.BgLogger().Info("server info sync loop exited.")
	}()

	defer tidbutil.Recover(metrics.LabelDomain, "ServerInfoSyncLoop", nil, false)

	ticker := time.NewTicker(minTSReportInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.reporter.ReportMinStartTS(store, s.session)
		case <-s.Done():
			logutil.BgLogger().Info("server info syncer need to restart")
			if err := s.Restart(context.Background()); err != nil {
				logutil.BgLogger().Error("server info syncer restart failed", zap.Error(err))
			} else {
				logutil.BgLogger().Info("server info syncer restarted")
			}
		case <-exitCh:
			return
		}
	}
}

// NewTopologySessionAndStoreServerInfo creates a new etcd session and stores server info to etcd.
func (s *Syncer) NewTopologySessionAndStoreServerInfo(ctx context.Context) error {
	if s.etcdCli == nil {
		return nil
	}
	info := s.GetLocalServerInfo()
	logPrefix := fmt.Sprintf("[topology-syncer] %s/%s", TopologyInformationPath, net.JoinHostPort(info.IP, strconv.Itoa(int(info.Port))))
	session, err := tidbutil.NewSession(ctx, logPrefix, s.etcdCli, tidbutil.NewSessionDefaultRetryCnt, TopologySessionTTL)
	if err != nil {
		return err
	}

	s.topologySession = session
	return s.StoreTopologyInfo(ctx)
}

// StoreTopologyInfo stores the topology of tidb to etcd.
func (s *Syncer) StoreTopologyInfo(ctx context.Context) error {
	if s.etcdCli == nil {
		return nil
	}
	info := s.info.Load()
	topologyInfo := info.ToTopologyInfo()
	infoBuf, err := json.Marshal(topologyInfo)
	if err != nil {
		return errors.Trace(err)
	}
	str := string(hack.String(infoBuf))
	key := fmt.Sprintf("%s/%s/info", TopologyInformationPath, net.JoinHostPort(info.IP, strconv.Itoa(int(info.Port))))
	// Note: no lease is required here.
	err = util.PutKVToEtcd(ctx, s.etcdCli, KeyOpDefaultRetryCnt, key, str)
	if err != nil {
		return err
	}
	// Initialize ttl.
	return s.updateTopologyAliveness(ctx)
}

// refreshTopology refreshes etcd topology with ttl stored in "/topology/tidb/ip:port/ttl".
func (s *Syncer) updateTopologyAliveness(ctx context.Context) error {
	if s.etcdCli == nil {
		return nil
	}
	info := s.GetLocalServerInfo()
	key := fmt.Sprintf("%s/%s/ttl", TopologyInformationPath, net.JoinHostPort(info.IP, strconv.Itoa(int(info.Port))))
	return util.PutKVToEtcd(ctx, s.etcdCli, KeyOpDefaultRetryCnt, key,
		fmt.Sprintf("%v", time.Now().UnixNano()),
		clientv3.WithLease(s.topologySession.Lease()))
}

// GetAllTiDBTopology gets all tidb topology
func (s *Syncer) GetAllTiDBTopology(ctx context.Context) ([]*TopologyInfo, error) {
	topos := make([]*TopologyInfo, 0)
	if s.etcdCli == nil {
		return topos, nil
	}
	response, err := s.etcdCli.Get(ctx, TopologyInformationPath, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	for _, kv := range response.Kvs {
		if !strings.HasSuffix(string(kv.Key), "/info") {
			continue
		}
		var topo *TopologyInfo
		err = json.Unmarshal(kv.Value, &topo)
		if err != nil {
			return nil, err
		}
		topos = append(topos, topo)
	}
	return topos, nil
}

// RemoveTopologyInfo remove self server topology information from etcd.
func (s *Syncer) RemoveTopologyInfo() {
	if s.etcdCli == nil {
		return
	}
	info := s.info.Load()
	prefix := fmt.Sprintf(
		"%s/%s",
		TopologyInformationPath,
		net.JoinHostPort(info.IP, strconv.Itoa(int(info.Port))),
	)
	err := util.DeleteKeysWithPrefixFromEtcd(prefix, s.etcdCli, KeyOpDefaultRetryCnt, KeyOpDefaultTimeout)
	if err != nil {
		logutil.BgLogger().Error("remove topology info failed", zap.Error(err))
	}
}

// TopologyDone returns a channel that closes when the topology syncer is no longer being refreshed.
func (s *Syncer) TopologyDone() <-chan struct{} {
	if s.etcdCli == nil {
		return make(chan struct{}, 1)
	}
	return s.topologySession.Done()
}

// RestartTopology restart the topology syncer with new session leaseID and store server info to etcd again.
func (s *Syncer) RestartTopology(ctx context.Context) error {
	return s.NewTopologySessionAndStoreServerInfo(ctx)
}

// TopologySyncLoop syncs the topology information periodically.
func (s *Syncer) TopologySyncLoop(exitCh chan struct{}) {
	defer tidbutil.Recover(metrics.LabelDomain, "TopologySyncLoop", nil, false)
	ticker := time.NewTicker(TopologyTimeToRefresh)
	defer func() {
		ticker.Stop()
		logutil.BgLogger().Info("topology sync loop exited.")
	}()

	for {
		select {
		case <-ticker.C:
			err := s.StoreTopologyInfo(context.Background())
			if err != nil {
				logutil.BgLogger().Warn("refresh topology in loop failed", zap.Error(err))
			}
		case <-s.TopologyDone():
			logutil.BgLogger().Info("server topology syncer need to restart")
			if err := s.RestartTopology(context.Background()); err != nil {
				logutil.BgLogger().Warn("server topology syncer restart failed", zap.Error(err))
			} else {
				logutil.BgLogger().Info("server topology syncer restarted")
			}
		case <-exitCh:
			return
		}
	}
}

// getInfo gets server information from etcd according to the key and opts.
func getInfo(ctx context.Context, etcdCli *clientv3.Client, key string, retryCnt int, timeout time.Duration, opts ...clientv3.OpOption) (map[string]*ServerInfo, error) {
	var err error
	var resp *clientv3.GetResponse
	allInfo := make(map[string]*ServerInfo)
	for range retryCnt {
		select {
		case <-ctx.Done():
			err = errors.Trace(ctx.Err())
			return nil, err
		default:
		}
		childCtx, cancel := context.WithTimeout(ctx, timeout)
		resp, err = etcdCli.Get(childCtx, key, opts...)
		cancel()
		if err != nil {
			logutil.BgLogger().Info("get key failed", zap.String("key", key), zap.Error(err))
			time.Sleep(200 * time.Millisecond)
			continue
		}
		for _, kv := range resp.Kvs {
			info := &ServerInfo{}
			err = info.Unmarshal(kv.Value)
			if err != nil {
				logutil.BgLogger().Info("get key failed", zap.String("key", string(kv.Key)), zap.ByteString("value", kv.Value),
					zap.Error(err))
				return nil, errors.Trace(err)
			}
			allInfo[info.ID] = info
		}
		return allInfo, nil
	}
	return nil, errors.Trace(err)
}

// getServerInfo gets self tidb server information.
func getServerInfo(id string, serverIDGetter func() uint64, assumedKS string) *ServerInfo {
	cfg := config.GetGlobalConfig()
	info := &ServerInfo{
		StaticInfo: StaticInfo{
			ID:              id,
			IP:              cfg.AdvertiseAddress,
			Port:            cfg.Port,
			StatusPort:      cfg.Status.StatusPort,
			Lease:           cfg.Lease,
			StartTimestamp:  time.Now().Unix(),
			Keyspace:        config.GetGlobalKeyspaceName(),
			AssumedKeyspace: assumedKS,
			ServerIDGetter:  serverIDGetter,
		},
		DynamicInfo: DynamicInfo{
			Labels: maps.Clone(cfg.Labels),
		},
	}
	info.Version = mysql.ServerVersion
	info.GitHash = versioninfo.TiDBGitHash

	metrics.ServerInfo.WithLabelValues(mysql.TiDBReleaseVersion, info.GitHash).Set(float64(info.StartTimestamp))

	if val, _err_ := failpoint.Eval(_curpkg_("mockServerInfo")); _err_ == nil {
		if val.(bool) {
			info.StartTimestamp = 1282967700
			info.Labels = map[string]string{
				"foo": "bar",
			}
		}
	}

	return info
}
