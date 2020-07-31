// Copyright 2018 PingCAP, Inc.
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

package infosync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/pd/v4/server/schedule/placement"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv/oracle"
	util2 "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/pdapi"
	"github.com/pingcap/tidb/util/versioninfo"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.uber.org/zap"
)

const (
	// ServerInformationPath store server information such as IP, port and so on.
	ServerInformationPath = "/tidb/server/info"
	// ServerMinStartTSPath store the server min start timestamp.
	ServerMinStartTSPath = "/tidb/server/minstartts"
	// TiFlashTableSyncProgressPath store the tiflash table replica sync progress.
	TiFlashTableSyncProgressPath = "/tiflash/table/sync"
	// keyOpDefaultRetryCnt is the default retry count for etcd store.
	keyOpDefaultRetryCnt = 5
	// keyOpDefaultTimeout is the default time out for etcd store.
	keyOpDefaultTimeout = 1 * time.Second
	// InfoSessionTTL is the ETCD session's TTL in seconds.
	InfoSessionTTL = 10 * 60
	// ReportInterval is interval of infoSyncerKeeper reporting min startTS.
	ReportInterval = 30 * time.Second
	// TopologyInformationPath means etcd path for storing topology info.
	TopologyInformationPath = "/topology/tidb"
	// TopologySessionTTL is ttl for topology, ant it's the ETCD session's TTL in seconds.
	TopologySessionTTL = 45
	// TopologyTimeToRefresh means time to refresh etcd.
	TopologyTimeToRefresh = 30 * time.Second
	// TopologyPrometheus means address of prometheus.
	TopologyPrometheus = "/topology/prometheus"
	// TablePrometheusCacheExpiry is the expiry time for prometheus address cache.
	TablePrometheusCacheExpiry = 10 * time.Second
)

// ErrPrometheusAddrIsNotSet is the error that Prometheus address is not set in PD and etcd
var ErrPrometheusAddrIsNotSet = terror.ClassDomain.New(errno.ErrPrometheusAddrIsNotSet, errno.MySQLErrName[errno.ErrPrometheusAddrIsNotSet])

// InfoSyncer stores server info to etcd when the tidb-server starts and delete when tidb-server shuts down.
type InfoSyncer struct {
	etcdCli         *clientv3.Client
	info            *ServerInfo
	serverInfoPath  string
	minStartTS      uint64
	minStartTSPath  string
	manager         util2.SessionManager
	session         *concurrency.Session
	topologySession *concurrency.Session
	prometheusAddr  string
	modifyTime      time.Time
}

// ServerInfo is server static information.
// It will not be updated when tidb-server running. So please only put static information in ServerInfo struct.
type ServerInfo struct {
	ServerVersionInfo
	ID             string `json:"ddl_id"`
	IP             string `json:"ip"`
	Port           uint   `json:"listening_port"`
	StatusPort     uint   `json:"status_port"`
	Lease          string `json:"lease"`
	BinlogStatus   string `json:"binlog_status"`
	StartTimestamp int64  `json:"start_timestamp"`
}

// ServerVersionInfo is the server version and git_hash.
type ServerVersionInfo struct {
	Version string `json:"version"`
	GitHash string `json:"git_hash"`
}

// globalInfoSyncer stores the global infoSyncer.
// Use a global variable for simply the code, use the domain.infoSyncer will have circle import problem in some pkg.
// Use atomic.Value to avoid data race in the test.
var globalInfoSyncer atomic.Value

func getGlobalInfoSyncer() (*InfoSyncer, error) {
	v := globalInfoSyncer.Load()
	if v == nil {
		return nil, errors.New("infoSyncer is not initialized")
	}
	return v.(*InfoSyncer), nil
}

func setGlobalInfoSyncer(is *InfoSyncer) {
	globalInfoSyncer.Store(is)
}

// GlobalInfoSyncerInit return a new InfoSyncer. It is exported for testing.
func GlobalInfoSyncerInit(ctx context.Context, id string, etcdCli *clientv3.Client, skipRegisterToDashBoard bool) (*InfoSyncer, error) {
	is := &InfoSyncer{
		etcdCli:        etcdCli,
		info:           getServerInfo(id),
		serverInfoPath: fmt.Sprintf("%s/%s", ServerInformationPath, id),
		minStartTSPath: fmt.Sprintf("%s/%s", ServerMinStartTSPath, id),
	}
	err := is.init(ctx, skipRegisterToDashBoard)
	if err != nil {
		return nil, err
	}
	setGlobalInfoSyncer(is)
	return is, nil
}

// Init creates a new etcd session and stores server info to etcd.
func (is *InfoSyncer) init(ctx context.Context, skipRegisterToDashboard bool) error {
	err := is.newSessionAndStoreServerInfo(ctx, owner.NewSessionDefaultRetryCnt)
	if err != nil {
		return err
	}
	if skipRegisterToDashboard {
		return nil
	}
	return is.newTopologySessionAndStoreServerInfo(ctx, owner.NewSessionDefaultRetryCnt)
}

// SetSessionManager set the session manager for InfoSyncer.
func (is *InfoSyncer) SetSessionManager(manager util2.SessionManager) {
	is.manager = manager
}

// GetServerInfo gets self server static information.
func GetServerInfo() (*ServerInfo, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, err
	}
	return is.info, nil
}

// GetServerInfoByID gets specified server static information from etcd.
func GetServerInfoByID(ctx context.Context, id string) (*ServerInfo, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, err
	}
	return is.getServerInfoByID(ctx, id)
}

func (is *InfoSyncer) getServerInfoByID(ctx context.Context, id string) (*ServerInfo, error) {
	if is.etcdCli == nil || id == is.info.ID {
		return is.info, nil
	}
	key := fmt.Sprintf("%s/%s", ServerInformationPath, id)
	infoMap, err := getInfo(ctx, is.etcdCli, key, keyOpDefaultRetryCnt, keyOpDefaultTimeout)
	if err != nil {
		return nil, err
	}
	info, ok := infoMap[id]
	if !ok {
		return nil, errors.Errorf("[info-syncer] get %s failed", key)
	}
	return info, nil
}

// GetAllServerInfo gets all servers static information from etcd.
func GetAllServerInfo(ctx context.Context) (map[string]*ServerInfo, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, err
	}
	return is.getAllServerInfo(ctx)
}

// UpdateTiFlashTableSyncProgress is used to update the tiflash table replica sync progress.
func UpdateTiFlashTableSyncProgress(ctx context.Context, tid int64, progress float64) error {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return err
	}
	if is.etcdCli == nil {
		return nil
	}
	key := fmt.Sprintf("%s/%v", TiFlashTableSyncProgressPath, tid)
	return util.PutKVToEtcd(ctx, is.etcdCli, keyOpDefaultRetryCnt, key, strconv.FormatFloat(progress, 'f', 2, 64))
}

// DeleteTiFlashTableSyncProgress is used to delete the tiflash table replica sync progress.
func DeleteTiFlashTableSyncProgress(tid int64) error {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return err
	}
	if is.etcdCli == nil {
		return nil
	}
	key := fmt.Sprintf("%s/%v", TiFlashTableSyncProgressPath, tid)
	return util.DeleteKeyFromEtcd(key, is.etcdCli, keyOpDefaultRetryCnt, keyOpDefaultTimeout)
}

// GetTiFlashTableSyncProgress uses to get all the tiflash table replica sync progress.
func GetTiFlashTableSyncProgress(ctx context.Context) (map[int64]float64, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, err
	}
	progressMap := make(map[int64]float64)
	if is.etcdCli == nil {
		return progressMap, nil
	}
	for i := 0; i < keyOpDefaultRetryCnt; i++ {
		resp, err := is.etcdCli.Get(ctx, TiFlashTableSyncProgressPath+"/", clientv3.WithPrefix())
		if err != nil {
			logutil.BgLogger().Info("get tiflash table replica sync progress failed, continue checking.", zap.Error(err))
			continue
		}
		for _, kv := range resp.Kvs {
			tid, err := strconv.ParseInt(string(kv.Key[len(TiFlashTableSyncProgressPath)+1:]), 10, 64)
			if err != nil {
				logutil.BgLogger().Info("invalid tiflash table replica sync progress key.", zap.String("key", string(kv.Key)))
				continue
			}
			progress, err := strconv.ParseFloat(string(kv.Value), 64)
			if err != nil {
				logutil.BgLogger().Info("invalid tiflash table replica sync progress value.",
					zap.String("key", string(kv.Key)), zap.String("value", string(kv.Value)))
				continue
			}
			progressMap[tid] = progress
		}
		break
	}
	return progressMap, nil
}

func doRequest(ctx context.Context, addrs []string, route, method string, body io.Reader) error {
	var err error
	var req *http.Request
	for _, addr := range addrs {
		var url string
		if strings.HasPrefix(addr, "http://") {
			url = fmt.Sprintf("%s%s", addr, route)
		} else {
			url = fmt.Sprintf("http://%s%s", addr, route)
		}

		if ctx != nil {
			req, err = http.NewRequestWithContext(ctx, method, url, body)
		} else {
			req, err = http.NewRequest(method, url, body)
		}
		if err != nil {
			return err
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}

		res, err := http.DefaultClient.Do(req)
		if err == nil {
			defer terror.Log(res.Body.Close())
			if res.StatusCode != http.StatusOK {
				bodyBytes, err := ioutil.ReadAll(res.Body)
				return errors.Wrapf(err, "%s", bodyBytes)
			}
			return nil
		}
	}
	return err
}

// UpdatePlacementRules is used to notify PD changes of placement rules.
func UpdatePlacementRules(ctx context.Context, rules []*placement.Rule) error {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return err
	}

	var addrs []string
	if is.etcdCli != nil {
		addrs = is.etcdCli.Endpoints()
	}

	if len(addrs) == 0 {
		return errors.Errorf("pd unavailable")
	}

	b, err := json.Marshal(rules)
	if err != nil {
		return err
	}

	err = doRequest(ctx, addrs, path.Join(pdapi.Config, "rules"), http.MethodPost, bytes.NewReader(b))
	if err != nil {
		return err
	}

	return nil
}

func (is *InfoSyncer) getAllServerInfo(ctx context.Context) (map[string]*ServerInfo, error) {
	allInfo := make(map[string]*ServerInfo)
	if is.etcdCli == nil {
		allInfo[is.info.ID] = getServerInfo(is.info.ID)
		return allInfo, nil
	}
	allInfo, err := getInfo(ctx, is.etcdCli, ServerInformationPath, keyOpDefaultRetryCnt, keyOpDefaultTimeout, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	return allInfo, nil
}

// storeServerInfo stores self server static information to etcd.
func (is *InfoSyncer) storeServerInfo(ctx context.Context) error {
	if is.etcdCli == nil {
		return nil
	}
	infoBuf, err := json.Marshal(is.info)
	if err != nil {
		return errors.Trace(err)
	}
	str := string(hack.String(infoBuf))
	err = util.PutKVToEtcd(ctx, is.etcdCli, keyOpDefaultRetryCnt, is.serverInfoPath, str, clientv3.WithLease(is.session.Lease()))
	return err
}

// RemoveServerInfo remove self server static information from etcd.
func (is *InfoSyncer) RemoveServerInfo() {
	if is.etcdCli == nil {
		return
	}
	err := util.DeleteKeyFromEtcd(is.serverInfoPath, is.etcdCli, keyOpDefaultRetryCnt, keyOpDefaultTimeout)
	if err != nil {
		logutil.BgLogger().Error("remove server info failed", zap.Error(err))
	}
}

type topologyInfo struct {
	ServerVersionInfo
	StatusPort     uint   `json:"status_port"`
	DeployPath     string `json:"deploy_path"`
	StartTimestamp int64  `json:"start_timestamp"`
}

func (is *InfoSyncer) getTopologyInfo() topologyInfo {
	s, err := os.Executable()
	if err != nil {
		s = ""
	}
	dir := path.Dir(s)
	return topologyInfo{
		ServerVersionInfo: ServerVersionInfo{
			Version: mysql.TiDBReleaseVersion,
			GitHash: is.info.ServerVersionInfo.GitHash,
		},
		StatusPort:     is.info.StatusPort,
		DeployPath:     dir,
		StartTimestamp: is.info.StartTimestamp,
	}
}

// StoreTopologyInfo  stores the topology of tidb to etcd.
func (is *InfoSyncer) StoreTopologyInfo(ctx context.Context) error {
	if is.etcdCli == nil {
		return nil
	}
	topologyInfo := is.getTopologyInfo()
	infoBuf, err := json.Marshal(topologyInfo)
	if err != nil {
		return errors.Trace(err)
	}
	str := string(hack.String(infoBuf))
	key := fmt.Sprintf("%s/%s:%v/info", TopologyInformationPath, is.info.IP, is.info.Port)
	// Note: no lease is required here.
	err = util.PutKVToEtcd(ctx, is.etcdCli, keyOpDefaultRetryCnt, key, str)
	if err != nil {
		return err
	}
	// Initialize ttl.
	return is.updateTopologyAliveness(ctx)
}

// GetMinStartTS get min start timestamp.
// Export for testing.
func (is *InfoSyncer) GetMinStartTS() uint64 {
	return is.minStartTS
}

// storeMinStartTS stores self server min start timestamp to etcd.
func (is *InfoSyncer) storeMinStartTS(ctx context.Context) error {
	if is.etcdCli == nil {
		return nil
	}
	return util.PutKVToEtcd(ctx, is.etcdCli, keyOpDefaultRetryCnt, is.minStartTSPath,
		strconv.FormatUint(is.minStartTS, 10),
		clientv3.WithLease(is.session.Lease()))
}

// RemoveMinStartTS removes self server min start timestamp from etcd.
func (is *InfoSyncer) RemoveMinStartTS() {
	if is.etcdCli == nil {
		return
	}
	err := util.DeleteKeyFromEtcd(is.minStartTSPath, is.etcdCli, keyOpDefaultRetryCnt, keyOpDefaultTimeout)
	if err != nil {
		logutil.BgLogger().Error("remove minStartTS failed", zap.Error(err))
	}
}

// ReportMinStartTS reports self server min start timestamp to ETCD.
func (is *InfoSyncer) ReportMinStartTS(store kv.Storage) {
	if is.manager == nil {
		// Server may not start in time.
		return
	}
	pl := is.manager.ShowProcessList()

	// Calculate the lower limit of the start timestamp to avoid extremely old transaction delaying GC.
	currentVer, err := store.CurrentVersion()
	if err != nil {
		logutil.BgLogger().Error("update minStartTS failed", zap.Error(err))
		return
	}
	now := time.Unix(0, oracle.ExtractPhysical(currentVer.Ver)*1e6)
	startTSLowerLimit := variable.GoTimeToTS(now.Add(-time.Duration(kv.MaxTxnTimeUse) * time.Millisecond))

	minStartTS := variable.GoTimeToTS(now)
	for _, info := range pl {
		if info.CurTxnStartTS > startTSLowerLimit && info.CurTxnStartTS < minStartTS {
			minStartTS = info.CurTxnStartTS
		}
	}

	is.minStartTS = minStartTS
	err = is.storeMinStartTS(context.Background())
	if err != nil {
		logutil.BgLogger().Error("update minStartTS failed", zap.Error(err))
	}
}

// Done returns a channel that closes when the info syncer is no longer being refreshed.
func (is *InfoSyncer) Done() <-chan struct{} {
	if is.etcdCli == nil {
		return make(chan struct{}, 1)
	}
	return is.session.Done()
}

// TopologyDone returns a channel that closes when the topology syncer is no longer being refreshed.
func (is *InfoSyncer) TopologyDone() <-chan struct{} {
	if is.etcdCli == nil {
		return make(chan struct{}, 1)
	}
	return is.topologySession.Done()
}

// Restart restart the info syncer with new session leaseID and store server info to etcd again.
func (is *InfoSyncer) Restart(ctx context.Context) error {
	return is.newSessionAndStoreServerInfo(ctx, owner.NewSessionDefaultRetryCnt)
}

// RestartTopology restart the topology syncer with new session leaseID and store server info to etcd again.
func (is *InfoSyncer) RestartTopology(ctx context.Context) error {
	return is.newTopologySessionAndStoreServerInfo(ctx, owner.NewSessionDefaultRetryCnt)
}

// newSessionAndStoreServerInfo creates a new etcd session and stores server info to etcd.
func (is *InfoSyncer) newSessionAndStoreServerInfo(ctx context.Context, retryCnt int) error {
	if is.etcdCli == nil {
		return nil
	}
	logPrefix := fmt.Sprintf("[Info-syncer] %s", is.serverInfoPath)
	session, err := owner.NewSession(ctx, logPrefix, is.etcdCli, retryCnt, InfoSessionTTL)
	if err != nil {
		return err
	}
	is.session = session
	binloginfo.RegisterStatusListener(func(status binloginfo.BinlogStatus) error {
		is.info.BinlogStatus = status.String()
		err := is.storeServerInfo(ctx)
		return errors.Trace(err)
	})
	return is.storeServerInfo(ctx)
}

// newTopologySessionAndStoreServerInfo creates a new etcd session and stores server info to etcd.
func (is *InfoSyncer) newTopologySessionAndStoreServerInfo(ctx context.Context, retryCnt int) error {
	if is.etcdCli == nil {
		return nil
	}
	logPrefix := fmt.Sprintf("[topology-syncer] %s/%s:%d", TopologyInformationPath, is.info.IP, is.info.Port)
	session, err := owner.NewSession(ctx, logPrefix, is.etcdCli, retryCnt, TopologySessionTTL)
	if err != nil {
		return err
	}

	is.topologySession = session
	return is.StoreTopologyInfo(ctx)
}

// refreshTopology refreshes etcd topology with ttl stored in "/topology/tidb/ip:port/ttl".
func (is *InfoSyncer) updateTopologyAliveness(ctx context.Context) error {
	if is.etcdCli == nil {
		return nil
	}
	key := fmt.Sprintf("%s/%s:%v/ttl", TopologyInformationPath, is.info.IP, is.info.Port)
	return util.PutKVToEtcd(ctx, is.etcdCli, keyOpDefaultRetryCnt, key,
		fmt.Sprintf("%v", time.Now().UnixNano()),
		clientv3.WithLease(is.topologySession.Lease()))
}

// GetPrometheusAddr gets prometheus Address
func GetPrometheusAddr() (string, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return "", err
	}

	// if the cache of prometheusAddr is over 10s, update the prometheusAddr
	if time.Since(is.modifyTime) < TablePrometheusCacheExpiry {
		return is.prometheusAddr, nil
	}
	return is.getPrometheusAddr()
}

type prometheus struct {
	IP         string `json:"ip"`
	BinaryPath string `json:"binary_path"`
	Port       int    `json:"port"`
}

type metricStorage struct {
	PDServer struct {
		MetricStorage string `json:"metric-storage"`
	} `json:"pd-server"`
}

func (is *InfoSyncer) getPrometheusAddr() (string, error) {
	// Get PD servers info.
	pdAddrs := is.etcdCli.Endpoints()
	if len(pdAddrs) == 0 {
		return "", errors.Errorf("pd unavailable")
	}

	// Get prometheus address from pdApi.
	var url, res string
	if strings.HasPrefix(pdAddrs[0], "http://") {
		url = fmt.Sprintf("%s%s", pdAddrs[0], pdapi.Config)
	} else {
		url = fmt.Sprintf("http://%s%s", pdAddrs[0], pdapi.Config)
	}
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	var metricStorage metricStorage
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&metricStorage)
	if err != nil {
		return "", err
	}
	res = metricStorage.PDServer.MetricStorage

	// Get prometheus address from etcdApi.
	if res == "" {
		values, err := is.getPrometheusAddrFromEtcd(TopologyPrometheus)
		if err != nil {
			return "", errors.Trace(err)
		}
		if values == "" {
			return "", ErrPrometheusAddrIsNotSet
		}
		var prometheus prometheus
		err = json.Unmarshal([]byte(values), &prometheus)
		if err != nil {
			return "", errors.Trace(err)
		}
		res = fmt.Sprintf("http://%s:%v", prometheus.IP, prometheus.Port)
	}
	is.prometheusAddr = res
	is.modifyTime = time.Now()
	setGlobalInfoSyncer(is)
	return res, nil
}

func (is *InfoSyncer) getPrometheusAddrFromEtcd(k string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), keyOpDefaultTimeout)
	resp, err := is.etcdCli.Get(ctx, k)
	cancel()
	if err != nil {
		return "", errors.Trace(err)
	}
	if len(resp.Kvs) > 0 {
		return string(resp.Kvs[0].Value), nil
	}
	return "", nil
}

// getInfo gets server information from etcd according to the key and opts.
func getInfo(ctx context.Context, etcdCli *clientv3.Client, key string, retryCnt int, timeout time.Duration, opts ...clientv3.OpOption) (map[string]*ServerInfo, error) {
	var err error
	var resp *clientv3.GetResponse
	allInfo := make(map[string]*ServerInfo)
	for i := 0; i < retryCnt; i++ {
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
			info := &ServerInfo{
				BinlogStatus: binloginfo.BinlogStatusUnknown.String(),
			}
			err = json.Unmarshal(kv.Value, info)
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
func getServerInfo(id string) *ServerInfo {
	cfg := config.GetGlobalConfig()
	info := &ServerInfo{
		ID:             id,
		IP:             cfg.AdvertiseAddress,
		Port:           cfg.Port,
		StatusPort:     cfg.Status.StatusPort,
		Lease:          cfg.Lease,
		BinlogStatus:   binloginfo.GetStatus().String(),
		StartTimestamp: time.Now().Unix(),
	}
	info.Version = mysql.ServerVersion
	info.GitHash = versioninfo.TiDBGitHash

	failpoint.Inject("mockServerInfo", func(val failpoint.Value) {
		if val.(bool) {
			info.StartTimestamp = 1282967700000
		}
	})

	return info
}
