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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package infosync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl/label"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/types"
	util2 "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/pdapi"
	"github.com/pingcap/tidb/util/versioninfo"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
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
	// RequestRetryInterval is the sleep time before next retry for http request
	RequestRetryInterval = 200 * time.Millisecond
	// SyncBundlesMaxRetry is the max retry times for sync placement bundles
	SyncBundlesMaxRetry = 3
)

// ErrPrometheusAddrIsNotSet is the error that Prometheus address is not set in PD and etcd
var ErrPrometheusAddrIsNotSet = dbterror.ClassDomain.NewStd(errno.ErrPrometheusAddrIsNotSet)

// InfoSyncer stores server info to etcd when the tidb-server starts and delete when tidb-server shuts down.
type InfoSyncer struct {
	etcdCli                 *clientv3.Client
	info                    *ServerInfo
	serverInfoPath          string
	minStartTS              uint64
	minStartTSPath          string
	manager                 util2.SessionManager
	session                 *concurrency.Session
	topologySession         *concurrency.Session
	prometheusAddr          string
	modifyTime              time.Time
	labelRuleManager        LabelRuleManager
	placementManager        PlacementManager
	tiflashPlacementManager TiFlashPlacementManager
}

// ServerInfo is server static information.
// It will not be updated when tidb-server running. So please only put static information in ServerInfo struct.
type ServerInfo struct {
	ServerVersionInfo
	ID             string            `json:"ddl_id"`
	IP             string            `json:"ip"`
	Port           uint              `json:"listening_port"`
	StatusPort     uint              `json:"status_port"`
	Lease          string            `json:"lease"`
	BinlogStatus   string            `json:"binlog_status"`
	StartTimestamp int64             `json:"start_timestamp"`
	Labels         map[string]string `json:"labels"`
	// ServerID is a function, to always retrieve latest serverID from `Domain`,
	//   which will be changed on occasions such as connection to PD is restored after broken.
	ServerIDGetter func() uint64 `json:"-"`

	// JSONServerID is `serverID` for json marshal/unmarshal ONLY.
	JSONServerID uint64 `json:"server_id"`
}

// Marshal `ServerInfo` into bytes.
func (info *ServerInfo) Marshal() ([]byte, error) {
	info.JSONServerID = info.ServerIDGetter()
	infoBuf, err := json.Marshal(info)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return infoBuf, nil
}

// Unmarshal `ServerInfo` from bytes.
func (info *ServerInfo) Unmarshal(v []byte) error {
	if err := json.Unmarshal(v, info); err != nil {
		return err
	}
	info.ServerIDGetter = func() uint64 {
		return info.JSONServerID
	}
	return nil
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
func GlobalInfoSyncerInit(ctx context.Context, id string, serverIDGetter func() uint64, etcdCli *clientv3.Client, skipRegisterToDashBoard bool) (*InfoSyncer, error) {
	is := &InfoSyncer{
		etcdCli:        etcdCli,
		info:           getServerInfo(id, serverIDGetter),
		serverInfoPath: fmt.Sprintf("%s/%s", ServerInformationPath, id),
		minStartTSPath: fmt.Sprintf("%s/%s", ServerMinStartTSPath, id),
	}
	err := is.init(ctx, skipRegisterToDashBoard)
	if err != nil {
		return nil, err
	}
	if etcdCli != nil {
		is.labelRuleManager = initLabelRuleManager(etcdCli.Endpoints())
		is.placementManager = initPlacementManager(etcdCli.Endpoints())
		is.tiflashPlacementManager = initTiFlashPlacementManager(etcdCli.Endpoints())
	} else {
		is.labelRuleManager = initLabelRuleManager([]string{})
		is.placementManager = initPlacementManager([]string{})
		is.tiflashPlacementManager = initTiFlashPlacementManager([]string{})
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

// GetSessionManager get the session manager.
func (is *InfoSyncer) GetSessionManager() util2.SessionManager {
	return is.manager
}

func initLabelRuleManager(addrs []string) LabelRuleManager {
	if len(addrs) == 0 {
		return &mockLabelManager{labelRules: map[string][]byte{}}
	}
	return &PDLabelManager{addrs: addrs}
}

func initPlacementManager(addrs []string) PlacementManager {
	if len(addrs) == 0 {
		return &mockPlacementManager{}
	}
	return &PDPlacementManager{addrs: addrs}
}

func initTiFlashPlacementManager(addrs []string) TiFlashPlacementManager {
	if len(addrs) == 0 {
		m := mockTiFlashPlacementManager{}
		return &m
	}
	return &TiFlashPDPlacementManager{addrs: addrs}
}

// GetMockTiFlash can only be used in tests to get MockTiFlash
func GetMockTiFlash() *MockTiFlash {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil
	}

	m, ok := is.tiflashPlacementManager.(*mockTiFlashPlacementManager)
	if ok {
		return m.tiflash
	}
	return nil
}

// SetMockTiFlash can only be used in tests to set MockTiFlash
func SetMockTiFlash(tiflash *MockTiFlash) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return
	}

	m, ok := is.tiflashPlacementManager.(*mockTiFlashPlacementManager)
	if ok {
		m.tiflash = tiflash
	}
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
	// truncate progress with 2 decimal digits so that it will not be rounded to 1 when the progress is 0.995
	progressString := types.TruncateFloatToString(progress, 2)
	return util.PutKVToEtcd(ctx, is.etcdCli, keyOpDefaultRetryCnt, key, progressString)
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

func doRequest(ctx context.Context, addrs []string, route, method string, body io.Reader) ([]byte, error) {
	var err error
	var req *http.Request
	var res *http.Response
	for _, addr := range addrs {
		url := util2.ComposeURL(addr, route)
		req, err = http.NewRequestWithContext(ctx, method, url, body)
		if err != nil {
			return nil, err
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		start := time.Now()
		res, err = doRequestWithFailpoint(req)
		metrics.PDApiExecutionHistogram.WithLabelValues("placement").Observe(time.Since(start).Seconds())
		if err == nil {
			bodyBytes, err := io.ReadAll(res.Body)
			if err != nil {
				terror.Log(res.Body.Close())
				return nil, err
			}
			if res.StatusCode != http.StatusOK {
				err = ErrHTTPServiceError.FastGen("%s", bodyBytes)
				if res.StatusCode == http.StatusNotFound || res.StatusCode == http.StatusPreconditionFailed {
					err = nil
					bodyBytes = nil
				}
			}
			terror.Log(res.Body.Close())
			return bodyBytes, err
		}
	}
	return nil, err
}

func doRequestWithFailpoint(req *http.Request) (resp *http.Response, err error) {
	fpEnabled := false
	failpoint.Inject("FailPlacement", func(val failpoint.Value) {
		if val.(bool) {
			fpEnabled = true
			resp = &http.Response{StatusCode: http.StatusNotFound, Body: http.NoBody}
			err = nil
		}
	})
	if fpEnabled {
		return
	}
	return util2.InternalHTTPClient().Do(req)
}

// GetAllRuleBundles is used to get all rule bundles from PD. It is used to load full rules from PD while fullload infoschema.
func GetAllRuleBundles(ctx context.Context) ([]*placement.Bundle, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, err
	}

	return is.placementManager.GetAllRuleBundles(ctx)
}

// GetRuleBundle is used to get one specific rule bundle from PD.
func GetRuleBundle(ctx context.Context, name string) (*placement.Bundle, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, err
	}

	return is.placementManager.GetRuleBundle(ctx, name)
}

// PutRuleBundles is used to post specific rule bundles to PD.
func PutRuleBundles(ctx context.Context, bundles []*placement.Bundle) error {
	failpoint.Inject("putRuleBundlesError", func(isServiceError failpoint.Value) {
		var err error
		if isServiceError.(bool) {
			err = ErrHTTPServiceError.FastGen("mock service error")
		} else {
			err = errors.New("mock other error")
		}
		failpoint.Return(err)
	})

	is, err := getGlobalInfoSyncer()
	if err != nil {
		return err
	}

	return is.placementManager.PutRuleBundles(ctx, bundles)
}

// PutRuleBundlesWithRetry will retry for specified times when PutRuleBundles failed
func PutRuleBundlesWithRetry(ctx context.Context, bundles []*placement.Bundle, maxRetry int, interval time.Duration) (err error) {
	if maxRetry < 0 {
		maxRetry = 0
	}

	for i := 0; i <= maxRetry; i++ {
		if err = PutRuleBundles(ctx, bundles); err == nil || ErrHTTPServiceError.Equal(err) {
			return err
		}

		if i != maxRetry {
			logutil.BgLogger().Warn("Error occurs when PutRuleBundles, retry", zap.Error(err))
			time.Sleep(interval)
		}
	}

	return
}

// PutRuleBundlesWithDefaultRetry will retry for default times
func PutRuleBundlesWithDefaultRetry(ctx context.Context, bundles []*placement.Bundle) (err error) {
	return PutRuleBundlesWithRetry(ctx, bundles, SyncBundlesMaxRetry, RequestRetryInterval)
}

func (is *InfoSyncer) getAllServerInfo(ctx context.Context) (map[string]*ServerInfo, error) {
	allInfo := make(map[string]*ServerInfo)
	if is.etcdCli == nil {
		allInfo[is.info.ID] = getServerInfo(is.info.ID, is.info.ServerIDGetter)
		return allInfo, nil
	}
	allInfo, err := getInfo(ctx, is.etcdCli, ServerInformationPath, keyOpDefaultRetryCnt, keyOpDefaultTimeout, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	return allInfo, nil
}

// StoreServerInfo stores self server static information to etcd.
func (is *InfoSyncer) StoreServerInfo(ctx context.Context) error {
	if is.etcdCli == nil {
		return nil
	}
	infoBuf, err := is.info.Marshal()
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

// TopologyInfo is the topology info
type TopologyInfo struct {
	ServerVersionInfo
	IP             string            `json:"ip"`
	StatusPort     uint              `json:"status_port"`
	DeployPath     string            `json:"deploy_path"`
	StartTimestamp int64             `json:"start_timestamp"`
	Labels         map[string]string `json:"labels"`
}

func (is *InfoSyncer) getTopologyInfo() TopologyInfo {
	s, err := os.Executable()
	if err != nil {
		s = ""
	}
	dir := path.Dir(s)
	return TopologyInfo{
		ServerVersionInfo: ServerVersionInfo{
			Version: mysql.TiDBReleaseVersion,
			GitHash: is.info.ServerVersionInfo.GitHash,
		},
		IP:             is.info.IP,
		StatusPort:     is.info.StatusPort,
		DeployPath:     dir,
		StartTimestamp: is.info.StartTimestamp,
		Labels:         is.info.Labels,
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
	currentVer, err := store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		logutil.BgLogger().Error("update minStartTS failed", zap.Error(err))
		return
	}
	now := oracle.GetTimeFromTS(currentVer.Ver)
	startTSLowerLimit := oracle.GoTimeToLowerLimitStartTS(now, tikv.MaxTxnTimeUse)

	minStartTS := oracle.GoTimeToTS(now)
	for _, info := range pl {
		if info.CurTxnStartTS > startTSLowerLimit && info.CurTxnStartTS < minStartTS {
			minStartTS = info.CurTxnStartTS
		}
	}
	phyMinStartTs := oracle.ExtractPhysical(minStartTS)
	metrics.DomainMinStartTsGauge.Set(float64(phyMinStartTs))

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

// GetAllTiDBTopology gets all tidb topology
func (is *InfoSyncer) GetAllTiDBTopology(ctx context.Context) ([]*TopologyInfo, error) {
	topos := make([]*TopologyInfo, 0)
	response, err := is.etcdCli.Get(ctx, TopologyInformationPath, clientv3.WithPrefix())
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
		err := is.StoreServerInfo(ctx)
		return errors.Trace(err)
	})
	return is.StoreServerInfo(ctx)
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
	clientAvailable := is.etcdCli != nil
	var pdAddrs []string
	if clientAvailable {
		pdAddrs = is.etcdCli.Endpoints()
	}
	if !clientAvailable || len(pdAddrs) == 0 {
		return "", errors.Errorf("pd unavailable")
	}
	// Get prometheus address from pdApi.
	url := util2.ComposeURL(pdAddrs[0], pdapi.Config)
	resp, err := util2.InternalHTTPClient().Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var metricStorage metricStorage
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&metricStorage)
	if err != nil {
		return "", err
	}
	res := metricStorage.PDServer.MetricStorage

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
func getServerInfo(id string, serverIDGetter func() uint64) *ServerInfo {
	cfg := config.GetGlobalConfig()
	info := &ServerInfo{
		ID:             id,
		IP:             cfg.AdvertiseAddress,
		Port:           cfg.Port,
		StatusPort:     cfg.Status.StatusPort,
		Lease:          cfg.Lease,
		BinlogStatus:   binloginfo.GetStatus().String(),
		StartTimestamp: time.Now().Unix(),
		Labels:         cfg.Labels,
		ServerIDGetter: serverIDGetter,
	}
	info.Version = mysql.ServerVersion
	info.GitHash = versioninfo.TiDBGitHash

	metrics.ServerInfo.WithLabelValues(mysql.TiDBReleaseVersion, info.GitHash).Set(float64(info.StartTimestamp))

	failpoint.Inject("mockServerInfo", func(val failpoint.Value) {
		if val.(bool) {
			info.StartTimestamp = 1282967700
			info.Labels = map[string]string{
				"foo": "bar",
			}
		}
	})

	return info
}

// PutLabelRule synchronizes the label rule to PD.
func PutLabelRule(ctx context.Context, rule *label.Rule) error {
	if rule == nil {
		return nil
	}

	is, err := getGlobalInfoSyncer()
	if err != nil {
		return err
	}
	if is.labelRuleManager == nil {
		return nil
	}
	return is.labelRuleManager.PutLabelRule(ctx, rule)
}

// UpdateLabelRules synchronizes the label rule to PD.
func UpdateLabelRules(ctx context.Context, patch *label.RulePatch) error {
	if patch == nil || (len(patch.DeleteRules) == 0 && len(patch.SetRules) == 0) {
		return nil
	}

	is, err := getGlobalInfoSyncer()
	if err != nil {
		return err
	}
	if is.labelRuleManager == nil {
		return nil
	}
	return is.labelRuleManager.UpdateLabelRules(ctx, patch)
}

// GetAllLabelRules gets all label rules from PD.
func GetAllLabelRules(ctx context.Context) ([]*label.Rule, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, err
	}
	if is.labelRuleManager == nil {
		return nil, nil
	}
	return is.labelRuleManager.GetAllLabelRules(ctx)
}

// GetLabelRules gets the label rules according to the given IDs from PD.
func GetLabelRules(ctx context.Context, ruleIDs []string) (map[string]*label.Rule, error) {
	if len(ruleIDs) == 0 {
		return nil, nil
	}

	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, err
	}
	if is.labelRuleManager == nil {
		return nil, nil
	}
	return is.labelRuleManager.GetLabelRules(ctx, ruleIDs)
}

// SetTiFlashPlacementRule is a helper function to set placement rule.
func SetTiFlashPlacementRule(ctx context.Context, rule placement.TiFlashRule) error {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return errors.Trace(err)
	}
	return is.tiflashPlacementManager.SetPlacementRule(ctx, rule)
}

// DeleteTiFlashPlacementRule is to delete placement rule for certain group.
func DeleteTiFlashPlacementRule(ctx context.Context, group string, ruleID string) error {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return errors.Trace(err)
	}
	return is.tiflashPlacementManager.DeletePlacementRule(ctx, group, ruleID)
}

// GetTiFlashGroupRules to get all placement rule in a certain group.
func GetTiFlashGroupRules(ctx context.Context, group string) ([]placement.TiFlashRule, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return is.tiflashPlacementManager.GetGroupRules(ctx, group)
}

// PostTiFlashAccelerateSchedule sends `regions/accelerate-schedule` request.
func PostTiFlashAccelerateSchedule(ctx context.Context, tableID int64) error {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return errors.Trace(err)
	}
	return is.tiflashPlacementManager.PostAccelerateSchedule(ctx, tableID)
}

// GetTiFlashPDRegionRecordStats is a helper function calling `/stats/region`.
func GetTiFlashPDRegionRecordStats(ctx context.Context, tableID int64, stats *helper.PDRegionStats) error {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return errors.Trace(err)
	}
	return is.tiflashPlacementManager.GetPDRegionRecordStats(ctx, tableID, stats)
}

// GetTiFlashStoresStat gets the TiKV store information by accessing PD's api.
func GetTiFlashStoresStat(ctx context.Context) (*helper.StoresStat, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return is.tiflashPlacementManager.GetStoresStat(ctx)
}

// CloseTiFlashManager closes TiFlash manager.
func CloseTiFlashManager(ctx context.Context) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return
	}
	is.tiflashPlacementManager.Close(ctx)
}

// ConfigureTiFlashPDForTable configures pd rule for unpartitioned tables.
func ConfigureTiFlashPDForTable(id int64, count uint64, locationLabels *[]string) error {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return errors.Trace(err)
	}
	ctx := context.Background()
	logutil.BgLogger().Info("ConfigureTiFlashPDForTable", zap.Int64("tableID", id), zap.Uint64("count", count))
	ruleNew := MakeNewRule(id, count, *locationLabels)
	if e := is.tiflashPlacementManager.SetPlacementRule(ctx, *ruleNew); e != nil {
		return errors.Trace(e)
	}
	return nil
}

// ConfigureTiFlashPDForPartitions configures pd rule for all partition in partitioned tables.
func ConfigureTiFlashPDForPartitions(accel bool, definitions *[]model.PartitionDefinition, count uint64, locationLabels *[]string) error {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return errors.Trace(err)
	}
	ctx := context.Background()
	for _, p := range *definitions {
		logutil.BgLogger().Info("ConfigureTiFlashPDForPartitions", zap.Int64("partID", p.ID), zap.Bool("accel", accel), zap.Uint64("count", count))
		ruleNew := MakeNewRule(p.ID, count, *locationLabels)
		if e := is.tiflashPlacementManager.SetPlacementRule(ctx, *ruleNew); e != nil {
			return errors.Trace(e)
		}
		if accel {
			e := is.tiflashPlacementManager.PostAccelerateSchedule(ctx, p.ID)
			if e != nil {
				return errors.Trace(e)
			}
		}
	}
	return nil
}
