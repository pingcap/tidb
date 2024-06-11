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
	"net"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/label"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/domain/resourcegroup"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/binloginfo"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	util2 "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/engine"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/versioninfo"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
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
	// TopologyTiProxy means address of TiProxy.
	TopologyTiProxy = "/topology/tiproxy"
	// infoSuffix is the suffix of TiDB/TiProxy topology info.
	infoSuffix = "/info"
	// TopologyTiCDC means address of TiCDC.
	TopologyTiCDC = "/topology/ticdc"
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
	// `etcdClient` must be used when keyspace is not set, or when the logic to each etcd path needs to be separated by keyspace.
	etcdCli *clientv3.Client
	// `unprefixedEtcdCli` will never set the etcd namespace prefix by keyspace.
	// It is only used in storeMinStartTS and RemoveMinStartTS now.
	// It must be used when the etcd path isn't needed to separate by keyspace.
	// See keyspace RFC: https://github.com/pingcap/tidb/pull/39685
	unprefixedEtcdCli *clientv3.Client
	pdHTTPCli         pdhttp.Client
	info              *ServerInfo
	serverInfoPath    string
	minStartTS        uint64
	minStartTSPath    string
	managerMu         struct {
		mu sync.RWMutex
		util2.SessionManager
	}
	session               *concurrency.Session
	topologySession       *concurrency.Session
	prometheusAddr        string
	modifyTime            time.Time
	labelRuleManager      LabelRuleManager
	placementManager      PlacementManager
	scheduleManager       ScheduleManager
	tiflashReplicaManager TiFlashReplicaManager
	resourceManagerClient pd.ResourceManagerClient
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
	// which will be changed on occasions such as connection to PD is restored after broken.
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
// Use atomic.Pointer to avoid data race in the test.
var globalInfoSyncer atomic.Pointer[InfoSyncer]

func getGlobalInfoSyncer() (*InfoSyncer, error) {
	v := globalInfoSyncer.Load()
	if v == nil {
		return nil, errors.New("infoSyncer is not initialized")
	}
	return v, nil
}

func setGlobalInfoSyncer(is *InfoSyncer) {
	globalInfoSyncer.Store(is)
}

// GlobalInfoSyncerInit return a new InfoSyncer. It is exported for testing.
func GlobalInfoSyncerInit(
	ctx context.Context,
	id string,
	serverIDGetter func() uint64,
	etcdCli, unprefixedEtcdCli *clientv3.Client,
	pdCli pd.Client, pdHTTPCli pdhttp.Client,
	codec tikv.Codec,
	skipRegisterToDashBoard bool,
) (*InfoSyncer, error) {
	if pdHTTPCli != nil {
		pdHTTPCli = pdHTTPCli.
			WithCallerID("tidb-info-syncer").
			WithRespHandler(pdResponseHandler)
	}
	is := &InfoSyncer{
		etcdCli:           etcdCli,
		unprefixedEtcdCli: unprefixedEtcdCli,
		pdHTTPCli:         pdHTTPCli,
		info:              getServerInfo(id, serverIDGetter),
		serverInfoPath:    fmt.Sprintf("%s/%s", ServerInformationPath, id),
		minStartTSPath:    fmt.Sprintf("%s/%s", ServerMinStartTSPath, id),
	}
	err := is.init(ctx, skipRegisterToDashBoard)
	if err != nil {
		return nil, err
	}
	is.initLabelRuleManager()
	is.initPlacementManager()
	is.initScheduleManager()
	is.initTiFlashReplicaManager(codec)
	is.initResourceManagerClient(pdCli)
	setGlobalInfoSyncer(is)
	return is, nil
}

// Init creates a new etcd session and stores server info to etcd.
func (is *InfoSyncer) init(ctx context.Context, skipRegisterToDashboard bool) error {
	err := is.newSessionAndStoreServerInfo(ctx, util2.NewSessionDefaultRetryCnt)
	if err != nil {
		return err
	}
	if skipRegisterToDashboard {
		return nil
	}
	return is.newTopologySessionAndStoreServerInfo(ctx, util2.NewSessionDefaultRetryCnt)
}

// SetSessionManager set the session manager for InfoSyncer.
func (is *InfoSyncer) SetSessionManager(manager util2.SessionManager) {
	is.managerMu.mu.Lock()
	defer is.managerMu.mu.Unlock()
	is.managerMu.SessionManager = manager
}

// GetSessionManager get the session manager.
func (is *InfoSyncer) GetSessionManager() util2.SessionManager {
	is.managerMu.mu.RLock()
	defer is.managerMu.mu.RUnlock()
	return is.managerMu.SessionManager
}

func (is *InfoSyncer) initLabelRuleManager() {
	if is.pdHTTPCli == nil {
		is.labelRuleManager = &mockLabelManager{labelRules: map[string][]byte{}}
		return
	}
	is.labelRuleManager = &PDLabelManager{is.pdHTTPCli}
}

func (is *InfoSyncer) initPlacementManager() {
	if is.pdHTTPCli == nil {
		is.placementManager = &mockPlacementManager{}
		return
	}
	is.placementManager = &PDPlacementManager{is.pdHTTPCli}
}

func (is *InfoSyncer) initResourceManagerClient(pdCli pd.Client) {
	var cli pd.ResourceManagerClient = pdCli
	if pdCli == nil {
		cli = NewMockResourceManagerClient()
	}
	failpoint.Inject("managerAlreadyCreateSomeGroups", func(val failpoint.Value) {
		if val.(bool) {
			_, err := cli.AddResourceGroup(context.TODO(),
				&rmpb.ResourceGroup{
					Name: resourcegroup.DefaultResourceGroupName,
					Mode: rmpb.GroupMode_RUMode,
					RUSettings: &rmpb.GroupRequestUnitSettings{
						RU: &rmpb.TokenBucket{
							Settings: &rmpb.TokenLimitSettings{FillRate: 1000000, BurstLimit: -1},
						},
					},
				})
			if err != nil {
				log.Warn("fail to create default group", zap.Error(err))
			}
			_, err = cli.AddResourceGroup(context.TODO(),
				&rmpb.ResourceGroup{
					Name: "oltp",
					Mode: rmpb.GroupMode_RUMode,
					RUSettings: &rmpb.GroupRequestUnitSettings{
						RU: &rmpb.TokenBucket{
							Settings: &rmpb.TokenLimitSettings{FillRate: 1000000, BurstLimit: -1},
						},
					},
				})
			if err != nil {
				log.Warn("fail to create default group", zap.Error(err))
			}
		}
	})
	is.resourceManagerClient = cli
}

func (is *InfoSyncer) initTiFlashReplicaManager(codec tikv.Codec) {
	if is.pdHTTPCli == nil {
		is.tiflashReplicaManager = &mockTiFlashReplicaManagerCtx{tiflashProgressCache: make(map[int64]float64)}
		return
	}
	logutil.BgLogger().Warn("init TiFlashReplicaManager")
	is.tiflashReplicaManager = &TiFlashReplicaManagerCtx{pdHTTPCli: is.pdHTTPCli, tiflashProgressCache: make(map[int64]float64), codec: codec}
}

func (is *InfoSyncer) initScheduleManager() {
	if is.pdHTTPCli == nil {
		is.scheduleManager = &mockScheduleManager{}
		return
	}
	is.scheduleManager = &PDScheduleManager{is.pdHTTPCli}
}

// GetMockTiFlash can only be used in tests to get MockTiFlash
func GetMockTiFlash() *MockTiFlash {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil
	}

	m, ok := is.tiflashReplicaManager.(*mockTiFlashReplicaManagerCtx)
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

	m, ok := is.tiflashReplicaManager.(*mockTiFlashReplicaManagerCtx)
	if ok {
		m.SetMockTiFlash(tiflash)
	}
}

// GetServerInfo gets self server static information.
func GetServerInfo() (*ServerInfo, error) {
	failpoint.Inject("mockGetServerInfo", func(v failpoint.Value) {
		var res ServerInfo
		err := json.Unmarshal([]byte(v.(string)), &res)
		failpoint.Return(&res, err)
	})
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
	failpoint.Inject("mockGetAllServerInfo", func(val failpoint.Value) {
		res := make(map[string]*ServerInfo)
		err := json.Unmarshal([]byte(val.(string)), &res)
		failpoint.Return(res, err)
	})
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, err
	}
	return is.getAllServerInfo(ctx)
}

// UpdateServerLabel updates the server label for global info syncer.
func UpdateServerLabel(ctx context.Context, labels map[string]string) error {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return err
	}
	// when etcdCli is nil, the server infos are generated from the latest config, no need to update.
	if is.etcdCli == nil {
		return nil
	}
	selfInfo, err := is.getServerInfoByID(ctx, is.info.ID)
	if err != nil {
		return err
	}
	changed := false
	for k, v := range labels {
		if selfInfo.Labels[k] != v {
			changed = true
			selfInfo.Labels[k] = v
		}
	}
	if !changed {
		return nil
	}
	infoBuf, err := selfInfo.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	str := string(hack.String(infoBuf))
	err = util.PutKVToEtcd(ctx, is.etcdCli, keyOpDefaultRetryCnt, is.serverInfoPath, str, clientv3.WithLease(is.session.Lease()))
	return err
}

// DeleteTiFlashTableSyncProgress is used to delete the tiflash table replica sync progress.
func DeleteTiFlashTableSyncProgress(tableInfo *model.TableInfo) error {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return err
	}
	if pi := tableInfo.GetPartitionInfo(); pi != nil {
		for _, p := range pi.Definitions {
			is.tiflashReplicaManager.DeleteTiFlashProgressFromCache(p.ID)
		}
	} else {
		is.tiflashReplicaManager.DeleteTiFlashProgressFromCache(tableInfo.ID)
	}
	return nil
}

// MustGetTiFlashProgress gets tiflash replica progress from tiflashProgressCache, if cache not exist, it calculates progress from PD and TiFlash and inserts progress into cache.
func MustGetTiFlashProgress(tableID int64, replicaCount uint64, tiFlashStores *map[int64]pdhttp.StoreInfo) (float64, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return 0, err
	}
	progressCache, isExist := is.tiflashReplicaManager.GetTiFlashProgressFromCache(tableID)
	if isExist {
		return progressCache, nil
	}
	if *tiFlashStores == nil {
		// We need the up-to-date information about TiFlash stores.
		// Since TiFlash Replica synchronize may happen immediately after new TiFlash stores are added.
		tikvStats, err := is.tiflashReplicaManager.GetStoresStat(context.Background())
		// If MockTiFlash is not set, will issue a MockTiFlashError here.
		if err != nil {
			return 0, err
		}
		stores := make(map[int64]pdhttp.StoreInfo)
		for _, store := range tikvStats.Stores {
			if engine.IsTiFlashHTTPResp(&store.Store) {
				stores[store.Store.ID] = store
			}
		}
		*tiFlashStores = stores
		logutil.BgLogger().Debug("updateTiFlashStores finished", zap.Int("TiFlash store count", len(*tiFlashStores)))
	}
	progress, err := is.tiflashReplicaManager.CalculateTiFlashProgress(tableID, replicaCount, *tiFlashStores)
	if err != nil {
		return 0, err
	}
	is.tiflashReplicaManager.UpdateTiFlashProgressCache(tableID, progress)
	return progress, nil
}

// pdResponseHandler will be injected into the PD HTTP client to handle the response,
// this is to maintain consistency with the original logic without the PD HTTP client.
func pdResponseHandler(resp *http.Response, res any) error {
	defer func() { terror.Log(resp.Body.Close()) }()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
		if res != nil && bodyBytes != nil {
			return json.Unmarshal(bodyBytes, res)
		}
		return nil
	}
	logutil.BgLogger().Warn("response not 200",
		zap.String("method", resp.Request.Method),
		zap.String("host", resp.Request.URL.Host),
		zap.String("url", resp.Request.URL.RequestURI()),
		zap.Int("http status", resp.StatusCode),
	)
	if resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusPreconditionFailed {
		return ErrHTTPServiceError.FastGen("%s", bodyBytes)
	}
	return nil
}

// GetAllRuleBundles is used to get all rule bundles from PD It is used to load full rules from PD while fullload infoschema.
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

// GetResourceGroup is used to get one specific resource group from resource manager.
func GetResourceGroup(ctx context.Context, name string) (*rmpb.ResourceGroup, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, err
	}

	return is.resourceManagerClient.GetResourceGroup(ctx, name)
}

// ListResourceGroups is used to get all resource groups from resource manager.
func ListResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, err
	}

	return is.resourceManagerClient.ListResourceGroups(ctx)
}

// AddResourceGroup is used to create one specific resource group to resource manager.
func AddResourceGroup(ctx context.Context, group *rmpb.ResourceGroup) error {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return err
	}
	_, err = is.resourceManagerClient.AddResourceGroup(ctx, group)
	return err
}

// ModifyResourceGroup is used to modify one specific resource group to resource manager.
func ModifyResourceGroup(ctx context.Context, group *rmpb.ResourceGroup) error {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return err
	}
	_, err = is.resourceManagerClient.ModifyResourceGroup(ctx, group)
	return err
}

// DeleteResourceGroup is used to delete one specific resource group from resource manager.
func DeleteResourceGroup(ctx context.Context, name string) error {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return err
	}
	_, err = is.resourceManagerClient.DeleteResourceGroup(ctx, name)
	return err
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
	key := fmt.Sprintf("%s/%s/info", TopologyInformationPath, net.JoinHostPort(is.info.IP, strconv.Itoa(int(is.info.Port))))
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
	if is.unprefixedEtcdCli == nil {
		return nil
	}
	return util.PutKVToEtcd(ctx, is.unprefixedEtcdCli, keyOpDefaultRetryCnt, is.minStartTSPath,
		strconv.FormatUint(is.minStartTS, 10),
		clientv3.WithLease(is.session.Lease()))
}

// RemoveMinStartTS removes self server min start timestamp from etcd.
func (is *InfoSyncer) RemoveMinStartTS() {
	if is.unprefixedEtcdCli == nil {
		return
	}
	err := util.DeleteKeyFromEtcd(is.minStartTSPath, is.unprefixedEtcdCli, keyOpDefaultRetryCnt, keyOpDefaultTimeout)
	if err != nil {
		logutil.BgLogger().Error("remove minStartTS failed", zap.Error(err))
	}
}

// ReportMinStartTS reports self server min start timestamp to ETCD.
func (is *InfoSyncer) ReportMinStartTS(store kv.Storage) {
	sm := is.GetSessionManager()
	if sm == nil {
		return
	}
	pl := sm.ShowProcessList()
	innerSessionStartTSList := sm.GetInternalSessionStartTSList()

	// Calculate the lower limit of the start timestamp to avoid extremely old transaction delaying GC.
	currentVer, err := store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		logutil.BgLogger().Error("update minStartTS failed", zap.Error(err))
		return
	}
	now := oracle.GetTimeFromTS(currentVer.Ver)
	// GCMaxWaitTime is in seconds, GCMaxWaitTime * 1000 converts it to milliseconds.
	startTSLowerLimit := oracle.GoTimeToLowerLimitStartTS(now, variable.GCMaxWaitTime.Load()*1000)
	minStartTS := oracle.GoTimeToTS(now)
	logutil.BgLogger().Debug("ReportMinStartTS", zap.Uint64("initial minStartTS", minStartTS),
		zap.Uint64("StartTSLowerLimit", startTSLowerLimit))
	for _, info := range pl {
		if info.CurTxnStartTS > startTSLowerLimit && info.CurTxnStartTS < minStartTS {
			minStartTS = info.CurTxnStartTS
		}
	}

	for _, innerTS := range innerSessionStartTSList {
		logutil.BgLogger().Debug("ReportMinStartTS", zap.Uint64("Internal Session Transaction StartTS", innerTS))
		kv.PrintLongTimeInternalTxn(now, innerTS, false)
		if innerTS > startTSLowerLimit && innerTS < minStartTS {
			minStartTS = innerTS
		}
	}

	is.minStartTS = kv.GetMinInnerTxnStartTS(now, startTSLowerLimit, minStartTS)

	err = is.storeMinStartTS(context.Background())
	if err != nil {
		logutil.BgLogger().Error("update minStartTS failed", zap.Error(err))
	}
	logutil.BgLogger().Debug("ReportMinStartTS", zap.Uint64("final minStartTS", is.minStartTS))
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
	return is.newSessionAndStoreServerInfo(ctx, util2.NewSessionDefaultRetryCnt)
}

// RestartTopology restart the topology syncer with new session leaseID and store server info to etcd again.
func (is *InfoSyncer) RestartTopology(ctx context.Context) error {
	return is.newTopologySessionAndStoreServerInfo(ctx, util2.NewSessionDefaultRetryCnt)
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
	session, err := util2.NewSession(ctx, logPrefix, is.etcdCli, retryCnt, util.SessionTTL)
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
	logPrefix := fmt.Sprintf("[topology-syncer] %s/%s", TopologyInformationPath, net.JoinHostPort(is.info.IP, strconv.Itoa(int(is.info.Port))))
	session, err := util2.NewSession(ctx, logPrefix, is.etcdCli, retryCnt, TopologySessionTTL)
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
	key := fmt.Sprintf("%s/%s/ttl", TopologyInformationPath, net.JoinHostPort(is.info.IP, strconv.Itoa(int(is.info.Port))))
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
	// Get prometheus address from pdhttp.
	url := util2.ComposeURL(pdAddrs[0], pdhttp.Config)
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
		res = fmt.Sprintf("http://%s", net.JoinHostPort(prometheus.IP, strconv.Itoa(prometheus.Port)))
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
func UpdateLabelRules(ctx context.Context, patch *pdhttp.LabelRulePatch) error {
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

// CalculateTiFlashProgress calculates TiFlash replica progress
func CalculateTiFlashProgress(tableID int64, replicaCount uint64, tiFlashStores map[int64]pdhttp.StoreInfo) (float64, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return 0, errors.Trace(err)
	}
	return is.tiflashReplicaManager.CalculateTiFlashProgress(tableID, replicaCount, tiFlashStores)
}

// UpdateTiFlashProgressCache updates tiflashProgressCache
func UpdateTiFlashProgressCache(tableID int64, progress float64) error {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return errors.Trace(err)
	}
	is.tiflashReplicaManager.UpdateTiFlashProgressCache(tableID, progress)
	return nil
}

// GetTiFlashProgressFromCache gets tiflash replica progress from tiflashProgressCache
func GetTiFlashProgressFromCache(tableID int64) (float64, bool) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		logutil.BgLogger().Error("GetTiFlashProgressFromCache get info sync failed", zap.Int64("tableID", tableID), zap.Error(err))
		return 0, false
	}
	return is.tiflashReplicaManager.GetTiFlashProgressFromCache(tableID)
}

// CleanTiFlashProgressCache clean progress cache
func CleanTiFlashProgressCache() {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return
	}
	is.tiflashReplicaManager.CleanTiFlashProgressCache()
}

// SetTiFlashGroupConfig is a helper function to set tiflash rule group config
func SetTiFlashGroupConfig(ctx context.Context) error {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return errors.Trace(err)
	}
	logutil.BgLogger().Info("SetTiFlashGroupConfig")
	return is.tiflashReplicaManager.SetTiFlashGroupConfig(ctx)
}

// SetTiFlashPlacementRule is a helper function to set placement rule.
// It is discouraged to use SetTiFlashPlacementRule directly,
// use `ConfigureTiFlashPDForTable`/`ConfigureTiFlashPDForPartitions` instead.
func SetTiFlashPlacementRule(ctx context.Context, rule pdhttp.Rule) error {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return errors.Trace(err)
	}
	logutil.BgLogger().Info("SetTiFlashPlacementRule", zap.String("ruleID", rule.ID))
	return is.tiflashReplicaManager.SetPlacementRule(ctx, &rule)
}

// DeleteTiFlashPlacementRule is to delete placement rule for certain group.
func DeleteTiFlashPlacementRule(ctx context.Context, group string, ruleID string) error {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return errors.Trace(err)
	}
	logutil.BgLogger().Info("DeleteTiFlashPlacementRule", zap.String("ruleID", ruleID))
	return is.tiflashReplicaManager.DeletePlacementRule(ctx, group, ruleID)
}

// GetTiFlashGroupRules to get all placement rule in a certain group.
func GetTiFlashGroupRules(ctx context.Context, group string) ([]*pdhttp.Rule, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return is.tiflashReplicaManager.GetGroupRules(ctx, group)
}

// GetTiFlashRegionCountFromPD is a helper function calling `/stats/region`.
func GetTiFlashRegionCountFromPD(ctx context.Context, tableID int64, regionCount *int) error {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return errors.Trace(err)
	}
	return is.tiflashReplicaManager.GetRegionCountFromPD(ctx, tableID, regionCount)
}

// GetTiFlashStoresStat gets the TiKV store information by accessing PD's api.
func GetTiFlashStoresStat(ctx context.Context) (*pdhttp.StoresInfo, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return is.tiflashReplicaManager.GetStoresStat(ctx)
}

// CloseTiFlashManager closes TiFlash manager.
func CloseTiFlashManager(ctx context.Context) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return
	}
	is.tiflashReplicaManager.Close(ctx)
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
	if e := is.tiflashReplicaManager.SetPlacementRule(ctx, &ruleNew); e != nil {
		return errors.Trace(e)
	}
	return nil
}

// ConfigureTiFlashPDForPartitions configures pd rule for all partition in partitioned tables.
func ConfigureTiFlashPDForPartitions(accel bool, definitions *[]model.PartitionDefinition, count uint64, locationLabels *[]string, tableID int64) error {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return errors.Trace(err)
	}
	ctx := context.Background()
	rules := make([]*pdhttp.Rule, 0, len(*definitions))
	pids := make([]int64, 0, len(*definitions))
	for _, p := range *definitions {
		logutil.BgLogger().Info("ConfigureTiFlashPDForPartitions", zap.Int64("tableID", tableID), zap.Int64("partID", p.ID), zap.Bool("accel", accel), zap.Uint64("count", count))
		ruleNew := MakeNewRule(p.ID, count, *locationLabels)
		rules = append(rules, &ruleNew)
		pids = append(pids, p.ID)
	}
	if e := is.tiflashReplicaManager.SetPlacementRuleBatch(ctx, rules); e != nil {
		return errors.Trace(e)
	}
	if accel {
		if e := is.tiflashReplicaManager.PostAccelerateScheduleBatch(ctx, pids); e != nil {
			return errors.Trace(e)
		}
	}
	return nil
}

// StoreInternalSession is the entry function for store an internal session to SessionManager.
// return whether the session is stored successfully.
func StoreInternalSession(se any) bool {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return false
	}
	sm := is.GetSessionManager()
	if sm == nil {
		return false
	}
	sm.StoreInternalSession(se)
	return true
}

// DeleteInternalSession is the entry function for delete an internal session from SessionManager.
func DeleteInternalSession(se any) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return
	}
	sm := is.GetSessionManager()
	if sm == nil {
		return
	}
	sm.DeleteInternalSession(se)
}

// SetEtcdClient is only used for test.
func SetEtcdClient(etcdCli *clientv3.Client) {
	is, err := getGlobalInfoSyncer()

	if err != nil {
		return
	}
	is.etcdCli = etcdCli
}

// GetEtcdClient is only used for test.
func GetEtcdClient() *clientv3.Client {
	is, err := getGlobalInfoSyncer()

	if err != nil {
		return nil
	}
	return is.etcdCli
}

// GetPDScheduleConfig gets the schedule information from pd
func GetPDScheduleConfig(ctx context.Context) (map[string]any, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return is.scheduleManager.GetScheduleConfig(ctx)
}

// SetPDScheduleConfig sets the schedule information for pd
func SetPDScheduleConfig(ctx context.Context, config map[string]any) error {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return errors.Trace(err)
	}
	return is.scheduleManager.SetScheduleConfig(ctx, config)
}

// TiProxyServerInfo is the server info for TiProxy.
type TiProxyServerInfo struct {
	Version        string `json:"version"`
	GitHash        string `json:"git_hash"`
	IP             string `json:"ip"`
	Port           string `json:"port"`
	StatusPort     string `json:"status_port"`
	StartTimestamp int64  `json:"start_timestamp"`
}

// GetTiProxyServerInfo gets all TiProxy servers information from etcd.
func GetTiProxyServerInfo(ctx context.Context) (map[string]*TiProxyServerInfo, error) {
	failpoint.Inject("mockGetTiProxyServerInfo", func(val failpoint.Value) {
		res := make(map[string]*TiProxyServerInfo)
		err := json.Unmarshal([]byte(val.(string)), &res)
		failpoint.Return(res, err)
	})
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, err
	}
	return is.getTiProxyServerInfo(ctx)
}

func (is *InfoSyncer) getTiProxyServerInfo(ctx context.Context) (map[string]*TiProxyServerInfo, error) {
	// In test.
	if is.etcdCli == nil {
		return nil, nil
	}

	var err error
	var resp *clientv3.GetResponse
	allInfo := make(map[string]*TiProxyServerInfo)
	for i := 0; i < keyOpDefaultRetryCnt; i++ {
		if ctx.Err() != nil {
			return nil, errors.Trace(ctx.Err())
		}
		childCtx, cancel := context.WithTimeout(ctx, keyOpDefaultTimeout)
		resp, err = is.etcdCli.Get(childCtx, TopologyTiProxy, clientv3.WithPrefix())
		cancel()
		if err != nil {
			logutil.BgLogger().Info("get key failed", zap.String("key", TopologyTiProxy), zap.Error(err))
			time.Sleep(200 * time.Millisecond)
			continue
		}
		for _, kv := range resp.Kvs {
			key := string(kv.Key)
			if !strings.HasSuffix(key, infoSuffix) {
				continue
			}
			addr := key[len(TopologyTiProxy)+1 : len(key)-len(infoSuffix)]
			var info TiProxyServerInfo
			err = json.Unmarshal(kv.Value, &info)
			if err != nil {
				logutil.BgLogger().Info("unmarshal key failed", zap.String("key", key), zap.ByteString("value", kv.Value),
					zap.Error(err))
				return nil, errors.Trace(err)
			}
			allInfo[addr] = &info
		}
		return allInfo, nil
	}
	return nil, errors.Trace(err)
}

// TiCDCInfo is the server info for TiCDC.
type TiCDCInfo struct {
	ID             string `json:"id"`
	Address        string `json:"address"`
	Version        string `json:"version"`
	GitHash        string `json:"git-hash"`
	DeployPath     string `json:"deploy-path"`
	StartTimestamp int64  `json:"start-timestamp"`
	ClusterID      string `json:"cluster-id"`
}

// GetTiCDCServerInfo gets all TiCDC servers information from etcd.
func GetTiCDCServerInfo(ctx context.Context) ([]*TiCDCInfo, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, err
	}
	return is.getTiCDCServerInfo(ctx)
}

func (is *InfoSyncer) getTiCDCServerInfo(ctx context.Context) ([]*TiCDCInfo, error) {
	// In test.
	if is.etcdCli == nil {
		return nil, nil
	}

	var err error
	var resp *clientv3.GetResponse
	allInfo := make([]*TiCDCInfo, 0)
	for i := 0; i < keyOpDefaultRetryCnt; i++ {
		if ctx.Err() != nil {
			return nil, errors.Trace(ctx.Err())
		}
		childCtx, cancel := context.WithTimeout(ctx, keyOpDefaultTimeout)
		resp, err = is.etcdCli.Get(childCtx, TopologyTiCDC, clientv3.WithPrefix())
		cancel()
		if err != nil {
			logutil.BgLogger().Info("get key failed", zap.String("key", TopologyTiCDC), zap.Error(err))
			time.Sleep(200 * time.Millisecond)
			continue
		}
		for _, kv := range resp.Kvs {
			key := string(kv.Key)
			keyParts := strings.Split(key, "/")
			if len(keyParts) < 3 {
				logutil.BgLogger().Info("invalid ticdc key", zap.String("key", key))
				continue
			}
			clusterID := keyParts[1]

			var info TiCDCInfo
			err := json.Unmarshal(kv.Value, &info)
			if err != nil {
				logutil.BgLogger().Info("unmarshal key failed", zap.String("key", key), zap.ByteString("value", kv.Value),
					zap.Error(err))
				return nil, errors.Trace(err)
			}
			info.Version = strings.TrimPrefix(info.Version, "v")
			info.ClusterID = clusterID
			allInfo = append(allInfo, &info)
		}
		return allInfo, nil
	}
	return nil, errors.Trace(err)
}
