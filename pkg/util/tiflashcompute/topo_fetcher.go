// Copyright 2023 PingCAP, Inc.
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

package tiflashcompute

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var globalTopoFetcher TopoFetcher
var _ TopoFetcher = &MockTopoFetcher{}
var _ TopoFetcher = &AWSTopoFetcher{}
var _ TopoFetcher = &TestTopoFetcher{}

const (
	// MockASStr is string value for mock AutoScaler.
	MockASStr = "mock"
	// AWSASStr is string value for aws AutoScaler.
	AWSASStr = "aws"
	// GCPASStr is string value for gcp AutoScaler.
	GCPASStr = "gcp"
	// TestASStr is string value for test AutoScaler.
	TestASStr = "test"
	// InvalidASStr is string value for invalid AutoScaler.
	InvalidASStr = "invalid"
)

const (
	// MockASType is int value for mock AutoScaler.
	MockASType int = iota
	// AWSASType is int value for aws AutoScaler.
	AWSASType
	// GCPASType is int value for gcp AutoScaler.
	GCPASType
	// TestASType is for local tidb test AutoScaler.
	TestASType
	// InvalidASType is int value for invalid check.
	InvalidASType
)

const (
	// DefAWSAutoScalerAddr is the default address for AWS AutoScaler.
	DefAWSAutoScalerAddr = "tiflash-autoscale-lb.tiflash-autoscale.svc.cluster.local:8081"
	// DefASStr default AutoScaler.
	DefASStr = AWSASStr

	awsFixedPoolHTTPPath    = "sharedfixedpool"
	awsFetchHTTPPath        = "resume-and-get-topology"
	httpGetFailedErrMsg     = "get tiflash_compute topology failed"
	parseTopoTSFailedErrMsg = "parse timestamp of tiflash_compute topology failed"
)

var errTopoFetcher = dbterror.ClassUtil.NewStd(errno.ErrInternal)

// RecoveryType is for MPPErrRecovery.
type RecoveryType uint32

const (
	// RecoveryTypeNull means no need to recovery error.
	RecoveryTypeNull RecoveryType = iota
	// RecoveryTypeMemLimit means need to recovery MemLimit error.
	RecoveryTypeMemLimit
)

func (r *RecoveryType) toString() (string, error) {
	if *r == RecoveryTypeNull {
		return "Null", nil
	} else if *r == RecoveryTypeMemLimit {
		return "MemLimit", nil
	}
	return "", errors.New("unsupported recovery type for topo_fetcher")
}

// TopoFetcher is interface for fetching topo from AutoScaler.
// We support the following kinds of AutoScaler for now:
//  1. MockAutoScaler: Normally for test, can run in local environment.
//  2. AWSAutoScaler: AutoScaler runs on AWS.
//  3. GCPAutoScaler: AutoScaler runs on GCP.
//  4. TestAutoScaler: AutoScaler just for unit test.
type TopoFetcher interface {
	// Always fetch topo from AutoScaler, then return topo.
	// If topo is empty, will not return error.
	FetchAndGetTopo() ([]string, error)

	// Try recovery error then fetch new topo.
	RecoveryAndGetTopo(recovery RecoveryType, oriCNCnt int) ([]string, error)
}

// IsValidAutoScalerConfig return true if user config of autoscaler type is valid.
func IsValidAutoScalerConfig(typ string) bool {
	t := getAutoScalerType(typ)
	return t == MockASType || t == AWSASType || t == GCPASType
}

// getAutoScalerType return topo fetcher type.
func getAutoScalerType(typ string) int {
	switch typ {
	case MockASStr:
		return MockASType
	case AWSASStr:
		return AWSASType
	case GCPASStr:
		return GCPASType
	case TestASStr:
		return TestASType
	default:
		return InvalidASType
	}
}

// InitGlobalTopoFetcher init globalTopoFetcher if is in disaggregated-tiflash mode. It's not thread-safe.
func InitGlobalTopoFetcher(typ string, addr string, clusterID string, isFixedPool bool) (err error) {
	logutil.BgLogger().Info("init globalTopoFetcher", zap.Any("type", typ), zap.Any("addr", addr),
		zap.Any("clusterID", clusterID), zap.Any("isFixedPool", isFixedPool))
	if clusterID == "" || addr == "" {
		return errors.Errorf("ClusterID(%s) or AutoScaler(%s) addr is empty", clusterID, addr)
	}

	ft := getAutoScalerType(typ)
	switch ft {
	case MockASType:
		globalTopoFetcher = NewMockAutoScalerFetcher(addr)
	case AWSASType:
		globalTopoFetcher = NewAWSAutoScalerFetcher(addr, clusterID, isFixedPool)
	case GCPASType:
		err = errors.Errorf("topo fetch not implemented yet(%s)", typ)
	case TestASType:
		globalTopoFetcher = NewTestAutoScalerFetcher()
	default:
		globalTopoFetcher = nil
		err = errors.Errorf("unexpected topo fetch type. expect: %s or %s or %s, got %s",
			MockASStr, AWSASStr, GCPASStr, typ)
	}
	return err
}

// GetGlobalTopoFetcher return global topo fetcher, not thread safe.
func GetGlobalTopoFetcher() TopoFetcher {
	return globalTopoFetcher
}

// MockTopoFetcher will fetch topo from MockAutoScaler.
// MockScaler can run in local environment.
type MockTopoFetcher struct {
	mu struct {
		sync.RWMutex
		topo []string
	}
	// Mock AutoScaler addr.
	addr string
}

// NewMockAutoScalerFetcher create a new MockTopoFetcher.
func NewMockAutoScalerFetcher(addr string) *MockTopoFetcher {
	f := &MockTopoFetcher{}
	f.mu.topo = make([]string, 0, 8)
	f.addr = addr
	return f
}

// FetchAndGetTopo implements TopoFetcher interface.
func (f *MockTopoFetcher) FetchAndGetTopo() ([]string, error) {
	err := f.fetchTopo()
	if err != nil {
		return nil, err
	}

	curTopo := f.getTopo()
	logutil.BgLogger().Debug("FetchAndGetTopo", zap.Any("topo", curTopo))
	return curTopo, nil
}

// RecoveryAndGetTopo implements TopoFetcher interface.
func (*MockTopoFetcher) RecoveryAndGetTopo(RecoveryType, int) ([]string, error) {
	return nil, errors.New("RecoveryAndGetTopo not implemented")
}

// getTopo return the cached topo.
func (f *MockTopoFetcher) getTopo() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.mu.topo
}

// assureTopo will make sure topo is greater than nodeNum.
func (f *MockTopoFetcher) assureTopo(nodeNum int) error {
	para := url.Values{}
	para.Add("node_num", strconv.Itoa(nodeNum))
	u := url.URL{
		Scheme:   "http",
		Host:     f.addr,
		Path:     "/assume-and-get-topo",
		RawQuery: para.Encode(),
	}
	url := u.String()
	logutil.BgLogger().Info("assureTopo", zap.Any("url", url))

	newTopo, err := mockHTTPGetAndParseResp(url)
	if err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.mu.topo = newTopo
	return nil
}

// fetchTopo will fetch newest topo from mock autoscaler.
func (f *MockTopoFetcher) fetchTopo() error {
	u := url.URL{
		Scheme: "http",
		Host:   f.addr,
		Path:   "/fetch_topo",
	}
	url := u.String()
	logutil.BgLogger().Info("fetchTopo", zap.Any("url", url))

	newTopo, err := mockHTTPGetAndParseResp(url)
	if err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.mu.topo = newTopo
	return nil
}

func httpGetAndParseResp(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		logutil.BgLogger().Error(err.Error())
		return nil, errTopoFetcher.GenWithStackByArgs(httpGetFailedErrMsg)
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		logutil.BgLogger().Error(err.Error())
		return nil, errTopoFetcher.GenWithStackByArgs(httpGetFailedErrMsg)
	}
	bStr := string(b)
	if resp.StatusCode != http.StatusOK {
		logutil.BgLogger().Error("http get mock AutoScaler failed", zap.Any("url", url),
			zap.Any("status code", http.StatusText(resp.StatusCode)),
			zap.Any("http body", bStr))
		return nil, errTopoFetcher.GenWithStackByArgs(httpGetFailedErrMsg)
	}
	return b, nil
}

// mockHTTPGetAndParseResp send http get request and parse topo to []string.
func mockHTTPGetAndParseResp(url string) ([]string, error) {
	b, err := httpGetAndParseResp(url)
	if err != nil {
		return nil, err
	}

	// For MockAutoScaler, topo is like: ip:port;ip:port.
	bStr := string(b)
	newTopo := strings.Split(bStr, ";")
	if len(bStr) == 0 || len(newTopo) == 0 {
		return nil, errors.New("topo list is empty")
	}
	logutil.BgLogger().Debug("httpGetAndParseResp succeed", zap.Any("new topo", newTopo))
	return newTopo, nil
}

// AWSTopoFetcher will fetch topo from AWSAutoScaler.
type AWSTopoFetcher struct {
	mu struct {
		sync.RWMutex
		topo   []string
		topoTS int64
	}
	// AWS AutoScaler addr.
	// These should be init when TiDB start, all single threaded, no need to lock.
	addr        string
	clusterID   string
	isFixedPool bool
}

type resumeAndGetTopologyResult struct {
	HasError  int      `json:"hasError"`
	ErrorInfo string   `json:"errorInfo"`
	State     string   `json:"state"`
	Topology  []string `json:"topology"`
	Timestamp string   `json:"timestamp"`
}

// NewAWSAutoScalerFetcher create a new AWSTopoFetcher.
func NewAWSAutoScalerFetcher(addr string, clusterID string, isFixed bool) *AWSTopoFetcher {
	f := &AWSTopoFetcher{}
	f.mu.topo = make([]string, 0, 8)
	f.mu.topoTS = -1
	f.addr = addr
	f.clusterID = clusterID
	f.isFixedPool = isFixed
	return f
}

// FetchAndGetTopo implements TopoFetcher interface.
func (f *AWSTopoFetcher) FetchAndGetTopo() (curTopo []string, err error) {
	return f.fetchAndGetTopo(RecoveryTypeNull, 0)
}

// RecoveryAndGetTopo implements TopoFetcher interface.
func (f *AWSTopoFetcher) RecoveryAndGetTopo(recovery RecoveryType, oriCNCnt int) (curTopo []string, err error) {
	return f.fetchAndGetTopo(recovery, oriCNCnt)
}

func (f *AWSTopoFetcher) fetchAndGetTopo(recovery RecoveryType, oriCNCnt int) (curTopo []string, err error) {
	defer func() {
		logutil.BgLogger().Info("AWSTopoFetcher FetchAndGetTopo done", zap.Any("curTopo", curTopo))
	}()

	if recovery != RecoveryTypeNull && recovery != RecoveryTypeMemLimit {
		return nil, errors.Errorf("topo_fetcher cannot handle error: %v", recovery)
	}

	if recovery == RecoveryTypeMemLimit && oriCNCnt == 0 {
		return nil, errors.New("ori CN count should not be zero")
	}

	if f.isFixedPool {
		curTopo, _ = f.getTopo()
		if len(curTopo) != 0 {
			return curTopo, nil
		}

		if err = f.fetchFixedPoolTopo(); err != nil {
			return nil, err
		}
		curTopo, _ = f.getTopo()
		return curTopo, nil
	}

	if err = f.fetchTopo(recovery, oriCNCnt); err != nil {
		return nil, err
	}

	curTopo, _ = f.getTopo()
	return curTopo, nil
}

func awsHTTPGetAndParseResp(url string) (*resumeAndGetTopologyResult, error) {
	b, err := httpGetAndParseResp(url)
	if err != nil {
		return nil, err
	}

	res := &resumeAndGetTopologyResult{}
	if err = json.Unmarshal(b, &res); err != nil {
		logutil.BgLogger().Error(err.Error())
		return nil, errTopoFetcher.GenWithStackByArgs(httpGetFailedErrMsg)
	}

	logutil.BgLogger().Info("awsHTTPGetAndParseResp succeed", zap.Any("resp", res))
	return res, nil
}

func (f *AWSTopoFetcher) tryUpdateTopo(newTopo *resumeAndGetTopologyResult) (updated bool, err error) {
	cachedTopo, cachedTS := f.getTopo()
	newTS, err := strconv.ParseInt(newTopo.Timestamp, 10, 64)
	defer func() {
		logutil.BgLogger().Info("try update topo", zap.Any("updated", updated), zap.Any("err", err),
			zap.Any("cached TS", cachedTS), zap.Any("cached Topo", cachedTopo),
			zap.Any("fetch TS", newTopo.Timestamp), zap.Any("converted TS", newTS), zap.Any("fetch topo", newTopo.Topology))
	}()
	if err != nil {
		return updated, errTopoFetcher.GenWithStackByArgs(parseTopoTSFailedErrMsg)
	}

	if cachedTS >= newTS {
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	cachedTS = f.mu.topoTS
	if cachedTS > newTS {
		return
	}
	updated = true
	f.mu.topo = newTopo.Topology
	f.mu.topoTS = newTS
	return
}

func (f *AWSTopoFetcher) fetchFixedPoolTopo() error {
	u := url.URL{
		Scheme: "http",
		Host:   f.addr,
		Path:   awsFixedPoolHTTPPath,
	}
	url := u.String()
	logutil.BgLogger().Info("fetchFixedPoolTopo", zap.Any("url", url))

	newTopo, err := awsHTTPGetAndParseResp(url)
	if err != nil {
		return err
	}

	_, err = f.tryUpdateTopo(newTopo)
	if err != nil {
		return err
	}
	return nil
}

func (f *AWSTopoFetcher) fetchTopo(recovery RecoveryType, oriCNCnt int) error {
	para := url.Values{}
	para.Add("tidbclusterid", f.clusterID)

	if recovery == RecoveryTypeMemLimit {
		msg, err := recovery.toString()
		if err != nil {
			return err
		}
		para.Add("recovery", msg)
		para.Add("cn_cnt", strconv.Itoa(oriCNCnt))
	}

	u := url.URL{
		Scheme:   "http",
		Host:     f.addr,
		Path:     awsFetchHTTPPath,
		RawQuery: para.Encode(),
	}
	url := u.String()
	logutil.BgLogger().Info("fetchTopo", zap.Any("url", url))

	newTopo, err := awsHTTPGetAndParseResp(url)
	if err != nil {
		return err
	}

	_, err = f.tryUpdateTopo(newTopo)
	if err != nil {
		return err
	}
	return nil
}

func (f *AWSTopoFetcher) getTopo() ([]string, int64) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.mu.topo, f.mu.topoTS
}

// TestTopoFetcher will return empty topo list, just for unit test.
type TestTopoFetcher struct{}

// NewTestAutoScalerFetcher returns TestTopoFetcher.
func NewTestAutoScalerFetcher() *TestTopoFetcher {
	return &TestTopoFetcher{}
}

// FetchAndGetTopo implements TopoFetcher interface.
func (*TestTopoFetcher) FetchAndGetTopo() ([]string, error) {
	return []string{}, nil
}

// RecoveryAndGetTopo implements TopoFetcher interface.
func (*TestTopoFetcher) RecoveryAndGetTopo(RecoveryType, int) ([]string, error) {
	return nil, errors.New("RecoveryAndGetTopo not implemented")
}
