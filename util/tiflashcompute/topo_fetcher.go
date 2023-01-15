// Copyright 2015 PingCAP, Inc.
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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

var globalTopoFetcher TopoFetcher
var _ TopoFetcher = &MockTopoFetcher{}

const (
	// MockASStr is String value for mock AutoScaler.
	MockASStr = "mock"
	// AWSASStr is String value for mock AutoScaler.
	AWSASStr = "aws"
	// GCPASStr is String value for mock AutoScaler.
	GCPASStr = "gcp"
)

const (
	// MockASType is int value for mock AutoScaler.
	MockASType int = iota
	// AWSASType is int value for mock AutoScaler.
	AWSASType
	// GCPASType is int value for mock AutoScaler.
	GCPASType
	// InvalidASType is int value for invalid check.
	InvalidASType
)

// TopoFetcher is interface for fetching topo from AutoScaler.
// There are three kinds of AutoScaler for now:
//  1. MockAutoScaler: Normally for test, can run in local environment.
//  2. AWSAutoScaler: AutoScaler runs on AWS.
//  3. GCPAutoScaler: AutoScaler runs on GCP.
type TopoFetcher interface {
	// Return tiflash compute topo cache, if topo is empty, will fetch topo from AutoScaler.
	// If topo is empty after fetch, will return error.
	AssureAndGetTopo() ([]string, error)

	// Always fetch topo from AutoScaler, then return topo.
	// If topo is empty, will not return error. You can call AssureAndGetTopo() to make sure topo is not empty.
	FetchAndGetTopo() ([]string, error)
}

// GetAutoScalerType return topo fetcher type.
func GetAutoScalerType(typ string) int {
	switch typ {
	case MockASStr:
		return MockASType
	case AWSASStr:
		return AWSASType
	case GCPASStr:
		return GCPASType
	default:
		return InvalidASType
	}
}

// InitGlobalTopoFetcher init globalTopoFetcher if is in disaggregated-tiflash mode. It's not thread-safe.
func InitGlobalTopoFetcher(typ string, addr string) error {
	if globalTopoFetcher != nil {
		return errors.New("globalTopoFetcher alread inited")
	}

	ft := GetAutoScalerType(typ)
	switch ft {
	case MockASType:
		globalTopoFetcher = NewMockAutoScalerFetcher(addr)
		return nil
	case AWSASType, GCPASType:
		return errors.Errorf("topo fetch not implemented yet(%s)", typ)
	}
	return errors.Errorf("unexpected topo fetch type. expect: %s or %s or %s, got %s",
		MockASStr, AWSASStr, GCPASStr, typ)
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
		topo   []string
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

// AssureAndGetTopo implements TopoFetcher interface.
func (f *MockTopoFetcher) AssureAndGetTopo() ([]string, error) {
	curTopo := f.getTopo()

	if len(curTopo) == 0 {
		logutil.BgLogger().Info("tiflash compute topo is empty, updating")
		err := f.assureTopo(1)
		if err != nil {
			return nil, err
		}
	}

	curTopo = f.getTopo()
	logutil.BgLogger().Debug("AssureAndGetTopo", zap.Any("topo", curTopo))
	if len(curTopo) == 0 {
		return curTopo, errors.New("topo is still empty after updated from mock AutoScaler")
	}
	return curTopo, nil
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

	newTopo, err := httpGetAndParseResp(url)
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

	newTopo, err := httpGetAndParseResp(url)
	if err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.mu.topo = newTopo
	return nil
}

// httpGetAndParseResp send http get request and parse topo to []string.
func httpGetAndParseResp(url string) ([]string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bStr := string(b)
	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("http get mock AutoScaler failed. url: %s, status code: %s, http resp body: %s", url, http.StatusText(resp.StatusCode), bStr)
	}

	// For MockAutoScaler, topo is like: ip:port;ip:port.
	newTopo := strings.Split(bStr, ";")
	if len(bStr) == 0 || len(newTopo) == 0 {
		return nil, errors.New("topo list is empty")
	}
	logutil.BgLogger().Debug("assureTopo succeed", zap.Any("new topo", newTopo))
	return newTopo, nil
}

type AWSTopoFetcher struct {
	mu struct {
		sync.RWMutex
		topo []string
		topoTS int64
	}
	// AWS AutoScaler addr.
	addr string
	IsFixedPool bool
}

func NewAWSAutoScalerFetcher(addr string) *AWSTopoFetcher {
	f := &AWSTopoFetcher{}
	f.mu.topo = make([]string, 0, 8)
	f.mu.topoTS = -1
	f.addr = addr
	return f
}

func (f *AWSTopoFetcher) AssureAndGetTopo() ([]string, error) {
	return nil, nil
}

func (f *AWSTopoFetcher) FetchAndGetTopo() ([]string, error) {
	return nil, nil
}
