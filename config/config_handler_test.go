// Copyright 2020 PingCAP, Inc.
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

package config

import (
	"context"
	"fmt"
	"os"
	"path"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/configpb"
	"github.com/pingcap/pd/v4/client"
)

type mockPDConfigClient struct {
	status      *configpb.Status
	version     *configpb.Version
	confContent atomic.Value
	err         error
}

var mockPDConfigClient0 = new(mockPDConfigClient)
var newMockPDConfigClientErr error

func newMockPDConfigClient([]string, pd.SecurityOption) (pd.ConfigClient, error) {
	return mockPDConfigClient0, newMockPDConfigClientErr
}

func (mc *mockPDConfigClient) GetClusterID(ctx context.Context) uint64 {
	return 0
}

func (mc *mockPDConfigClient) Create(ctx context.Context, v *configpb.Version, component, componentID, config string) (*configpb.Status, *configpb.Version, string, error) {
	return mc.status, mc.version, mc.confContent.Load().(string), mc.err
}

func (mc *mockPDConfigClient) Get(ctx context.Context, v *configpb.Version, component, componentID string) (*configpb.Status, *configpb.Version, string, error) {
	return mc.status, mc.version, mc.confContent.Load().(string), mc.err
}

func (mc *mockPDConfigClient) Update(ctx context.Context, v *configpb.Version, kind *configpb.ConfigKind, entries []*configpb.ConfigEntry) (*configpb.Status, *configpb.Version, error) {
	return nil, nil, nil
}

func (mc *mockPDConfigClient) Delete(ctx context.Context, v *configpb.Version, kind *configpb.ConfigKind) (*configpb.Status, error) {
	return nil, nil
}

func (mc *mockPDConfigClient) Close() {}

func (s *testConfigSuite) TestConstantConfHandler(c *C) {
	conf := defaultConf
	conf.Store = "mock"
	confPath := path.Join(os.TempDir(), "test-conf-path.toml")
	ch, err := NewConfHandler(confPath, &conf, nil, nil)
	c.Assert(err, IsNil)
	_, ok := ch.(*constantConfHandler)
	c.Assert(ok, IsTrue)
	c.Assert(ch.GetConfig(), Equals, &conf)
}

func (s *testConfigSuite) TestPDConfHandler(c *C) {
	conf := defaultConf

	// wrong path
	confPath := path.Join(os.TempDir(), "test-conf-path.toml")
	conf.Store = "WRONGPATH"
	conf.Path = "WRONGPATH"
	_, err := newPDConfHandler(confPath, &conf, nil, newMockPDConfigClient)
	c.Assert(err, NotNil)

	// error when creating PD config client
	conf.Store = "tikv"
	conf.Path = "node1:2379"
	newMockPDConfigClientErr = fmt.Errorf("")
	_, err = newPDConfHandler(confPath, &conf, nil, newMockPDConfigClient)
	c.Assert(err, NotNil)

	// error when registering
	newMockPDConfigClientErr = nil
	mockPDConfigClient0.err = fmt.Errorf("")
	mockPDConfigClient0.confContent.Store("")
	ch, err := newPDConfHandler(confPath, &conf, nil, newMockPDConfigClient)
	c.Assert(err, IsNil) // the local config will be used
	ch.Close()

	// wrong response when registering
	mockPDConfigClient0.err = nil
	mockPDConfigClient0.status = &configpb.Status{Code: configpb.StatusCode_UNKNOWN}
	ch, err = newPDConfHandler(confPath, &conf, nil, newMockPDConfigClient)
	c.Assert(err, IsNil)
	ch.Close()

	// create client successfully
	mockPDConfigClient0.status.Code = configpb.StatusCode_WRONG_VERSION
	content, _ := encodeConfig(&conf)
	mockPDConfigClient0.confContent.Store(content)
	ch, err = newPDConfHandler(confPath, &conf, nil, newMockPDConfigClient)
	c.Assert(err, IsNil)
	ch.Close()

	// update log level
	tmCh := make(chan time.Time)
	tmAfter := func(d time.Duration) <-chan time.Time {
		return tmCh
	}
	cnt := 0
	var registerWg, reloadWg, reloadWg2 sync.WaitGroup
	registerWg.Add(1)
	reloadWg.Add(1)
	mockReloadFunc := func(oldConf, newConf *Config) {
		if cnt == 0 {
			registerWg.Done()
		} else if cnt == 1 {
			c.Assert(oldConf.Performance.MaxMemory, Equals, uint64(233))
			c.Assert(newConf.Performance.MaxMemory, Equals, uint64(123))
			reloadWg.Done()
		} else {
			reloadWg2.Done()
		}
		cnt++
	}

	conf.Performance.MaxMemory = 233
	content, _ = encodeConfig(&conf)
	mockPDConfigClient0.confContent.Store(content)
	ch, err = newPDConfHandler(confPath, &conf, mockReloadFunc, newMockPDConfigClient)
	c.Assert(err, IsNil)
	ch.timeAfter = tmAfter

	ch.Start()
	registerWg.Wait() // wait for register

	newConf := conf
	newConf.Performance.MaxMemory = 123
	newContent, _ := encodeConfig(&newConf)
	mockPDConfigClient0.confContent.Store(newContent)
	tmCh <- time.Now()
	reloadWg.Wait()

	tmCh <- time.Now() // wait for another reloading to ensure the global config has been updated
	reloadWg2.Wait()
	c.Assert(ch.GetConfig().Performance.MaxMemory, Equals, uint64(123))
	ch.Close()
}

func (s *testConfigSuite) TestEnableDynamicConfig(c *C) {
	conf := &defaultConf
	confPath := path.Join(os.TempDir(), "test-conf-path.toml")
	for _, store := range []string{"tikv", "mocktikv"} {
		for _, enable := range []bool{true, false} {
			conf.Store = store
			conf.EnableDynamicConfig = enable
			ch, err := NewConfHandler(confPath, conf, nil, newMockPDConfigClient)
			c.Assert(err, IsNil)
			if store == "tikv" && enable == true {
				c.Assert(fmt.Sprintf("%v", reflect.TypeOf(ch)), Equals, "*config.pdConfHandler")
			} else {
				c.Assert(fmt.Sprintf("%v", reflect.TypeOf(ch)), Equals, "*config.constantConfHandler")
			}
		}
	}
}

func (s *testConfigSuite) TestDynamicConfigItems(c *C) {
	tmCh := make(chan time.Time)
	tmAfter := func(d time.Duration) <-chan time.Time {
		return tmCh
	}
	initConf, err := CloneConf(&defaultConf)
	c.Assert(err, IsNil)
	initConf.Store = "tikv"
	initConf.Path = "node1:2379"
	newMockPDConfigClientErr = nil
	initContent, err := encodeConfig(initConf)
	c.Assert(err, IsNil)
	mockPDConfigClient0.err = nil
	mockPDConfigClient0.confContent.Store(initContent)
	mockPDConfigClient0.status = &configpb.Status{Code: configpb.StatusCode_WRONG_VERSION}

	cnt := 0
	var registerWg, reloadWg sync.WaitGroup
	registerWg.Add(1)
	reloadWg.Add(1)
	mockReloadFunc := func(oldConf, newConf *Config) {
		if cnt == 0 { // register
			newContent, err := encodeConfig(newConf)
			c.Assert(err, IsNil)
			c.Assert(newContent, Equals, initContent) // no change now
			registerWg.Done()
		} else if cnt == 1 {
			c.Assert(newConf.Performance.MaxProcs, Equals, uint(2333))
			c.Assert(newConf.Performance.MaxMemory, Equals, uint64(2333))
			c.Assert(newConf.Performance.CrossJoin, Equals, false)
			c.Assert(newConf.Performance.FeedbackProbability, Equals, 0.2333)
			c.Assert(newConf.Performance.QueryFeedbackLimit, Equals, uint(2333))
			c.Assert(newConf.Performance.PseudoEstimateRatio, Equals, 0.2333)
			c.Assert(newConf.Performance.StmtCountLimit, Equals, uint(2333))
			c.Assert(newConf.Performance.TCPKeepAlive, Equals, true)
			c.Assert(newConf.OOMAction, Equals, "cancel")
			c.Assert(newConf.MemQuotaQuery, Equals, int64(2333))
			c.Assert(newConf.TiKVClient.StoreLimit, Equals, int64(2333))
			c.Assert(newConf.Log.Level, Equals, "error")
			c.Assert(newConf.Log.SlowThreshold, Equals, uint64(2333))
			c.Assert(newConf.Log.QueryLogMaxLen, Equals, uint64(2333))
			c.Assert(newConf.Log.ExpensiveThreshold, Equals, uint(2333))
			c.Assert(newConf.CheckMb4ValueInUTF8, Equals, false)
			c.Assert(newConf.EnableStreaming, Equals, true)
			c.Assert(newConf.TxnLocalLatches.Capacity, Equals, uint(2333))
			c.Assert(newConf.PreparedPlanCache.Enabled, Equals, true)
			c.Assert(newConf.CompatibleKillQuery, Equals, true)
			c.Assert(newConf.TreatOldVersionUTF8AsUTF8MB4, Equals, true)
			c.Assert(newConf.OpenTracing.Enable, Equals, true)
			reloadWg.Done()
		}
		cnt++
	}
	ch, err := newPDConfHandler("", initConf, mockReloadFunc, newMockPDConfigClient)
	c.Assert(err, IsNil)
	ch.timeAfter = tmAfter

	ch.Start()
	registerWg.Wait() // wait for register

	// test all dynamic config items
	newConf, err := CloneConf(&defaultConf)
	c.Assert(err, IsNil)
	newConf.Performance.MaxProcs = 2333
	newConf.Performance.MaxMemory = 2333
	newConf.Performance.CrossJoin = false
	newConf.Performance.FeedbackProbability = 0.2333
	newConf.Performance.QueryFeedbackLimit = 2333
	newConf.Performance.PseudoEstimateRatio = 0.2333
	newConf.Performance.StmtCountLimit = 2333
	newConf.Performance.TCPKeepAlive = true
	newConf.OOMAction = "cancel"
	newConf.MemQuotaQuery = 2333
	newConf.TiKVClient.StoreLimit = 2333
	newConf.Log.Level = "error"
	newConf.Log.SlowThreshold = 2333
	newConf.Log.QueryLogMaxLen = 2333
	newConf.Log.ExpensiveThreshold = 2333
	newConf.CheckMb4ValueInUTF8 = false
	newConf.EnableStreaming = true
	newConf.TxnLocalLatches.Capacity = 2333
	newConf.PreparedPlanCache.Enabled = true
	newConf.CompatibleKillQuery = true
	newConf.TreatOldVersionUTF8AsUTF8MB4 = true
	newConf.OpenTracing.Enable = true
	newContent, err := encodeConfig(newConf)
	c.Assert(err, IsNil)
	mockPDConfigClient0.confContent.Store(newContent)
	tmCh <- time.Now()
	reloadWg.Wait()
	ch.Close()
}
