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
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/configpb"
	"github.com/pingcap/pd/client"
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
	ch, err := NewConfHandler(&conf, nil, nil)
	c.Assert(err, IsNil)
	_, ok := ch.(*constantConfHandler)
	c.Assert(ok, IsTrue)
	c.Assert(ch.GetConfig(), Equals, &conf)
}

func (s *testConfigSuite) TestPDConfHandler(c *C) {
	conf := defaultConf

	// wrong path
	conf.Store = "WRONGPATH"
	conf.Path = "WRONGPATH"
	_, err := newPDConfHandler(&conf, nil, newMockPDConfigClient)
	c.Assert(err, NotNil)

	// error when creating PD config client
	conf.Store = "tikv"
	conf.Path = "node1:2379"
	newMockPDConfigClientErr = fmt.Errorf("")
	_, err = newPDConfHandler(&conf, nil, newMockPDConfigClient)
	c.Assert(err, NotNil)

	// error when registering
	newMockPDConfigClientErr = nil
	mockPDConfigClient0.err = fmt.Errorf("")
	mockPDConfigClient0.confContent.Store("")
	ch, err := newPDConfHandler(&conf, nil, newMockPDConfigClient)
	c.Assert(err, IsNil) // the local config will be used
	ch.Close()

	// wrong response when registering
	mockPDConfigClient0.err = nil
	mockPDConfigClient0.status = &configpb.Status{Code: configpb.StatusCode_UNKNOWN}
	ch, err = newPDConfHandler(&conf, nil, newMockPDConfigClient)
	c.Assert(err, IsNil)
	ch.Close()

	// create client successfully
	mockPDConfigClient0.status.Code = configpb.StatusCode_WRONG_VERSION
	content, _ := encodeConfig(&conf)
	mockPDConfigClient0.confContent.Store(content)
	ch, err = newPDConfHandler(&conf, nil, newMockPDConfigClient)
	c.Assert(err, IsNil)
	ch.Close()

	// update log level
	wg := sync.WaitGroup{}
	wg.Add(1)
	mockReloadFunc := func(oldConf, newConf *Config) {
		c.Assert(oldConf.Performance.MaxMemory, Equals, uint64(233))
		c.Assert(newConf.Performance.MaxMemory, Equals, uint64(123))
		wg.Done()
	}
	conf.Performance.MaxMemory = 233
	ch, err = newPDConfHandler(&conf, mockReloadFunc, newMockPDConfigClient)
	c.Assert(err, IsNil)
	ch.interval = time.Second
	newConf := conf
	newConf.Performance.MaxMemory = 123
	newContent, _ := encodeConfig(&newConf)
	mockPDConfigClient0.confContent.Store(newContent)
	ch.Start()
	wg.Wait()
	c.Assert(ch.GetConfig().Performance.MaxMemory, Equals, uint64(123))
	ch.Close()
}

func (s *testConfigSuite) TestEnableDynamicConfig(c *C) {
	conf := &defaultConf
	for _, store := range []string{"tikv", "mocktikv"} {
		for _, enable := range []bool{true, false} {
			conf.Store = store
			conf.EnableDynamicConfig = enable
			ch, err := NewConfHandler(conf, nil, newMockPDConfigClient)
			c.Assert(err, IsNil)
			if store == "tikv" && enable == true {
				c.Assert(fmt.Sprintf("%v", reflect.TypeOf(ch)), Equals, "*config.pdConfHandler")
			} else {
				c.Assert(fmt.Sprintf("%v", reflect.TypeOf(ch)), Equals, "*config.constantConfHandler")
			}
		}
	}
}
