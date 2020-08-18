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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"

	"github.com/BurntSushi/toml"
	. "github.com/pingcap/check"
)

func (s *testConfigSuite) TestCloneConf(c *C) {
	c1, err := CloneConf(&defaultConf)
	c.Assert(err, IsNil)
	c2, err := CloneConf(c1)
	c.Assert(err, IsNil)
	c.Assert(reflect.DeepEqual(c1, c2), IsTrue)

	c1.Store = "abc"
	c1.Port = 2333
	c1.Log.EnableSlowLog = !c1.Log.EnableSlowLog
	c1.RepairTableList = append(c1.RepairTableList, "abc")
	c.Assert(c1.Store, Not(Equals), c2.Store)
	c.Assert(c1.Port, Not(Equals), c2.Port)
	c.Assert(c1.Log.EnableSlowLog, Not(Equals), c2.Log.EnableSlowLog)
	c.Assert(fmt.Sprintf("%v", c1.RepairTableList), Not(Equals), fmt.Sprintf("%v", c2.RepairTableList))
}

func (s *testConfigSuite) TestMergeConfigItems(c *C) {
	oriConf, _ := CloneConf(&defaultConf)
	oldConf, _ := CloneConf(oriConf)
	newConf, _ := CloneConf(oldConf)

	// allowed
	newConf.Performance.MaxProcs = 123
	newConf.Performance.MaxMemory = 123
	newConf.Performance.CrossJoin = false
	newConf.Performance.FeedbackProbability = 123
	newConf.Performance.QueryFeedbackLimit = 123
	newConf.Performance.PseudoEstimateRatio = 123
	newConf.OOMAction = "panic"
	newConf.MemQuotaQuery = 123
	newConf.MemQuotaStatistic = 123
	newConf.TiKVClient.StoreLimit = 123

	// rejected
	newConf.Store = "tiflash"
	newConf.Port = 2333
	newConf.AdvertiseAddress = "1.2.3.4"
	newConf.Log.SlowThreshold = 2345

	as, rs := MergeConfigItems(oldConf, newConf)
	c.Assert(len(as), Equals, 10)
	c.Assert(len(rs), Equals, 4)
	for _, a := range as {
		_, ok := dynamicConfigItems[a]
		c.Assert(ok, IsTrue)
	}
	for _, a := range rs {
		_, ok := dynamicConfigItems[a]
		c.Assert(ok, IsFalse)
	}

	c.Assert(oldConf.Performance.MaxProcs, Equals, newConf.Performance.MaxProcs)
	c.Assert(oldConf.Performance.MaxMemory, Equals, newConf.Performance.MaxMemory)
	c.Assert(oldConf.Performance.CrossJoin, Equals, newConf.Performance.CrossJoin)
	c.Assert(oldConf.Performance.FeedbackProbability, Equals, newConf.Performance.FeedbackProbability)
	c.Assert(oldConf.Performance.QueryFeedbackLimit, Equals, newConf.Performance.QueryFeedbackLimit)
	c.Assert(oldConf.Performance.PseudoEstimateRatio, Equals, newConf.Performance.PseudoEstimateRatio)
	c.Assert(oldConf.OOMAction, Equals, newConf.OOMAction)
	c.Assert(oldConf.MemQuotaQuery, Equals, newConf.MemQuotaQuery)
	c.Assert(oldConf.TiKVClient.StoreLimit, Equals, newConf.TiKVClient.StoreLimit)
	c.Assert(oldConf.Log.SlowThreshold, Equals, newConf.Log.SlowThreshold)

	c.Assert(oldConf.Store, Equals, oriConf.Store)
	c.Assert(oldConf.Port, Equals, oriConf.Port)
	c.Assert(oldConf.AdvertiseAddress, Equals, oriConf.AdvertiseAddress)
}

func (s *testConfigSuite) TestAtomicWriteConfig(c *C) {
	conf, _ := CloneConf(&defaultConf)
	confPath := filepath.Join(os.TempDir(), "test-write-config.toml")
	conf.Performance.MaxMemory = 123
	conf.Performance.MaxProcs = 234
	conf.Performance.PseudoEstimateRatio = 3.45
	c.Assert(atomicWriteConfig(conf, confPath), IsNil)

	content, err := ioutil.ReadFile(confPath)
	c.Assert(err, IsNil)
	dconf, err := decodeConfig(string(content))
	c.Assert(err, IsNil)
	c.Assert(dconf.Performance.MaxMemory, Equals, uint64(123))
	c.Assert(dconf.Performance.MaxProcs, Equals, uint(234))
	c.Assert(dconf.Performance.PseudoEstimateRatio, Equals, 3.45)

	conf.Performance.MaxMemory = 321
	conf.Performance.MaxProcs = 432
	conf.Performance.PseudoEstimateRatio = 54.3
	c.Assert(atomicWriteConfig(conf, confPath), IsNil)

	content, err = ioutil.ReadFile(confPath)
	c.Assert(err, IsNil)
	dconf, err = decodeConfig(string(content))
	c.Assert(err, IsNil)
	c.Assert(dconf.Performance.MaxMemory, Equals, uint64(321))
	c.Assert(dconf.Performance.MaxProcs, Equals, uint(432))
	c.Assert(dconf.Performance.PseudoEstimateRatio, Equals, 54.3)
}

func (s *testConfigSuite) TestFlattenConfig(c *C) {
	toJSONStr := func(v interface{}) string {
		str, err := json.Marshal(v)
		c.Assert(err, IsNil)
		return string(str)
	}

	jsonConf := `{
	"k0": 233333,
	"k1": "v1",
	"k2": ["v2-1", "v2-2", "v2-3"],
	"k3": [{"k3-1":"v3-1"}, {"k3-2":"v3-2"}, {"k3-3":"v3-3"}],
	"k4": {
		"k4-1": [1, 2, 3, 4],
		"k4-2": [5, 6, 7, 8],
		"k4-3": [666]
	}}`
	nested := make(map[string]interface{})
	c.Assert(json.Unmarshal([]byte(jsonConf), &nested), IsNil)
	flatMap := FlattenConfigItems(nested)
	c.Assert(len(flatMap), Equals, 7)
	c.Assert(toJSONStr(flatMap["k0"]), Equals, "233333")
	c.Assert(flatMap["k1"], Equals, "v1")
	c.Assert(toJSONStr(flatMap["k2"]), Equals, `["v2-1","v2-2","v2-3"]`)
	c.Assert(toJSONStr(flatMap["k3"]), Equals, `[{"k3-1":"v3-1"},{"k3-2":"v3-2"},{"k3-3":"v3-3"}]`)
	c.Assert(toJSONStr(flatMap["k4.k4-1"]), Equals, `[1,2,3,4]`)
	c.Assert(toJSONStr(flatMap["k4.k4-2"]), Equals, `[5,6,7,8]`)
	c.Assert(toJSONStr(flatMap["k4.k4-3"]), Equals, `[666]`)

	tomlConf := `
port=4000
[log]
level='info'
format='text'
[isolation-read]
engines = ["tikv", "tiflash", "tidb"]
`
	nested = make(map[string]interface{})
	c.Assert(toml.Unmarshal([]byte(tomlConf), &nested), IsNil)
	flatMap = FlattenConfigItems(nested)
	c.Assert(len(flatMap), Equals, 4)
	c.Assert(toJSONStr(flatMap["port"]), Equals, "4000")
	c.Assert(toJSONStr(flatMap["log.level"]), Equals, `"info"`)
	c.Assert(toJSONStr(flatMap["log.format"]), Equals, `"text"`)
	c.Assert(toJSONStr(flatMap["isolation-read.engines"]), Equals, `["tikv","tiflash","tidb"]`)
}
