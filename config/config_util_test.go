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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
)

func TestCloneConf(t *testing.T) {
	c1, err := CloneConf(&defaultConf)
	require.NoError(t, err)
	c2, err := CloneConf(c1)
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual(c1, c2))

	c1.Store = "abc"
	c1.Port = 2333
	c1.Instance.EnableSlowLog.Store(!c1.Instance.EnableSlowLog.Load())
	c1.RepairTableList = append(c1.RepairTableList, "abc")
	require.NotEqual(t, c2.Store, c1.Store)
	require.NotEqual(t, c2.Port, c1.Port)
	require.NotEqual(t, c2.Instance.EnableSlowLog, c1.Instance.EnableSlowLog)
	require.NotEqual(t, fmt.Sprintf("%v", c2.RepairTableList), fmt.Sprintf("%v", c1.RepairTableList))
}

func TestMergeConfigItems(t *testing.T) {
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
	newConf.TiKVClient.StoreLimit = 123

	// rejected
	newConf.Store = "tiflash"
	newConf.Port = 2333
	newConf.AdvertiseAddress = "1.2.3.4"
	newConf.Instance.SlowThreshold = 2345

	as, rs := MergeConfigItems(oldConf, newConf)
	require.Equal(t, 8, len(as))
	require.Equal(t, 3, len(rs))
	for _, a := range as {
		_, ok := dynamicConfigItems[a]
		require.True(t, ok)
	}
	for _, a := range rs {
		_, ok := dynamicConfigItems[a]
		require.False(t, ok)
	}

	require.Equal(t, newConf.Performance.MaxProcs, oldConf.Performance.MaxProcs)
	require.Equal(t, newConf.Performance.MaxMemory, oldConf.Performance.MaxMemory)
	require.Equal(t, newConf.Performance.CrossJoin, oldConf.Performance.CrossJoin)
	require.Equal(t, newConf.Performance.FeedbackProbability, oldConf.Performance.FeedbackProbability)
	require.Equal(t, newConf.Performance.QueryFeedbackLimit, oldConf.Performance.QueryFeedbackLimit)
	require.Equal(t, newConf.Performance.PseudoEstimateRatio, oldConf.Performance.PseudoEstimateRatio)
	require.Equal(t, newConf.TiKVClient.StoreLimit, oldConf.TiKVClient.StoreLimit)
	require.Equal(t, newConf.Instance.SlowThreshold, oldConf.Instance.SlowThreshold)

	require.Equal(t, oriConf.Store, oldConf.Store)
	require.Equal(t, oriConf.Port, oldConf.Port)
	require.Equal(t, oriConf.AdvertiseAddress, oldConf.AdvertiseAddress)
}

func TestFlattenConfig(t *testing.T) {
	toJSONStr := func(v interface{}) string {
		str, err := json.Marshal(v)
		require.NoError(t, err)
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
	require.NoError(t, json.Unmarshal([]byte(jsonConf), &nested))
	flatMap := FlattenConfigItems(nested)
	require.Equal(t, 7, len(flatMap))
	require.Equal(t, "233333", toJSONStr(flatMap["k0"]))
	require.Equal(t, "v1", flatMap["k1"])
	require.Equal(t, `["v2-1","v2-2","v2-3"]`, toJSONStr(flatMap["k2"]))
	require.Equal(t, `[{"k3-1":"v3-1"},{"k3-2":"v3-2"},{"k3-3":"v3-3"}]`, toJSONStr(flatMap["k3"]))
	require.Equal(t, `[1,2,3,4]`, toJSONStr(flatMap["k4.k4-1"]))
	require.Equal(t, `[5,6,7,8]`, toJSONStr(flatMap["k4.k4-2"]))
	require.Equal(t, `[666]`, toJSONStr(flatMap["k4.k4-3"]))

	tomlConf := `
port=4000
[log]
level='info'
format='text'
[isolation-read]
engines = ["tikv", "tiflash", "tidb"]
`
	nested = make(map[string]interface{})
	require.NoError(t, toml.Unmarshal([]byte(tomlConf), &nested))
	flatMap = FlattenConfigItems(nested)
	require.Equal(t, 4, len(flatMap))
	require.Equal(t, "4000", toJSONStr(flatMap["port"]))
	require.Equal(t, `"info"`, toJSONStr(flatMap["log.level"]))
	require.Equal(t, `"text"`, toJSONStr(flatMap["log.format"]))
	require.Equal(t, `["tikv","tiflash","tidb"]`, toJSONStr(flatMap["isolation-read.engines"]))
}
