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
	"reflect"

	tikvcfg "github.com/tikv/client-go/v2/config"
)

// CloneConf deeply clones this config.
func CloneConf(conf *Config) (*Config, error) {
	content, err := json.Marshal(conf)
	if err != nil {
		return nil, err
	}
	var clonedConf Config
	if err := json.Unmarshal(content, &clonedConf); err != nil {
		return nil, err
	}
	return &clonedConf, nil
}

var (
	// dynamicConfigItems contains all config items that can be changed during runtime.
	dynamicConfigItems = map[string]struct{}{
		"Performance.MaxProcs":            {},
		"Performance.MaxMemory":           {},
		"Performance.CrossJoin":           {},
		"Performance.FeedbackProbability": {},
		"Performance.QueryFeedbackLimit":  {},
		"Performance.PseudoEstimateRatio": {},
		"Performance.StmtCountLimit":      {},
		"Performance.TCPKeepAlive":        {},
		"TiKVClient.StoreLimit":           {},
		"Log.Level":                       {},
		"Log.ExpensiveThreshold":          {},
		"Instance.SlowThreshold":          {},
		"Instance.CheckMb4ValueInUTF8":    {},
		"TxnLocalLatches.Capacity":        {},
		"CompatibleKillQuery":             {},
		"TreatOldVersionUTF8AsUTF8MB4":    {},
		"OpenTracing.Enable":              {},
	}
)

// MergeConfigItems overwrites the dynamic config items and leaves the other items unchanged.
func MergeConfigItems(dstConf, newConf *Config) (acceptedItems, rejectedItems []string) {
	return mergeConfigItems(reflect.ValueOf(dstConf), reflect.ValueOf(newConf), "")
}

func mergeConfigItems(dstConf, newConf reflect.Value, fieldPath string) (acceptedItems, rejectedItems []string) {
	t := dstConf.Type()
	if t.Name() == "AtomicBool" {
		if reflect.DeepEqual(dstConf.Interface().(AtomicBool), newConf.Interface().(AtomicBool)) {
			return
		}
		if _, ok := dynamicConfigItems[fieldPath]; ok {
			dstConf.Set(newConf)
			return []string{fieldPath}, nil
		}
		return nil, []string{fieldPath}
	}
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		dstConf = dstConf.Elem()
		newConf = newConf.Elem()
	}
	if t.Kind() != reflect.Struct {
		if reflect.DeepEqual(dstConf.Interface(), newConf.Interface()) {
			return
		}
		if _, ok := dynamicConfigItems[fieldPath]; ok {
			dstConf.Set(newConf)
			return []string{fieldPath}, nil
		}
		return nil, []string{fieldPath}
	}

	for i := 0; i < t.NumField(); i++ {
		fieldName := t.Field(i).Name
		if fieldPath != "" {
			fieldName = fieldPath + "." + fieldName
		}
		as, rs := mergeConfigItems(dstConf.Field(i), newConf.Field(i), fieldName)
		acceptedItems = append(acceptedItems, as...)
		rejectedItems = append(rejectedItems, rs...)
	}
	return
}

// ConfReloadFunc is used to reload the config to make it work.
type ConfReloadFunc func(oldConf, newConf *Config)

// FlattenConfigItems flatten this config, see more cases in the test.
func FlattenConfigItems(nestedConfig map[string]interface{}) map[string]interface{} {
	flatMap := make(map[string]interface{})
	flatten(flatMap, nestedConfig, "")
	return flatMap
}

func flatten(flatMap map[string]interface{}, nested interface{}, prefix string) {
	switch nested := nested.(type) {
	case map[string]interface{}:
		for k, v := range nested {
			path := k
			if prefix != "" {
				path = prefix + "." + k
			}
			flatten(flatMap, v, path)
		}
	default: // don't flatten arrays
		flatMap[prefix] = nested
	}
}

// GetTxnScopeFromConfig extracts @@txn_scope value from the config.
func GetTxnScopeFromConfig() string {
	return tikvcfg.GetTxnScopeFromConfig()
}
