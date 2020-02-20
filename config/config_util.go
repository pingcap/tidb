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
	"reflect"

	"github.com/pingcap/tidb/util/set"
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
	dynamicConfigItems = []string{
		"Performance.MaxProcs", "Performance.MaxMemory",
		"Performance.CrossJoin", "Performance.FeedbackProbability",
		"Performance.QueryFeedbackLimit", "Performance.PseudoEstimateRatio",
		"OOMAction", "MemQuotaQuery", "TiKVClient.StoreLimit",
	}
)

// MergeConfigItems overwrites the dynamic config items and leaves the other items unchanged.
func MergeConfigItems(dstConf, newConf *Config) (acceptedItems, rejectedItems []string) {
	dynamicConfigItemsSet := set.NewStringSet(dynamicConfigItems...)
	return mergeConfigItems(reflect.ValueOf(dstConf), reflect.ValueOf(newConf), "", dynamicConfigItemsSet)
}

func mergeConfigItems(dstConf, newConf reflect.Value, fieldPath string, dynamicConfigItemsSet set.StringSet) (acceptedItems, rejectedItems []string) {
	t := dstConf.Type()
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		dstConf = dstConf.Elem()
		newConf = newConf.Elem()
	}
	if t.Kind() != reflect.Struct {
		if reflect.DeepEqual(dstConf.Interface(), newConf.Interface()) {
			return
		}
		if dynamicConfigItemsSet.Exist(fieldPath) {
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
		as, rs := mergeConfigItems(dstConf.Field(i), newConf.Field(i), fieldName, dynamicConfigItemsSet)
		acceptedItems = append(acceptedItems, as...)
		rejectedItems = append(rejectedItems, rs...)
	}
	return
}
