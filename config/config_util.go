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
	"time"

	"github.com/pingcap/errors"
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
		"OOMAction":                       {},
		"MemQuotaQuery":                   {},
		"TiKVClient.StoreLimit":           {},
		"Log.Level":                       {},
		"Log.SlowThreshold":               {},
		"Log.QueryLogMaxLen":              {},
		"Log.ExpensiveThreshold":          {},
		"CheckMb4ValueInUTF8":             {},
		"EnableStreaming":                 {},
		"TxnLocalLatches.Capacity":        {},
		"CompatibleKillQuery":             {},
		"TreatOldVersionUTF8AsUTF8MB4":    {},
		"OpenTracing.Enable":              {},
		"PreparedPlanCache.Enabled":       {},
	}
)

// MergeConfigItems overwrites the dynamic config items and leaves the other items unchanged.
func MergeConfigItems(dstConf, newConf *Config) (acceptedItems, rejectedItems []string) {
	return mergeConfigItems(reflect.ValueOf(dstConf), reflect.ValueOf(newConf), "")
}

func mergeConfigItems(dstConf, newConf reflect.Value, fieldPath string) (acceptedItems, rejectedItems []string) {
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

func atomicWriteConfig(c *Config, confPath string) (err error) {
	content, err := encodeConfig(c)
	if err != nil {
		return err
	}
	tmpConfPath := filepath.Join(os.TempDir(), fmt.Sprintf("tmp_conf_%v.toml", time.Now().Format("20060102150405")))
	if err := ioutil.WriteFile(tmpConfPath, []byte(content), 0666); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(os.Rename(tmpConfPath, confPath))
}
