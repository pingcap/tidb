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

	"github.com/pingcap/errors"
	tikvcfg "github.com/tikv/client-go/v2/config"
	atomicutil "go.uber.org/atomic"
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

// nullableBool defaults unset bool options to unset instead of false, which enables us to know if the user has set 2
// conflict options at the same time.
type nullableBool struct {
	IsValid bool
	IsTrue  bool
}

func (b *nullableBool) toBool() bool {
	return b.IsValid && b.IsTrue
}

func (b nullableBool) MarshalJSON() ([]byte, error) {
	switch b {
	case nbTrue:
		return json.Marshal(true)
	case nbFalse:
		return json.Marshal(false)
	default:
		return json.Marshal(nil)
	}
}

func (b *nullableBool) UnmarshalText(text []byte) error {
	str := string(text)
	switch str {
	case "", "null":
		*b = nbUnset
		return nil
	case "true":
		*b = nbTrue
	case "false":
		*b = nbFalse
	default:
		*b = nbUnset
		return errors.New("Invalid value for bool type: " + str)
	}
	return nil
}

func (b nullableBool) MarshalText() ([]byte, error) {
	if !b.IsValid {
		return []byte(""), nil
	}
	if b.IsTrue {
		return []byte("true"), nil
	}
	return []byte("false"), nil
}

func (b *nullableBool) UnmarshalJSON(data []byte) error {
	var err error
	var v interface{}
	if err = json.Unmarshal(data, &v); err != nil {
		return err
	}
	switch raw := v.(type) {
	case bool:
		*b = nullableBool{true, raw}
	default:
		*b = nbUnset
	}
	return err
}

// AtomicBool is a helper type for atomic operations on a boolean value.
type AtomicBool struct {
	atomicutil.Bool
}

// NewAtomicBool creates an AtomicBool.
func NewAtomicBool(v bool) *AtomicBool {
	return &AtomicBool{*atomicutil.NewBool(v)}
}

// MarshalText implements the encoding.TextMarshaler interface.
func (b AtomicBool) MarshalText() ([]byte, error) {
	if b.Load() {
		return []byte("true"), nil
	}
	return []byte("false"), nil
}

// UnmarshalText implements the encoding.TextUnmarshaler interface.
func (b *AtomicBool) UnmarshalText(text []byte) error {
	str := string(text)
	switch str {
	case "", "null":
		*b = AtomicBool{*atomicutil.NewBool(false)}
	case "true":
		*b = AtomicBool{*atomicutil.NewBool(true)}
	case "false":
		*b = AtomicBool{*atomicutil.NewBool(false)}
	default:
		*b = AtomicBool{*atomicutil.NewBool(false)}
		return errors.New("Invalid value for bool type: " + str)
	}
	return nil
}

// AtomicString is a helper type for atomic operations on a string value.
type AtomicString struct {
	atomicutil.String
}

// NewAtomicString creates an AtomicString
func NewAtomicString(v string) *AtomicString {
	return &AtomicString{*atomicutil.NewString(v)}
}

// MarshalText implements the encoding.TextMarshaler interface.
func (s AtomicString) MarshalText() ([]byte, error) {
	return []byte(s.Load()), nil
}

// UnmarshalText implements the encoding.TextUnmarshaler interface.
func (s *AtomicString) UnmarshalText(text []byte) error {
	str := string(text)
	*s = AtomicString{*atomicutil.NewString(str)}
	return nil
}
