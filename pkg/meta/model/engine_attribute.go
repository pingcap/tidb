// Copyright 2025 PingCAP, Inc.
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

package model

import (
	"encoding/json"
)

// EngineAttribute is the JSON format of ENGINE_ATTRIBUTE property of tables.
type EngineAttribute struct {
	StorageClass json.RawMessage `json:"storage_class"`
}

// ParseEngineAttributeFromString parses EngineAttribute from a string.
func ParseEngineAttributeFromString(input string) (*EngineAttribute, error) {
	attr := &EngineAttribute{}
	if len(input) == 0 {
		return attr, nil
	}
	err := json.Unmarshal([]byte(input), attr)
	if err != nil {
		return nil, err
	}
	return attr, nil
}

// Name of storage class tiers
const (
	StorageClassTierStandard string = "STANDARD"
	StorageClassTierIA       string = "IA"

	StorageClassTierDefault string = StorageClassTierStandard
)

// StorageClassDef is the tier & scope definition for storage class.
type StorageClassDef struct {
	Tier        string                    `json:"tier"`
	NamesIn     []string                  `json:"names_in"`
	LessThan    *string                   `json:"less_than"`
	ValuesIn    []string                  `json:"values_in"`
	Transitions []StorageClassTransitRule `json:"transitions"`
}

// HasNoScopeDef checks whether the storage class definition has no scope definition.
func (d *StorageClassDef) HasNoScopeDef() bool {
	return len(d.NamesIn) == 0 && d.LessThan == nil && len(d.ValuesIn) == 0
}

// StorageClassSettings is the settings for storage class.
type StorageClassSettings struct {
	Defs []*StorageClassDef `json:"defs"`
}

// StorageClassTransitRule defines the storage class transition rule.
type StorageClassTransitRule struct {
	Tier         string `json:"tier"`
	AfterDays    uint   `json:"after_days"`
	AfterSeconds uint   `json:"after_seconds,omitempty"`
}

// TotalSeconds returns the total seconds after which the transition happens.
func (r *StorageClassTransitRule) TotalSeconds() uint {
	return r.AfterDays*86400 + r.AfterSeconds
}

func buildStorageClassString(tier string, transitions []StorageClassTransitRule) string {
	if len(transitions) == 0 {
		return tier
	}

	type storageClassInfo struct {
		Tier        string                    `json:"tier"`
		Transitions []StorageClassTransitRule `json:"transitions,omitempty"`
	}
	sc := storageClassInfo{
		Tier:        tier,
		Transitions: transitions,
	}
	s, _ := json.Marshal(sc)
	return string(s)
}
