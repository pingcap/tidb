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
	"slices"
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

	// StorageClassTransitionStateCompleted means every observed replica reached
	// the operation's target before the operation ended.
	StorageClassTransitionStateCompleted = "COMPLETED"
	// StorageClassTransitionStateSuperseded means a newer explicit DDL replaced
	// the operation before it completed.
	StorageClassTransitionStateSuperseded = "SUPERSEDED"
)

// StorageClassTransitionState identifies one active explicit storage-class
// transition. PartitionName is empty for a table-level transition.
type StorageClassTransitionState struct {
	// Target is the SQL-facing IA or STANDARD target.
	Target string `json:"target"`
	// StartTS is the operation identity allocated from TSO.
	StartTS uint64 `json:"start_ts"`
	// SchemaName and TableName are the SQL object names at operation start.
	SchemaName string `json:"schema_name"`
	TableName  string `json:"table_name"`
	// PartitionName is the partition name at operation start.
	PartitionName string `json:"partition_name,omitempty"`
}

// Clone clones an active storage-class transition.
func (s *StorageClassTransitionState) Clone() *StorageClassTransitionState {
	if s == nil {
		return nil
	}
	cloned := *s
	return &cloned
}

// StorageClassTransitionTarget identifies one physical table or partition in
// a logical storage-class operation.
type StorageClassTransitionTarget struct {
	PhysicalID    int64  `json:"physical_id"`
	PartitionID   int64  `json:"partition_id,omitempty"`
	PartitionName string `json:"partition_name,omitempty"`
}

// StorageClassTransitionHistory is a durable pending history record embedded
// in TableInfo. The DDL owner copies it to the system history table and removes
// it only after that insert succeeds.
type StorageClassTransitionHistory struct {
	Target            string                         `json:"target"`
	State             string                         `json:"state"`
	StartTS           uint64                         `json:"start_ts"`
	FinishTS          uint64                         `json:"finish_ts"`
	SchemaName        string                         `json:"schema_name"`
	TableName         string                         `json:"table_name"`
	Targets           []StorageClassTransitionTarget `json:"targets"`
	TotalReplicas     uint64                         `json:"total_replicas,omitempty"`
	CompletedReplicas uint64                         `json:"completed_replicas,omitempty"`
	StatusValid       bool                           `json:"status_valid,omitempty"`
}

// Clone clones a pending storage-class history record.
func (h StorageClassTransitionHistory) Clone() StorageClassTransitionHistory {
	h.Targets = slices.Clone(h.Targets)
	return h
}

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
