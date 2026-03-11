// Copyright 2015 PingCAP, Inc.
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

package ast

import pmodel "github.com/pingcap/tidb/pkg/parser/model"

// Keep AST model types compatible with parser/model without duplicating implementations.
type (
	TableLockType     = pmodel.TableLockType
	ViewAlgorithm     = pmodel.ViewAlgorithm
	ViewSecurity      = pmodel.ViewSecurity
	ViewCheckOption   = pmodel.ViewCheckOption
	PartitionType     = pmodel.PartitionType
	PrimaryKeyType    = pmodel.PrimaryKeyType
	ReferOptionType   = pmodel.ReferOptionType
	CIStr             = pmodel.CIStr
	RunawayActionType = pmodel.RunawayActionType
	RunawayWatchType  = pmodel.RunawayWatchType
	RunawayOptionType = pmodel.RunawayOptionType
	ColumnChoice      = pmodel.ColumnChoice
)

const (
	TableLockNone       = pmodel.TableLockNone
	TableLockRead       = pmodel.TableLockRead
	TableLockReadLocal  = pmodel.TableLockReadLocal
	TableLockReadOnly   = pmodel.TableLockReadOnly
	TableLockWrite      = pmodel.TableLockWrite
	TableLockWriteLocal = pmodel.TableLockWriteLocal
)

const (
	AlgorithmUndefined = pmodel.AlgorithmUndefined
	AlgorithmMerge     = pmodel.AlgorithmMerge
	AlgorithmTemptable = pmodel.AlgorithmTemptable
)

const (
	SecurityDefiner = pmodel.SecurityDefiner
	SecurityInvoker = pmodel.SecurityInvoker
)

const (
	CheckOptionLocal    = pmodel.CheckOptionLocal
	CheckOptionCascaded = pmodel.CheckOptionCascaded
)

const (
	PartitionTypeNone       = pmodel.PartitionTypeNone
	PartitionTypeRange      = pmodel.PartitionTypeRange
	PartitionTypeHash       = pmodel.PartitionTypeHash
	PartitionTypeList       = pmodel.PartitionTypeList
	PartitionTypeKey        = pmodel.PartitionTypeKey
	PartitionTypeSystemTime = pmodel.PartitionTypeSystemTime
)

const (
	PrimaryKeyTypeDefault      = pmodel.PrimaryKeyTypeDefault
	PrimaryKeyTypeClustered    = pmodel.PrimaryKeyTypeClustered
	PrimaryKeyTypeNonClustered = pmodel.PrimaryKeyTypeNonClustered
)

// IndexType is the type of index.
type IndexType int

// String implements Stringer interface.
func (t IndexType) String() string {
	switch t {
	case IndexTypeBtree:
		return "BTREE"
	case IndexTypeHash:
		return "HASH"
	case IndexTypeRtree:
		return "RTREE"
	case IndexTypeHypo:
		return "HYPO"
	case IndexTypeHNSW:
		return "HNSW"
	case IndexTypeVector:
		return "VECTOR"
	case IndexTypeInverted:
		return "INVERTED"
	case IndexTypeFulltext:
		return "FULLTEXT"
	default:
		return ""
	}
}

// Index types.
// Warning: 1) Also used in TiFlash 2) May come from a previous version persisted in TableInfo.
// So you must keep it compatible when modifying it.
const (
	IndexTypeInvalid IndexType = iota
	IndexTypeBtree
	IndexTypeHash
	IndexTypeRtree
	IndexTypeHypo
	IndexTypeVector
	IndexTypeInverted
	// IndexTypeHNSW is only used in AST.
	// It will be rewritten into IndexTypeVector after preprocessor phase.
	IndexTypeHNSW
	IndexTypeFulltext
)

const (
	ReferOptionNoOption   = pmodel.ReferOptionNoOption
	ReferOptionRestrict   = pmodel.ReferOptionRestrict
	ReferOptionCascade    = pmodel.ReferOptionCascade
	ReferOptionSetNull    = pmodel.ReferOptionSetNull
	ReferOptionNoAction   = pmodel.ReferOptionNoAction
	ReferOptionSetDefault = pmodel.ReferOptionSetDefault
)

const (
	RunawayActionNone        = pmodel.RunawayActionNone
	RunawayActionDryRun      = pmodel.RunawayActionDryRun
	RunawayActionCooldown    = pmodel.RunawayActionCooldown
	RunawayActionKill        = pmodel.RunawayActionKill
	RunawayActionSwitchGroup = pmodel.RunawayActionSwitchGroup
)

const (
	WatchNone    = pmodel.WatchNone
	WatchExact   = pmodel.WatchExact
	WatchSimilar = pmodel.WatchSimilar
	WatchPlan    = pmodel.WatchPlan
)

const (
	RunawayRule   = pmodel.RunawayRule
	RunawayAction = pmodel.RunawayAction
	RunawayWatch  = pmodel.RunawayWatch
)

const (
	DefaultChoice    = pmodel.DefaultChoice
	AllColumns       = pmodel.AllColumns
	PredicateColumns = pmodel.PredicateColumns
	ColumnList       = pmodel.ColumnList
)

const (
	LowPriorityValue    = pmodel.LowPriorityValue
	MediumPriorityValue = pmodel.MediumPriorityValue
	HighPriorityValue   = pmodel.HighPriorityValue
)

// NewCIStr creates a new CIStr.
func NewCIStr(s string) CIStr {
	return pmodel.NewCIStr(s)
}

// PriorityValueToName converts the priority value to corresponding name.
func PriorityValueToName(value uint64) string {
	return pmodel.PriorityValueToName(value)
}
