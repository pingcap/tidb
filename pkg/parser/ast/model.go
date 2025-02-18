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

import (
	"encoding/json"
	"strings"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/types"
)

// TableLockType is the type of the table lock.
type TableLockType byte

const (
	// TableLockNone means this table lock is absent.
	TableLockNone TableLockType = iota
	// TableLockRead means the session with this lock can read the table (but not write it).
	// Multiple sessions can acquire a READ lock for the table at the same time.
	// Other sessions can read the table without explicitly acquiring a READ lock.
	TableLockRead
	// TableLockReadLocal is not supported.
	TableLockReadLocal
	// TableLockReadOnly is used to set a table into read-only status,
	// when the session exits, it will not release its lock automatically.
	TableLockReadOnly
	// TableLockWrite means only the session with this lock has write/read permission.
	// Only the session that holds the lock can access the table. No other session can access it until the lock is released.
	TableLockWrite
	// TableLockWriteLocal means the session with this lock has write/read permission, and the other session still has read permission.
	TableLockWriteLocal
)

// String implements fmt.Stringer interface.
func (t TableLockType) String() string {
	switch t {
	case TableLockNone:
		return "NONE"
	case TableLockRead:
		return "READ"
	case TableLockReadLocal:
		return "READ LOCAL"
	case TableLockReadOnly:
		return "READ ONLY"
	case TableLockWriteLocal:
		return "WRITE LOCAL"
	case TableLockWrite:
		return "WRITE"
	}
	return ""
}

// ViewAlgorithm is VIEW's SQL ALGORITHM characteristic.
// See https://dev.mysql.com/doc/refman/5.7/en/view-algorithms.html
type ViewAlgorithm int

// ViewAlgorithm values.
const (
	AlgorithmUndefined ViewAlgorithm = iota
	AlgorithmMerge
	AlgorithmTemptable
)

// String implements fmt.Stringer interface.
func (v *ViewAlgorithm) String() string {
	switch *v {
	case AlgorithmMerge:
		return "MERGE"
	case AlgorithmTemptable:
		return "TEMPTABLE"
	case AlgorithmUndefined:
		return "UNDEFINED"
	default:
		return "UNDEFINED"
	}
}

// ViewSecurity is VIEW's SQL SECURITY characteristic.
// See https://dev.mysql.com/doc/refman/5.7/en/create-view.html
type ViewSecurity int

// ViewSecurity values.
const (
	SecurityDefiner ViewSecurity = iota
	SecurityInvoker
)

// String implements fmt.Stringer interface.
func (v *ViewSecurity) String() string {
	switch *v {
	case SecurityInvoker:
		return "INVOKER"
	case SecurityDefiner:
		return "DEFINER"
	default:
		return "DEFINER"
	}
}

// ViewCheckOption is VIEW's WITH CHECK OPTION clause part.
// See https://dev.mysql.com/doc/refman/5.7/en/view-check-option.html
type ViewCheckOption int

// ViewCheckOption values.
const (
	CheckOptionLocal ViewCheckOption = iota
	CheckOptionCascaded
)

// String implements fmt.Stringer interface.
func (v *ViewCheckOption) String() string {
	switch *v {
	case CheckOptionLocal:
		return "LOCAL"
	case CheckOptionCascaded:
		return "CASCADED"
	default:
		return "CASCADED"
	}
}

// PartitionType is the type for PartitionInfo
type PartitionType int

// PartitionType types.
const (
	// Actually non-partitioned, but during DDL keeping the table as
	// a single partition
	PartitionTypeNone PartitionType = 0

	PartitionTypeRange      PartitionType = 1
	PartitionTypeHash       PartitionType = 2
	PartitionTypeList       PartitionType = 3
	PartitionTypeKey        PartitionType = 4
	PartitionTypeSystemTime PartitionType = 5
)

// String implements fmt.Stringer interface.
func (p PartitionType) String() string {
	switch p {
	case PartitionTypeRange:
		return "RANGE"
	case PartitionTypeHash:
		return "HASH"
	case PartitionTypeList:
		return "LIST"
	case PartitionTypeKey:
		return "KEY"
	case PartitionTypeSystemTime:
		return "SYSTEM_TIME"
	case PartitionTypeNone:
		return "NONE"
	default:
		return ""
	}
}

// PrimaryKeyType is the type of primary key.
// Available values are "clustered", "nonclustered", and ""(default).
type PrimaryKeyType int8

// String implements fmt.Stringer interface.
func (p PrimaryKeyType) String() string {
	switch p {
	case PrimaryKeyTypeClustered:
		return "CLUSTERED"
	case PrimaryKeyTypeNonClustered:
		return "NONCLUSTERED"
	default:
		return ""
	}
}

// PrimaryKeyType values.
const (
	PrimaryKeyTypeDefault PrimaryKeyType = iota
	PrimaryKeyTypeClustered
	PrimaryKeyTypeNonClustered
)

// IndexType is the type of index
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
	case IndexTypeInverted:
		return "INVERTED"
	default:
		return ""
	}
}

// IndexTypes
const (
	IndexTypeInvalid IndexType = iota
	IndexTypeBtree
	IndexTypeHash
	IndexTypeRtree
	IndexTypeHypo
	IndexTypeHNSW
	IndexTypeInverted
)

// ReferOptionType is the type for refer options.
type ReferOptionType int

// Refer option types.
const (
	ReferOptionNoOption ReferOptionType = iota
	ReferOptionRestrict
	ReferOptionCascade
	ReferOptionSetNull
	ReferOptionNoAction
	ReferOptionSetDefault
)

// String implements fmt.Stringer interface.
func (r ReferOptionType) String() string {
	switch r {
	case ReferOptionRestrict:
		return "RESTRICT"
	case ReferOptionCascade:
		return "CASCADE"
	case ReferOptionSetNull:
		return "SET NULL"
	case ReferOptionNoAction:
		return "NO ACTION"
	case ReferOptionSetDefault:
		return "SET DEFAULT"
	}
	return ""
}

// CIStr is case insensitive string.
type CIStr struct {
	O string `json:"O"` // Original string.
	L string `json:"L"` // Lower case string.
}

// Hash64 implements HashEquals interface.
func (cis *CIStr) Hash64(h types.IHasher) {
	h.HashString(cis.L)
}

// Equals implements HashEquals interface.
func (cis *CIStr) Equals(other any) bool {
	cis2, ok := other.(*CIStr)
	if !ok {
		return false
	}
	if cis == nil {
		return cis2 == nil
	}
	if cis2 == nil {
		return false
	}
	return cis.L == cis2.L
}

// String implements fmt.Stringer interface.
func (cis CIStr) String() string {
	return cis.O
}

// NewCIStr creates a new CIStr.
func NewCIStr(s string) (cs CIStr) {
	cs.O = s
	cs.L = strings.ToLower(s)
	return
}

// UnmarshalJSON implements the user defined unmarshal method.
// CIStr can be unmarshaled from a single string, so PartitionDefinition.Name
// in this change https://github.com/pingcap/tidb/pull/6460/files would be
// compatible during TiDB upgrading.
func (cis *CIStr) UnmarshalJSON(b []byte) error {
	type T CIStr
	if err := json.Unmarshal(b, (*T)(cis)); err == nil {
		return nil
	}

	// Unmarshal CIStr from a single string.
	err := json.Unmarshal(b, &cis.O)
	if err != nil {
		return errors.Trace(err)
	}
	cis.L = strings.ToLower(cis.O)
	return nil
}

// MemoryUsage return the memory usage of CIStr
func (cis *CIStr) MemoryUsage() (sum int64) {
	if cis == nil {
		return
	}

	return int64(unsafe.Sizeof(cis.O))*2 + int64(len(cis.O)+len(cis.L))
}

// RunawayActionType is the type of runaway action.
type RunawayActionType int32

// RunawayActionType values.
const (
	RunawayActionNone RunawayActionType = iota
	RunawayActionDryRun
	RunawayActionCooldown
	RunawayActionKill
	RunawayActionSwitchGroup
)

// RunawayWatchType is the type of runaway watch.
type RunawayWatchType int32

// RunawayWatchType values.
const (
	WatchNone RunawayWatchType = iota
	WatchExact
	WatchSimilar
	WatchPlan
)

// String implements fmt.Stringer interface.
func (t RunawayWatchType) String() string {
	switch t {
	case WatchExact:
		return "EXACT"
	case WatchSimilar:
		return "SIMILAR"
	case WatchPlan:
		return "PLAN"
	default:
		return "NONE"
	}
}

// RunawayOptionType is the runaway's option type.
type RunawayOptionType int

// RunawayOptionType values.
const (
	RunawayRule RunawayOptionType = iota
	RunawayAction
	RunawayWatch
)

// String implements fmt.Stringer interface.
func (t RunawayActionType) String() string {
	switch t {
	case RunawayActionDryRun:
		return "DRYRUN"
	case RunawayActionCooldown:
		return "COOLDOWN"
	case RunawayActionKill:
		return "KILL"
	case RunawayActionSwitchGroup:
		return "SWITCH_GROUP"
	default:
		return "DRYRUN"
	}
}

// ColumnChoice is the type of the column choice.
type ColumnChoice byte

// ColumnChoice values.
const (
	DefaultChoice ColumnChoice = iota
	AllColumns
	PredicateColumns
	ColumnList
)

// String implements fmt.Stringer interface.
func (s ColumnChoice) String() string {
	switch s {
	case AllColumns:
		return "ALL"
	case PredicateColumns:
		return "PREDICATE"
	case ColumnList:
		return "LIST"
	default:
		return "DEFAULT"
	}
}

// Priority values.
const (
	LowPriorityValue    = 1
	MediumPriorityValue = 8
	HighPriorityValue   = 16
)

// PriorityValueToName converts the priority value to corresponding name
func PriorityValueToName(value uint64) string {
	switch value {
	case LowPriorityValue:
		return "LOW"
	case MediumPriorityValue:
		return "MEDIUM"
	case HighPriorityValue:
		return "HIGH"
	default:
		return "MEDIUM"
	}
}
