// Copyright 2019 PingCAP, Inc.
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

package bindinfo

import (
	"time"
	"unsafe"

	"github.com/pingcap/parser"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/hint"
)

const (
	// Using is the bind info's in use status.
	Using = "using"
	// deleted is the bind info's deleted status.
	deleted = "deleted"
	// Invalid is the bind info's invalid status.
	Invalid = "invalid"
	// PendingVerify means the bind info needs to be verified.
	PendingVerify = "pending verify"
	// Rejected means that the bind has been rejected after verify process.
	// We can retry it after certain time has passed.
	Rejected = "rejected"
)

// BindType is the type of binding.
type BindType int64

const (
	// NormalizedBind means normalized binding.
	NormalizedBind = iota
	// Baseline means evolution binding.
	Baseline
)

// Binding stores the basic bind hint info.
type Binding struct {
	BindSQL string
	// Status represents the status of the binding. It can only be one of the following values:
	// 1. deleted: BindRecord is deleted, can not be used anymore.
	// 2. using: Binding is in the normal active mode.
	Status     string
	CreateTime types.Time
	UpdateTime types.Time
	Charset    string
	Collation  string
	BindType   BindType
	BucketID   int64
	Fixed      bool
	// Hint is the parsed hints, it is used to bind hints to stmt node.
	Hint *hint.HintsSet
	// ID is the string form of Hint. It would be non-empty only when the status is `Using` or `PendingVerify`.
	ID string
}

func (b *Binding) isSame(rb *Binding) bool {
	if b.ID != "" && rb.ID != "" {
		return b.ID == rb.ID
	}
	// Sometimes we cannot construct `ID` because of the changed schema, so we need to compare by bind sql.
	return b.BindSQL == rb.BindSQL
}

// SinceUpdateTime returns the duration since last update time. Export for test.
func (b *Binding) SinceUpdateTime() (time.Duration, error) {
	updateTime, err := b.UpdateTime.GoTime(time.Local)
	if err != nil {
		return 0, err
	}
	return time.Since(updateTime), nil
}

// cache is a k-v map, key is original sql, value is a slice of BindRecord.
type cache map[string][]*BindRecord

// BindRecord represents a sql bind record retrieved from the storage.
type BindRecord struct {
	OriginalSQL string
	Db          string

	NormalizedBinding *Binding
	Baselines         map[int64]*Binding
}

// GetFirstBinding return the first binding for a record.
func (br *BindRecord) GetFirstBinding() *Binding {
	if br.NormalizedBinding != nil {
		return br.NormalizedBinding
	}
	for _, bind := range br.Baselines {
		return bind
	}
	return nil
}

// HasUsingBinding checks if there are any using bindings in bind record.
func (br *BindRecord) HasUsingBinding() bool {
	return br.NormalizedBinding.Status == Using
}

// FindBinding find bindings in BindRecord.
func (br *BindRecord) FindBinding(hint string, bindType BindType, bucketID int64) *Binding {
	if bindType == NormalizedBind {
		if br.NormalizedBinding.ID == hint {
			return br.NormalizedBinding
		}
		return nil
	}
	if baseline, ok := br.Baselines[bucketID]; ok && baseline.ID == hint {
		return baseline
	}
	return nil
}

// FindBaseline find baseline in BindRecord.
func (br *BindRecord) FindBaseline(bucketID int64) *Binding {
	return br.Baselines[bucketID]
}

func (br *BindRecord) prepareHintsForBinding(sctx sessionctx.Context, bind *Binding, p *parser.Parser) error {
	if (bind.Hint != nil && bind.ID != "") || bind.Status == deleted {
		return nil
	}
	if sctx != nil {
		_, err := getHintsForSQL(sctx, bind.BindSQL)
		if err != nil {
			return err
		}
	}
	hintsSet, err := hint.ParseHintsSet(p, bind.BindSQL, bind.Charset, bind.Collation, br.Db)
	if err != nil {
		return err
	}
	hintsStr, err := hintsSet.Restore()
	if err != nil {
		return err
	}
	bind.Hint = hintsSet
	bind.ID = hintsStr
	return nil
}

// prepareHints builds ID and Hint for BindRecord. If sctx is not nil, we check if
// the BindSQL is still valid.
func (br *BindRecord) prepareHints(sctx sessionctx.Context) (err error) {
	p := parser.New()
	err = br.prepareHintsForBinding(sctx, br.NormalizedBinding, p)
	if err != nil {
		return err
	}
	for i := range br.Baselines {
		err = br.prepareHintsForBinding(sctx, br.Baselines[i], p)
		if err != nil {
			return err
		}
	}
	return nil
}

// `merge` merges two BindRecord. It will replace old bindings with new bindings if there are new updates.
func merge(lBindRecord, rBindRecord *BindRecord) *BindRecord {
	if lBindRecord == nil {
		return rBindRecord
	}
	if rBindRecord == nil {
		return lBindRecord
	}
	result := lBindRecord.shallowCopy()
	if rBindRecord.NormalizedBinding.UpdateTime.Compare(lBindRecord.NormalizedBinding.UpdateTime) > 0 {
		result.NormalizedBinding = rBindRecord.NormalizedBinding
	}
	for bucketID, rBaseline := range rBindRecord.Baselines {
		lBaseline, found := lBindRecord.Baselines[bucketID]
		if !found || rBaseline.UpdateTime.Compare(lBaseline.UpdateTime) >= 0 {
			result.Baselines[bucketID] = rBaseline
		}
	}
	return result
}

func (br *BindRecord) remove(del *BindRecord) *BindRecord {
	// Delete all bindings.
	if del.NormalizedBinding != nil && del.NormalizedBinding.isSame(br.NormalizedBinding) {
		return &BindRecord{OriginalSQL: br.OriginalSQL, Db: br.Db}
	}
	result := br.shallowCopy()
	for bucketID, deletedBaseline := range del.Baselines {
		baseline, found := result.Baselines[bucketID]
		if found && baseline.isSame(deletedBaseline) {
			delete(result.Baselines, bucketID)
		}
	}
	return result
}

func (br *BindRecord) removeDeletedBindings() *BindRecord {
	result := BindRecord{OriginalSQL: br.OriginalSQL, Db: br.Db, NormalizedBinding: br.NormalizedBinding}
	for idx, baseline := range br.Baselines {
		if baseline.Status != deleted {
			result.Baselines[idx] = baseline
		}
	}
	return &result
}

// shallowCopy shallow copies the BindRecord.
func (br *BindRecord) shallowCopy() *BindRecord {
	result := BindRecord{
		OriginalSQL:       br.OriginalSQL,
		Db:                br.Db,
		NormalizedBinding: br.NormalizedBinding,
		Baselines:         make(map[int64]*Binding, len(br.Baselines)),
	}
	for i, baseline := range br.Baselines {
		newBaseline := *baseline
		result.Baselines[i] = &newBaseline
	}
	return &result
}

func (br *BindRecord) isSame(other *BindRecord) bool {
	return br.OriginalSQL == other.OriginalSQL && br.Db == other.Db
}

var statusIndex = map[string]int{
	Using:   0,
	deleted: 1,
	Invalid: 2,
}

func (br *BindRecord) metrics() ([]float64, []int) {
	sizes := make([]float64, len(statusIndex))
	count := make([]int, len(statusIndex))
	if br == nil {
		return sizes, count
	}
	commonLength := float64(len(br.OriginalSQL) + len(br.Db))
	// We treat it as deleted if there are no bindings. It could only occur in session handles.
	if br.NormalizedBinding == nil {
		sizes[statusIndex[deleted]] = commonLength
		count[statusIndex[deleted]] = 1
		return sizes, count
	}
	// Make the common length counted in the first binding.
	sizes[statusIndex[br.NormalizedBinding.Status]] = commonLength
	for _, baseline := range br.Baselines {
		sizes[statusIndex[baseline.Status]] += baseline.size()
		count[statusIndex[baseline.Status]]++
	}
	return sizes, count
}

// size calculates the memory size of a bind info.
func (b *Binding) size() float64 {
	res := len(b.BindSQL) + len(b.Status) + 2*int(unsafe.Sizeof(b.CreateTime)) + len(b.Charset) + len(b.Collation)
	return float64(res)
}

func updateMetrics(scope string, before *BindRecord, after *BindRecord, sizeOnly bool) {
	beforeSize, beforeCount := before.metrics()
	afterSize, afterCount := after.metrics()
	for status, index := range statusIndex {
		metrics.BindMemoryUsage.WithLabelValues(scope, status).Add(afterSize[index] - beforeSize[index])
		if !sizeOnly {
			metrics.BindTotalGauge.WithLabelValues(scope, status).Add(float64(afterCount[index] - beforeCount[index]))
		}
	}
}
