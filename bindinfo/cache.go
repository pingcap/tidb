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
	// Hint is the parsed hints, it is used to bind hints to stmt node.
	Hint *HintsSet
	// ID is the string form of all hints. It is used to uniquely identify different hints.
	// It would be non-empty only when the status is `Using` or `PendingVerify`.
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

	Bindings []Binding
}

// HasUsingBinding checks if there are any using bindings in bind record.
func (br *BindRecord) HasUsingBinding() bool {
	for _, binding := range br.Bindings {
		if binding.Status == Using {
			return true
		}
	}
	return false
}

// FindBinding find bindings in BindRecord.
func (br *BindRecord) FindBinding(hint string) *Binding {
	for _, binding := range br.Bindings {
		if binding.ID == hint {
			return &binding
		}
	}
	return nil
}

func (br *BindRecord) prepareHints(sctx sessionctx.Context) error {
	p := parser.New()
	for i, bind := range br.Bindings {
		if (bind.Hint != nil && bind.ID != "") || bind.Status == deleted {
			continue
		}
		hintsSet, err := ParseHintsSet(p, bind.BindSQL, bind.Charset, bind.Collation)
		if err != nil {
			return err
		}
		hints, err := getHintsForSQL(sctx, bind.BindSQL)
		if err != nil {
			return err
		}
		br.Bindings[i].Hint = hintsSet
		br.Bindings[i].ID = hints
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
	for _, rbind := range rBindRecord.Bindings {
		found := false
		for j, lbind := range lBindRecord.Bindings {
			if lbind.isSame(&rbind) {
				found = true
				if rbind.UpdateTime.Compare(lbind.UpdateTime) >= 0 {
					result.Bindings[j] = rbind
				}
				break
			}
		}
		if !found {
			result.Bindings = append(result.Bindings, rbind)
		}
	}
	return result
}

func (br *BindRecord) remove(deleted *BindRecord) *BindRecord {
	// Delete all bindings.
	if len(deleted.Bindings) == 0 {
		return &BindRecord{OriginalSQL: br.OriginalSQL, Db: br.Db}
	}
	result := br.shallowCopy()
	for _, deletedBind := range deleted.Bindings {
		for i, bind := range result.Bindings {
			if bind.isSame(&deletedBind) {
				result.Bindings = append(result.Bindings[:i], result.Bindings[i+1:]...)
				break
			}
		}
	}
	return result
}

func (br *BindRecord) removeDeletedBindings() *BindRecord {
	result := BindRecord{OriginalSQL: br.OriginalSQL, Db: br.Db, Bindings: make([]Binding, 0, len(br.Bindings))}
	for _, binding := range br.Bindings {
		if binding.Status != deleted {
			result.Bindings = append(result.Bindings, binding)
		}
	}
	return &result
}

// shallowCopy shallow copies the BindRecord.
func (br *BindRecord) shallowCopy() *BindRecord {
	result := BindRecord{
		OriginalSQL: br.OriginalSQL,
		Db:          br.Db,
		Bindings:    make([]Binding, len(br.Bindings)),
	}
	copy(result.Bindings, br.Bindings)
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
	if len(br.Bindings) == 0 {
		sizes[statusIndex[deleted]] = commonLength
		count[statusIndex[deleted]] = 1
		return sizes, count
	}
	// Make the common length counted in the first binding.
	sizes[statusIndex[br.Bindings[0].Status]] = commonLength
	for _, binding := range br.Bindings {
		sizes[statusIndex[binding.Status]] += binding.size()
		count[statusIndex[binding.Status]]++
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
