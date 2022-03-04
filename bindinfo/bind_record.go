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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bindinfo

import (
	"time"
	"unsafe"

	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/hint"
)

const (
	// Enable is the bind info's in enable status.
	// It is the same as the previous 'Using' status.
	// Only use 'Enable' status in the future, not the 'Using' status.
	// The using status is preserved for compatibility.
	Enable = "enable"
	// Disable is the bind info's in disable status.
	Disable = "disable"
	// Using is the bind info's in use status.
	// The 'Using' status is preserved for compatibility.
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
	// Manual indicates the binding is created by SQL like "create binding for ...".
	Manual = "manual"
	// Capture indicates the binding is captured by TiDB automatically.
	Capture = "capture"
	// Evolve indicates the binding is evolved by TiDB from old bindings.
	Evolve = "evolve"
	// Builtin indicates the binding is a builtin record for internal locking purpose. It is also the status for the builtin binding.
	Builtin = "builtin"
)

// Binding stores the basic bind hint info.
type Binding struct {
	BindSQL string
	// Status represents the status of the binding. It can only be one of the following values:
	// 1. deleted: BindRecord is deleted, can not be used anymore.
	// 2. enable, using: Binding is in the normal active mode.
	Status     string
	CreateTime types.Time
	UpdateTime types.Time
	Source     string
	Charset    string
	Collation  string
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

// IsBindingEnable returns whether the binding is enable.
func (b *Binding) IsBindingEnable() bool {
	return b.Status == Enable || b.Status == Using
}

// SinceUpdateTime returns the duration since last update time. Export for test.
func (b *Binding) SinceUpdateTime() (time.Duration, error) {
	updateTime, err := b.UpdateTime.GoTime(time.Local)
	if err != nil {
		return 0, err
	}
	return time.Since(updateTime), nil
}

// BindRecord represents a sql bind record retrieved from the storage.
type BindRecord struct {
	OriginalSQL string
	Db          string

	Bindings []Binding
}

// HasUsingBinding checks if there are any using bindings in bind record.
func (br *BindRecord) HasUsingBinding() bool {
	for _, binding := range br.Bindings {
		if binding.IsBindingEnable() {
			return true
		}
	}
	return false
}

// FindUsingBinding gets the using binding.
// There is at most one binding that can be used now
func (br *BindRecord) FindUsingBinding() *Binding {
	for _, binding := range br.Bindings {
		if binding.IsBindingEnable() {
			return &binding
		}
	}
	return nil
}

// FindBinding find bindings in BindRecord.
func (br *BindRecord) FindBinding(hint string) *Binding {
	for i := range br.Bindings {
		binding := br.Bindings[i]
		if binding.ID == hint {
			return &binding
		}
	}
	return nil
}

// prepareHints builds ID and Hint for BindRecord. If sctx is not nil, we check if
// the BindSQL is still valid.
func (br *BindRecord) prepareHints(sctx sessionctx.Context) error {
	p := parser.New()
	for i, bind := range br.Bindings {
		if (bind.Hint != nil && bind.ID != "") || bind.Status == deleted {
			continue
		}
		hintsSet, stmt, warns, err := hint.ParseHintsSet(p, bind.BindSQL, bind.Charset, bind.Collation, br.Db)
		if err != nil {
			return err
		}
		if sctx != nil {
			paramChecker := &paramMarkerChecker{}
			stmt.Accept(paramChecker)
			if !paramChecker.hasParamMarker {
				_, err = getHintsForSQL(sctx, bind.BindSQL)
				if err != nil {
					return err
				}
			}
		}
		hintsStr, err := hintsSet.Restore()
		if err != nil {
			return err
		}
		// For `create global binding for select * from t using select * from t`, we allow it though hintsStr is empty.
		// For `create global binding for select * from t using select /*+ non_exist_hint() */ * from t`,
		// the hint is totally invalid, we escalate warning to error.
		if hintsStr == "" && len(warns) > 0 {
			return warns[0]
		}
		br.Bindings[i].Hint = hintsSet
		br.Bindings[i].ID = hintsStr
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
	for i := range rBindRecord.Bindings {
		rbind := rBindRecord.Bindings[i]
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
	for j := range deleted.Bindings {
		deletedBind := deleted.Bindings[j]
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
	return br.OriginalSQL == other.OriginalSQL
}

// size calculates the memory size of a BindRecord.
func (br *BindRecord) size() float64 {
	mem := float64(len(hack.Slice(br.OriginalSQL)) + len(hack.Slice(br.Db)))
	for _, binding := range br.Bindings {
		mem += binding.size()
	}
	return mem
}

var statusIndex = map[string]int{
	Enable:  0,
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
	res := len(b.BindSQL) + len(b.Status) + 2*int(unsafe.Sizeof(b.CreateTime)) + len(b.Charset) + len(b.Collation) + len(b.ID)
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
