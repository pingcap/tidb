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
	"unsafe"

	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

const (
	// Using is the bind info's in use status.
	Using = "using"
	// deleted is the bind info's deleted status.
	deleted = "deleted"
	// Invalid is the bind info's invalid status.
	Invalid = "invalid"
)

// BindMeta stores the basic bind info and bindSql astNode.
type BindMeta struct {
	*BindRecord
	// HintSet stores the set of hints of binding sql.
	*HintsSet
}

// cache is a k-v map, key is original sql, value is a slice of BindMeta.
type cache map[string][]*BindMeta

// BindRecord represents a sql bind record retrieved from the storage.
type BindRecord struct {
	OriginalSQL string
	BindSQL     string
	Db          string
	// Status represents the status of the binding. It can only be one of the following values:
	// 1. deleted: BindRecord is deleted, can not be used anymore.
	// 2. using: BindRecord is in the normal active mode.
	Status     string
	CreateTime types.Time
	UpdateTime types.Time
	Charset    string
	Collation  string
}

func newBindRecord(row chunk.Row) *BindRecord {
	return &BindRecord{
		OriginalSQL: row.GetString(0),
		BindSQL:     row.GetString(1),
		Db:          row.GetString(2),
		Status:      row.GetString(3),
		CreateTime:  row.GetTime(4),
		UpdateTime:  row.GetTime(5),
		Charset:     row.GetString(6),
		Collation:   row.GetString(7),
	}
}

// size calculates the memory size of a bind meta.
func (m *BindRecord) size() float64 {
	res := len(m.OriginalSQL) + len(m.BindSQL) + len(m.Db) + len(m.Status) + 2*int(unsafe.Sizeof(m.CreateTime)) + len(m.Charset) + len(m.Collation)
	return float64(res)
}

func (m *BindRecord) updateMetrics(scope string, inc bool) {
	if inc {
		metrics.BindMemoryUsage.WithLabelValues(scope, m.Status).Add(float64(m.size()))
		metrics.BindTotalGauge.WithLabelValues(scope, m.Status).Inc()
	} else {
		metrics.BindMemoryUsage.WithLabelValues(scope, m.Status).Sub(float64(m.size()))
		metrics.BindTotalGauge.WithLabelValues(scope, m.Status).Dec()
	}
}
