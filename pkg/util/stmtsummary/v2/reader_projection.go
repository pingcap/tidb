// Copyright 2026 PingCAP, Inc.
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

package stmtsummary

import (
	"encoding/json"
	"reflect"
	"sync"

	"github.com/pingcap/tidb/pkg/meta/model"
)

// stmtRecordProjection avoids allocating unused SQL and plan strings. Other
// fields retain their normal decoders, including validation of unused counters,
// times and collections and the fields required for filtering and privileges.
type stmtRecordProjection struct {
	recordType reflect.Type
}

// Keep these bits in the same order as projectedTextFields.
const (
	projectedNormalizedSQL uint8 = 1 << iota
	projectedBindingSQL
	projectedSampleSQL
	projectedPrevSQL
	projectedSamplePlan
	projectedSampleBinaryPlan
	projectedPlanHint
	projectedPlanCacheUnqualifiedLastReason
	projectedAllText = projectedNormalizedSQL | projectedBindingSQL | projectedSampleSQL | projectedPrevSQL |
		projectedSamplePlan | projectedSampleBinaryPlan | projectedPlanHint | projectedPlanCacheUnqualifiedLastReason
)

var projectedTextFields = [...]struct {
	name     string
	jsonName string
}{
	{"NormalizedSQL", "normalized_sql"},
	{"BindingSQL", "binding_sql"},
	{"SampleSQL", "sample_sql"},
	{"PrevSQL", "prev_sql"},
	{"SamplePlan", "sample_plan"},
	{"SampleBinaryPlan", "sample_binary_plan"},
	{"PlanHint", "plan_hint"},
	{"PlanCacheUnqualifiedLastReason", "plan_cache_unqualified_last_reason"},
}

// Eight text fields permit at most 256 decoder types, regardless of the number
// of queries or column combinations. Each immutable type is built only once.
var stmtRecordProjections [1 << len(projectedTextFields)]struct {
	once       sync.Once
	projection stmtRecordProjection
}

func makeStmtRecordProjection(columns []*model.ColumnInfo) *stmtRecordProjection {
	var selected uint8
	for _, column := range columns {
		switch column.Name.O {
		case DigestTextStr:
			selected |= projectedNormalizedSQL
		case BindingDigestTextStr:
			selected |= projectedBindingSQL
		case QuerySampleTextStr:
			selected |= projectedSampleSQL
		case PrevSampleTextStr:
			selected |= projectedPrevSQL
		case PlanStr:
			// PLAN's error diagnostic also includes the normalized SQL.
			selected |= projectedSamplePlan | projectedNormalizedSQL
		case BinaryPlan:
			selected |= projectedSampleBinaryPlan
		case PlanHint:
			selected |= projectedPlanHint
		case PlanCacheUnqualifiedLastReasonStr:
			selected |= projectedPlanCacheUnqualifiedLastReason
		}
	}
	if selected == projectedAllText {
		return nil
	}
	cached := &stmtRecordProjections[selected]
	cached.once.Do(func() {
		fields := []reflect.StructField{
			{Name: "StmtRecord", Type: reflect.TypeOf(StmtRecord{}), Anonymous: true},
			{Name: "Evicted", Type: reflect.TypeOf(false), Tag: `json:"evicted"`},
		}
		for i, field := range projectedTextFields {
			if selected&(1<<i) == 0 {
				fields = append(fields, reflect.StructField{
					Name: field.name,
					Type: reflect.TypeOf(discardedString{}),
					Tag:  reflect.StructTag(`json:"` + field.jsonName + `"`),
				})
			}
		}
		cached.projection.recordType = reflect.StructOf(fields)
	})
	return &cached.projection
}

// discardedString shadows only unrequested text. Selected strings remain in the
// embedded StmtRecord and use the standard decoder without a second parse.
type discardedString struct{}

// UnmarshalJSON preserves string/null acceptance even for unrequested fields.
// encoding/json already validates the complete document's syntax and escapes.
func (*discardedString) UnmarshalJSON(raw []byte) error {
	if raw[0] == '"' || string(raw) == "null" {
		return nil
	}
	// Keep rejecting a malformed known field rather than accepting a row that
	// the full-record decoder would have discarded. This is a cold error path.
	var unused string
	return json.Unmarshal(raw, &unused)
}

func (p *stmtRecordProjection) parse(raw []byte) (*StmtRecord, bool, error) {
	record := reflect.New(p.recordType)
	if err := json.Unmarshal(raw, record.Interface()); err != nil {
		return nil, false, err
	}
	if record.Elem().Field(1).Bool() {
		return nil, true, nil
	}
	return record.Elem().Field(0).Addr().Interface().(*StmtRecord), false, nil
}
