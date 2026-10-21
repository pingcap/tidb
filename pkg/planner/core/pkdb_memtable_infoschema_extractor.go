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

package core

import (
	"time"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/types"
)

// HasCreateTimeRange reports whether a usable CREATE_TIME range was extracted.
func (e *InfoSchemaTablesExtractor) HasCreateTimeRange() bool {
	return !e.StartTime.IsZero() || !e.EndTime.IsZero()
}

// IsTimeValid reports whether value is within the inclusive extracted range.
// A zero bound means that side is unrestricted.
func (e *InfoSchemaTablesExtractor) IsTimeValid(value types.Time) bool {
	return (e.StartTime.IsZero() || value.Compare(e.StartTime) >= 0) &&
		(e.EndTime.IsZero() || value.Compare(e.EndTime) <= 0)
}

// Extract implements the MemTablePredicateExtractor interface.
func (e *InfoSchemaTablesExtractor) Extract(
	ctx base.PlanContext,
	schema *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) []expression.Expression {
	remained := e.InfoSchemaBaseExtractor.Extract(ctx, schema, names, predicates)
	e.StartTime, e.EndTime = types.ZeroTime, types.ZeroTime
	if e.SkipRequest {
		return remained
	}

	_, startTime, endTime := e.extractTimeRange(
		ctx,
		schema,
		names,
		remained,
		"create_time",
		time.UTC,
		0,
	)
	if startTime == 0 && endTime == 0 {
		return remained
	}

	// The extracted bounds encode DATETIME fields in UTC, not instants in
	// the session time zone. Preserve those fields for comparison with CREATE_TIME.
	if startTime != 0 {
		e.StartTime = types.NewTime(types.FromGoTime(time.Unix(0, startTime).UTC()), mysql.TypeDatetime, types.MaxFsp)
	}
	if endTime != 0 {
		e.EndTime = types.NewTime(types.FromGoTime(time.Unix(0, endTime).UTC()), mysql.TypeDatetime, types.MaxFsp)
	}
	e.SkipRequest = startTime != 0 && endTime != 0 && startTime > endTime
	// Keep CREATE_TIME predicates in the Selection. Candidate filtering
	// must not define SQL time semantics.
	return remained
}
