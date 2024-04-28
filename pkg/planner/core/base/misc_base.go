// Copyright 2024 PingCAP, Inc.
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

package base

import (
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tipb/go-tipb"
)

// AccessObject represents what is accessed by an operator.
// It corresponds to the "access object" column in an EXPLAIN statement result.
type AccessObject interface {
	String() string
	NormalizedString() string
	// SetIntoPB transform itself into a protobuf message and set into the binary plan.
	SetIntoPB(*tipb.ExplainOperator)
}

// ShowPredicateExtractor is used to extract some predicates from `PatternLikeOrIlikeExpr` clause
// and push the predicates down to the data retrieving on reading memory table stage when use ShowStmt.
//
// e.g:
// SHOW COLUMNS FROM t LIKE '%abc%'
// We must request all components from the memory table, and filter the result by the PatternLikeOrIlikeExpr predicate.
//
// it is a way to fix https://github.com/pingcap/tidb/issues/29910.
type ShowPredicateExtractor interface {
	// Extract predicates which can be pushed down and returns whether the extractor can extract predicates.
	Extract() bool
	ExplainInfo() string
	Field() string
	FieldPatternLike() collate.WildcardPattern
}
