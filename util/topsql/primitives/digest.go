// Copyright 2021 PingCAP, Inc.
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

package primitives

import "github.com/pingcap/tidb/parser"

// SQLPlanDigest is a handy bundle of SQLDigest and PlanDigest that can be used as a Map key.
// This type should be used only in the topsql package. It is not recommended being leaked outside.
//
// TODO: A better abstraction would be "RecordKey", which is decoupled with the content inside. This can be helpful
// when we introduce other information for "Top", like DB and user.
type SQLPlanDigest struct {
	SQLDigest  parser.RawDigestString
	PlanDigest parser.RawDigestString
}

// BuildSQLPlanDigest builds a SQLPlanDigest without allocation.
func BuildSQLPlanDigest(sqlDigest, planDigest parser.RawDigestString) SQLPlanDigest {
	return SQLPlanDigest{
		SQLDigest:  sqlDigest,
		PlanDigest: planDigest,
	}
}
