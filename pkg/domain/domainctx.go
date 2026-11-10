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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package domain

import (
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
)

// GetDomain gets domain from context.
// might return nil if the session is a cross keyspace one.
func GetDomain(ctx contextutil.ValueStoreContext) *Domain {
	v, ok := ctx.GetDomain().(*Domain)
	if ok {
		return v
	}
	return nil
}

// GetAnalyzeStatsHandle returns the stats handle that ANALYZE uses in the session.
func GetAnalyzeStatsHandle(ctx contextutil.ValueStoreContext) statstypes.AnalyzeStatsHandle {
	h := GetDomain(ctx).StatsHandle()
	if h == nil {
		// A nil *Handle in the interface would not compare equal to nil.
		return nil
	}
	return h
}
