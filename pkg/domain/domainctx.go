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

type statsHandleKeyType struct{}

func (statsHandleKeyType) String() string {
	return "session_stats_handle"
}

// statsHandleKey is the key of the stats handle bound to a session that has no domain.
var statsHandleKey statsHandleKeyType

// BindAnalyzeStatsHandle binds the stats handle of an ANALYZE statement to a session that has no domain,
// i.e. a cross keyspace session. The binding stays for the life of the session.
func BindAnalyzeStatsHandle(ctx contextutil.ValueStoreContext, h statstypes.AnalyzeStatsHandle) {
	ctx.SetValue(statsHandleKey, h)
}

// GetAnalyzeStatsHandle returns the stats handle that ANALYZE uses in the session: the one of
// the session's domain, or the one bound to the session when it has no domain.
func GetAnalyzeStatsHandle(ctx contextutil.ValueStoreContext) statstypes.AnalyzeStatsHandle {
	if do := GetDomain(ctx); do != nil {
		h := do.StatsHandle()
		if h == nil {
			// A nil *Handle in the interface would not compare equal to nil.
			return nil
		}
		return h
	}
	h, _ := ctx.Value(statsHandleKey).(statstypes.AnalyzeStatsHandle)
	return h
}
