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

package session

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

var benchmarkLastTxnInfo string

func newLastTxnInfoTestSession(scope string, startTS uint64) *session {
	sessionVars := variable.NewSessionVars(nil)
	sessionVars.TxnCtx.TxnScope = scope
	sessionVars.TxnCtx.StartTS = startTS
	return &session{sessionVars: sessionVars}
}

func expectedLastTxnInfo(t testing.TB, scope string, startTS uint64) string {
	t.Helper()
	info, err := json.Marshal(transaction.TxnInfo{
		TxnScope: scope,
		StartTS:  startTS,
	})
	require.NoError(t, err)
	return string(info)
}

func TestSetLastTxnInfoBeforeTxnEndJSONCompatibility(t *testing.T) {
	testCases := []struct {
		name    string
		scope   string
		startTS uint64
	}{
		{name: "global-small", scope: kv.GlobalTxnScope, startTS: 1},
		{name: "global-max", scope: kv.GlobalTxnScope, startTS: math.MaxUint64},
		{name: "non-global", scope: "zone-1", startTS: math.MaxUint64},
		{name: "escaped-scope", scope: "zone<>&\"\n\\\u2028", startTS: 123456789},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			se := newLastTxnInfoTestSession(testCase.scope, testCase.startTS)
			se.setLastTxnInfoBeforeTxnEnd()
			require.Equal(t, expectedLastTxnInfo(t, testCase.scope, testCase.startTS), se.sessionVars.LastTxnInfo)
		})
	}

	t.Run("inactive-preserves-value", func(t *testing.T) {
		se := newLastTxnInfoTestSession(kv.GlobalTxnScope, 0)
		se.sessionVars.LastTxnInfo = "previous"
		se.setLastTxnInfoBeforeTxnEnd()
		require.Equal(t, "previous", se.sessionVars.LastTxnInfo)
	})
}

func BenchmarkSetLastTxnInfoBeforeTxnEnd(b *testing.B) {
	testCases := []struct {
		name    string
		scope   string
		startTS uint64
	}{
		{name: "global/max-start-ts", scope: kv.GlobalTxnScope, startTS: math.MaxUint64},
		{name: "global/small-start-ts", scope: kv.GlobalTxnScope, startTS: 1},
		{name: "non-global/escaped-scope", scope: "zone<>&\"\n\\\u2028", startTS: math.MaxUint64},
		{name: "inactive", scope: kv.GlobalTxnScope, startTS: 0},
	}

	for _, testCase := range testCases {
		b.Run(testCase.name, func(b *testing.B) {
			se := newLastTxnInfoTestSession(testCase.scope, testCase.startTS)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				se.setLastTxnInfoBeforeTxnEnd()
			}
			b.StopTimer()
			benchmarkLastTxnInfo = se.sessionVars.LastTxnInfo
		})
	}
}
