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

package sessiontxn

import (
	"fmt"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

// AssertRecordsKey is used to save failPoint invoke records
// Only for test
var AssertRecordsKey stringutil.StringerStr = "assertTxnManagerRecords"

// AssertTxnInfoSchemaKey is used to set the expected infoschema that should be check in failPoint
// Only for test
var AssertTxnInfoSchemaKey stringutil.StringerStr = "assertTxnInfoSchemaKey"

// AssertTxnInfoSchemaAfterRetryKey is used to set the expected infoschema that should be check in failPoint after retry
// Only for test
var AssertTxnInfoSchemaAfterRetryKey stringutil.StringerStr = "assertTxnInfoSchemaAfterRetryKey"

// BreakPointBeforeExecutorFirstRun is the key for the stop point where session stops before executor's first run
// Only for test
var BreakPointBeforeExecutorFirstRun = "beforeExecutorFirstRun"

// BreakPointOnStmtRetryAfterLockError s the key for the stop point where session stops after OnStmtRetry when lock error happens
// Only for test
var BreakPointOnStmtRetryAfterLockError = "lockErrorAndThenOnStmtRetryCalled"

// TsoRequestCount is the key for recording tso request counts in some places
var TsoRequestCount stringutil.StringerStr = "tsoRequestCount"

// TsoWaitCount doesn't include begin and commit
var TsoWaitCount stringutil.StringerStr = "tsoWaitCount"

// TsoUseConstantCount is the key for constant tso counter
var TsoUseConstantCount stringutil.StringerStr = "tsoUseConstantCount"

// CallOnStmtRetryCount is the key for recording calling OnStmtRetry at RC isolation level
var CallOnStmtRetryCount stringutil.StringerStr = "callOnStmtRetryCount"

// AssertLockErr is used to record the lock errors we encountered
// Only for test
var AssertLockErr stringutil.StringerStr = "assertLockError"

// RecordAssert is used only for test
func RecordAssert(sctx sessionctx.Context, name string, value any) {
	records, ok := sctx.Value(AssertRecordsKey).(map[string]any)
	if !ok {
		records = make(map[string]any)
		sctx.SetValue(AssertRecordsKey, records)
	}
	records[name] = value
}

// AssertTxnManagerInfoSchema is used only for test
func AssertTxnManagerInfoSchema(sctx sessionctx.Context, is any) {
	assertVersion := func(expected any) {
		if expected == nil {
			return
		}

		expectVer := expected.(infoschema.InfoSchema).SchemaMetaVersion()
		gotVer := GetTxnManager(sctx).GetTxnInfoSchema().SchemaMetaVersion()
		if gotVer != expectVer {
			panic(fmt.Sprintf("Txn schema version not match, expect:%d, got:%d", expectVer, gotVer))
		}
	}

	if localTables := sctx.GetSessionVars().LocalTemporaryTables; localTables != nil {
		got, ok := GetTxnManager(sctx).GetTxnInfoSchema().(*infoschema.SessionExtendedInfoSchema)
		if !ok {
			panic("Expected to be a SessionExtendedInfoSchema")
		}

		if got.LocalTemporaryTables != localTables {
			panic("Local tables should be the same with the one in session")
		}
	}

	assertVersion(is)
	assertVersion(sctx.Value(AssertTxnInfoSchemaKey))
}

// AssertTxnManagerReadTS is used only for test
func AssertTxnManagerReadTS(sctx sessionctx.Context, expected uint64) {
	actual, err := GetTxnManager(sctx).GetStmtReadTS()
	if err != nil {
		panic(err)
	}

	if actual != expected {
		panic(fmt.Sprintf("Txn read ts not match, expect:%d, got:%d", expected, actual))
	}
}

// AddAssertEntranceForLockError is used only for test
func AddAssertEntranceForLockError(sctx sessionctx.Context, name string) {
	records, ok := sctx.Value(AssertLockErr).(map[string]int)
	if !ok {
		records = make(map[string]int)
		sctx.SetValue(AssertLockErr, records)
	}
	if v, ok := records[name]; ok {
		records[name] = v + 1
	} else {
		records[name] = 1
	}
}

// TsoRequestCountInc is used only for test
// When it is called, there is a tso cmd request.
func TsoRequestCountInc(sctx sessionctx.Context) {
	count, ok := sctx.Value(TsoRequestCount).(uint64)
	if !ok {
		count = 0
	}
	count++
	sctx.SetValue(TsoRequestCount, count)
}

// TsoWaitCountInc is used only for test
// When it is called, there is a waiting tso operation
func TsoWaitCountInc(sctx sessionctx.Context) {
	count, ok := sctx.Value(TsoWaitCount).(uint64)
	if !ok {
		count = 0
	}
	count++
	sctx.SetValue(TsoWaitCount, count)
}

// TsoUseConstantCountInc is used to test constant tso count
func TsoUseConstantCountInc(sctx sessionctx.Context) {
	count, ok := sctx.Value(TsoUseConstantCount).(uint64)
	if !ok {
		count = 0
	}
	count++
	sctx.SetValue(TsoUseConstantCount, count)
}

// OnStmtRetryCountInc is used only for test.
// When it is called, there is calling `(p *PessimisticRCTxnContextProvider) OnStmtRetry`.
func OnStmtRetryCountInc(sctx sessionctx.Context) {
	count, ok := sctx.Value(CallOnStmtRetryCount).(int)
	if !ok {
		count = 0
	}
	count++
	sctx.SetValue(CallOnStmtRetryCount, count)
}

// ExecTestHook is used only for test. It consumes hookKey in session wait do what it gets from it.
func ExecTestHook(sctx sessionctx.Context, hookKey fmt.Stringer) {
	c := sctx.Value(hookKey)
	if ch, ok := c.(chan func()); ok {
		select {
		case fn := <-ch:
			fn()
		case <-time.After(time.Second * 10):
			panic("timeout waiting for chan")
		}
	}
}
