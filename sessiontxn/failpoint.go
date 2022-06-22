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

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/stringutil"
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

// HookBeforeFirstRunExecutorKey is the hook key for the executor first run
// Only for test
var HookBeforeFirstRunExecutorKey stringutil.StringerStr = "testHookKeyBeforeFirstRunExecutor"

// HookAfterOnStmtRetryWithLockErrorKey is the hook key for after OnStmtRetry with lock error
// Only for test
var HookAfterOnStmtRetryWithLockErrorKey stringutil.StringerStr = "testHookKeyAfterOnStmtRetryWithLockError"

// RecordAssert is used only for test
func RecordAssert(sctx sessionctx.Context, name string, value interface{}) {
	records, ok := sctx.Value(AssertRecordsKey).(map[string]interface{})
	if !ok {
		records = make(map[string]interface{})
		sctx.SetValue(AssertRecordsKey, records)
	}
	records[name] = value
}

// AssertTxnManagerInfoSchema is used only for test
func AssertTxnManagerInfoSchema(sctx sessionctx.Context, is interface{}) {
	assertVersion := func(expected interface{}) {
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
		got, ok := GetTxnManager(sctx).GetTxnInfoSchema().(*infoschema.TemporaryTableAttachedInfoSchema)
		if !ok {
			panic("Expected to be a TemporaryTableAttachedInfoSchema")
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
