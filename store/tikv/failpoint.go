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
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"github.com/pingcap/failpoint"
)

var (
	// MockRetryableErrorResp mocks an retryable error while processing response
	MockRetryableErrorResp failpoint.Failpoint
	// MockScatterRegionTimeout mocks timeout when trying to scatter region
	MockScatterRegionTimeout failpoint.Failpoint
	// MockSplitRegionTimeout mocks timeout when trying to split region
	MockSplitRegionTimeout failpoint.Failpoint
	// MockPessimisticLockErrWriteConflict mocks
	MockPessimisticLockErrWriteConflict failpoint.Failpoint
	// MockSingleStmtDeadLockRetrySleep mocks
	MockSingleStmtDeadLockRetrySleep failpoint.Failpoint
	// MockBeforeAsyncPessimisticRollback mocks
	MockBeforeAsyncPessimisticRollback failpoint.Failpoint
	// MockBeforePrewrite mocks
	MockBeforePrewrite failpoint.Failpoint
	// MockBeforeSchemaCheck mocks
	MockBeforeSchemaCheck failpoint.Failpoint
	// MockGetTxnStatusDelay mocks
	MockGetTxnStatusDelay failpoint.Failpoint
	// MockHandleTaskOnceError mocks
	MockHandleTaskOnceError failpoint.Failpoint
	// MockCommitError mocks
	MockCommitError failpoint.Failpoint
	// MockGetTSErrorInRetry mocks
	MockGetTSErrorInRetry failpoint.Failpoint
	// MockRetrySendReqToRegion mocks
	MockRetrySendReqToRegion failpoint.Failpoint
	// MockSyncBinlogCommit mocks
	MockSyncBinlogCommit failpoint.Failpoint
	// MockProbeSetVars mocks
	MockProbeSetVars failpoint.Failpoint
	// MockTestRateLimitActionMockConsumeAndAssert mocks
	MockTestRateLimitActionMockConsumeAndAssert failpoint.Failpoint
	// MockTestRateLimitActionMockWaitMax mocks
	MockTestRateLimitActionMockWaitMax failpoint.Failpoint
	// MockTicase4169 mocks
	MockTicase4169 failpoint.Failpoint
	// MockTicase4170 mocks
	MockTicase4170 failpoint.Failpoint
	// MockTicase4171 mocks
	MockTicase4171 failpoint.Failpoint
	// MockTikvStoreRespResult mocks
	MockTikvStoreRespResult failpoint.Failpoint
	// MockTikvStoreSendReqResult mocks
	MockTikvStoreSendReqResult failpoint.Failpoint
	// MockTxnExpireRetTTL mocks
	MockTxnExpireRetTTL failpoint.Failpoint
	// MockTxnNotFoundRetTTL mocks
	MockTxnNotFoundRetTTL failpoint.Failpoint
)
