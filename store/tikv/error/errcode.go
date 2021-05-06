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

package error

// MySQL error code.
// This value is numeric. It is not portable to other database systems.
const (
	CodeUnknown                     = 1105
	CodeLockWaitTimeout             = 1205
	CodeTruncatedWrongValue         = 1292
	CodeQueryInterrupted            = 1317
	CodeDivisionByZero              = 1365
	CodeDataOutOfRange              = 1690
	CodeLockAcquireFailAndNoWaitSet = 3572

	// TiKV/PD/TiFlash errors.
	CodePDServerTimeout   = 9001
	CodeTiKVServerBusy    = 9003
	CodeRegionUnavailable = 9005
	CodeGCTooEarly        = 9006

	CodeTiKVStoreLimit = 9008

	CodeTiFlashServerTimeout = 9012
	CodeTiFlashServerBusy    = 9013
)
