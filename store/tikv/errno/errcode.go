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

package errno

// MySQL error code.
// This value is numeric. It is not portable to other database systems.
const (
	ErrUnknown                     = 1105
	ErrLockWaitTimeout             = 1205
	ErrTruncatedWrongValue         = 1292
	ErrQueryInterrupted            = 1317
	ErrDivisionByZero              = 1365
	ErrDataOutOfRange              = 1690
	ErrLockAcquireFailAndNoWaitSet = 3572

	// TiKV/PD/TiFlash errors.
	ErrPDServerTimeout    = 9001
	ErrTiKVServerTimeout  = 9002
	ErrTiKVServerBusy     = 9003
	ErrResolveLockTimeout = 9004
	ErrRegionUnavailable  = 9005
	ErrGCTooEarly         = 9006

	ErrTiKVStoreLimit = 9008

	ErrTiKVStaleCommand          = 9010
	ErrTiKVMaxTimestampNotSynced = 9011
	ErrTiFlashServerTimeout      = 9012
	ErrTiFlashServerBusy         = 9013
)
