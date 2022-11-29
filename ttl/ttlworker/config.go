// Copyright 2022 PingCAP, Inc.
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

package ttlworker

import (
	"time"

	"go.uber.org/atomic"
)

// TODO: the following functions should be put in the variable pkg to avoid cyclic dependency after adding variables for the TTL
// some of them are only used in test

const jobManagerLoopTickerInterval = 10 * time.Second

const updateInfoSchemaCacheInterval = time.Minute
const updateTTLTableStatusCacheInterval = 10 * time.Minute

const ttlInternalSQLTimeout = 30 * time.Second
const ttlJobTimeout = 6 * time.Hour

// TODO: add this variable to the sysvar
const ttlJobInterval = time.Hour

// TODO: add these variables to the sysvar
var ttlJobScheduleWindowStartTime, _ = time.Parse(timeFormat, "2006-01-02 00:00:00")
var ttlJobScheduleWindowEndTime, _ = time.Parse(timeFormat, "2006-01-02 23:59:00")

// TODO: migrate these two count to sysvar

// ScanWorkersCount defines the count of scan worker
var ScanWorkersCount = atomic.NewUint64(0)

// DeleteWorkerCount defines the count of delete worker
var DeleteWorkerCount = atomic.NewUint64(0)

const resizeWorkersInterval = 30 * time.Second
const splitScanCount = 64
