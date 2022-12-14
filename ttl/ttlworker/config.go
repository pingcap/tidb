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
)

const jobManagerLoopTickerInterval = 10 * time.Second

const updateInfoSchemaCacheInterval = 2 * time.Minute
const updateTTLTableStatusCacheInterval = 2 * time.Minute

const ttlInternalSQLTimeout = 30 * time.Second
const resizeWorkersInterval = 30 * time.Second
const splitScanCount = 64
const ttlJobTimeout = 6 * time.Hour
