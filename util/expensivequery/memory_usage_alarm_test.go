// Copyright 2019 PingCAP, Inc.
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

package expensivequery

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIfNeedDoRecord(t *testing.T) {
	record := memoryUsageAlarm{}
	record.initMemoryUsageAlarmRecord()

	// mem usage ratio < 70% will not be recorded
	memUsed := 0.69 * float64(record.serverMemoryQuota)
	assert.False(t, record.needRecord(uint64(memUsed)))

	// mem usage ratio > 70% will not be recorded
	memUsed = 0.71 * float64(record.serverMemoryQuota)
	assert.True(t, record.needRecord(uint64(memUsed)))
	record.lastCheckTime = time.Now()
	record.lastRecordMemUsed = uint64(memUsed)

	// check time - last record time < 60s will not be recorded
	memUsed = 0.71 * float64(record.serverMemoryQuota)
	assert.False(t, record.needRecord(uint64(memUsed)))

	// check time - last record time > 60s will be recorded
	record.lastCheckTime = record.lastCheckTime.Add(-60 * time.Second)
	memUsed = 0.71 * float64(record.serverMemoryQuota)
	assert.True(t, record.needRecord(uint64(memUsed)))
	record.lastCheckTime = time.Now()
	record.lastRecordMemUsed = uint64(memUsed)

	// mem usage ratio - last mem usage ratio < 10% will not be recorded
	memUsed = 0.80 * float64(record.serverMemoryQuota)
	assert.False(t, record.needRecord(uint64(memUsed)))

	// mem usage ratio - last mem usage ratio > 10% will not be recorded even though check time - last record time
	memUsed = 0.82 * float64(record.serverMemoryQuota)
	assert.True(t, record.needRecord(uint64(memUsed)))
}
