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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIterScanTask(t *testing.T) {
	tbl := newMockTTLTbl(t, "t1")

	job := &ttlJob{
		tbl:   tbl,
		tasks: []*ttlScanTask{{}},
	}
	scanTask, err := job.peekScanTask()
	assert.NoError(t, err)
	assert.NotNil(t, scanTask)
	assert.Len(t, job.tasks, 1)

	job.nextScanTask()
	assert.True(t, job.AllSpawned())
}
