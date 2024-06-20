// Copyright 2023 PingCAP, Inc.
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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package ddl

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/util/intest"
)

type runningJobs struct {
	// although most of the usage is called by jobScheduler in a single goroutine,
	// runningJobs.remove is called by the worker goroutine. Another implementation
	// is let worker goroutine send the finished job to jobScheduler, and
	// jobScheduler calls remove, so no need to lock.
	mu sync.RWMutex

	ids          map[int64]struct{}
	idsStrGetter func() string
	// database -> table -> struct{}
	//
	// if the job is only related to a database, the table-level entry key is
	// model.InvolvingAll. When remove a job, runningJobs will make sure no
	// zero-length map exists in table-level.
	schemas map[string]map[string]struct{}
}

// TODO(lance6716): support jobs involving objects that are exclusive (ALTER
// PLACEMENT POLICY vs ALTER PLACEMENT POLICY) and shared (ALTER TABLE PLACEMENT
// POLICY vs ALTER TABLE PLACEMENT POLICY)
func newRunningJobs() *runningJobs {
	return &runningJobs{
		ids:          make(map[int64]struct{}),
		idsStrGetter: func() string { return "" },
		schemas:      make(map[string]map[string]struct{}),
	}
}

// add should only add the argument that passed the last checkRunnable.
func (j *runningJobs) add(jobID int64, involves []model.InvolvingSchemaInfo) {
	j.mu.Lock()
	defer j.mu.Unlock()

	j.ids[jobID] = struct{}{}
	j.updateIDsStrGetter()

	for _, info := range involves {
		// DDL jobs related to placement policies and resource groups
		if info.Database == model.InvolvingNone {
			// should not happen
			if intest.InTest {
				if info.Table != model.InvolvingNone {
					panic(fmt.Sprintf(
						"job %d is invalid, involved table name is not empty: %s",
						jobID, info.Table,
					))
				}
			}
			continue
		}

		if _, ok := j.schemas[info.Database]; !ok {
			j.schemas[info.Database] = make(map[string]struct{})
		}
		j.schemas[info.Database][info.Table] = struct{}{}
	}
}

// remove can be concurrently called with add and checkRunnable.
func (j *runningJobs) remove(jobID int64, involves []model.InvolvingSchemaInfo) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if intest.InTest {
		if _, ok := j.ids[jobID]; !ok {
			panic(fmt.Sprintf("job %d is not running", jobID))
		}
	}
	delete(j.ids, jobID)
	j.updateIDsStrGetter()

	for _, info := range involves {
		if db, ok := j.schemas[info.Database]; ok {
			delete(db, info.Table)
		}
		if len(j.schemas[info.Database]) == 0 {
			delete(j.schemas, info.Database)
		}
	}
}

func (j *runningJobs) updateIDsStrGetter() {
	var (
		once   sync.Once
		idsStr string
	)
	j.idsStrGetter = func() string {
		once.Do(func() {
			var sb strings.Builder
			i := 0
			for id := range j.ids {
				sb.WriteString(strconv.Itoa(int(id)))
				if i != len(j.ids)-1 {
					sb.WriteString(",")
				}
				i++
			}
			idsStr = sb.String()
		})
		return idsStr
	}
}

func (j *runningJobs) allIDs() string {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.idsStrGetter()
}

// checkRunnable checks whether the job can be run. If the caller found a
// runnable job and decides to add it, it must add before next checkRunnable
// invocation.
func (j *runningJobs) checkRunnable(jobID int64, involves []model.InvolvingSchemaInfo) bool {
	j.mu.RLock()
	defer j.mu.RUnlock()

	if _, ok := j.ids[jobID]; ok {
		// should not happen
		if intest.InTest {
			panic(fmt.Sprintf("job %d is already running", jobID))
		}
		return false
	}
	// Currently flashback cluster is the only DDL that involves ALL schemas.
	if _, ok := j.schemas[model.InvolvingAll]; ok {
		return false
	}

	for _, info := range involves {
		if info.Database == model.InvolvingAll {
			if len(j.schemas) != 0 {
				return false
			}
			continue
		}

		tbls, ok := j.schemas[info.Database]
		if !ok {
			continue
		}
		if info.Table == model.InvolvingAll {
			return false
		}
		if _, ok2 := tbls[model.InvolvingAll]; ok2 {
			return false
		}
		if _, ok2 := tbls[info.Table]; ok2 {
			return false
		}
	}
	return true
}
