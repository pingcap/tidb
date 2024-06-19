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
	schemas           map[string]map[string]struct{}
	placementPolicies map[string]struct{}
	resourceGroups    map[string]struct{}

	shared struct {
		schemas           map[string]map[string]struct{}
		placementPolicies map[string]struct{}
		resourceGroups    map[string]struct{}
	}

	// to implement the fair lock semantics, we need to save the pending exclusive
	// object requests to block future shared object requests.
	pending struct {
		schemas           map[string]map[string]struct{}
		placementPolicies map[string]struct{}
		resourceGroups    map[string]struct{}
	}
}

func newRunningJobs() *runningJobs {
	ret := &runningJobs{
		ids:               make(map[int64]struct{}),
		idsStrGetter:      func() string { return "" },
		schemas:           make(map[string]map[string]struct{}),
		placementPolicies: make(map[string]struct{}),
		resourceGroups:    make(map[string]struct{}),
	}
	ret.shared.schemas = make(map[string]map[string]struct{})
	ret.shared.placementPolicies = make(map[string]struct{})
	ret.shared.resourceGroups = make(map[string]struct{})
	ret.pending.schemas = make(map[string]map[string]struct{})
	ret.pending.placementPolicies = make(map[string]struct{})
	ret.pending.resourceGroups = make(map[string]struct{})

	return ret
}

// checkRunnable checks whether the job can be run. If the caller found a
// runnable job and decides to add it, it must addRunning before next
// checkRunnable invocation. Otherwise, it should addPending before next
// checkRunnable invocation.
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
		if intest.InTest {
			if info.Database == model.InvolvingNone && info.Table != model.InvolvingNone {
				panic(fmt.Sprintf(
					"job %d is invalid. While database is empty, involved table name is not empty: %s",
					jobID, info.Table,
				))
			}
			if info.Database != model.InvolvingNone && info.Table == model.InvolvingNone {
				panic(fmt.Sprintf(
					"job %d is invalid. While table is empty, involved database name is not empty: %s",
					jobID, info.Database,
				))
			}
		}

		// 1. check exclusive objects of involves. Exclusive objects conflicts with
		// running exclusive and shared objects.

		// 1.1 check for Database == model.InvolvingAll, where the only case is FLASHBACK
		// CLUSTER
		if info.Database == model.InvolvingAll {
			if len(j.schemas) != 0 || len(j.shared.schemas) != 0 ||
				len(j.placementPolicies) != 0 || len(j.shared.placementPolicies) != 0 ||
				len(j.resourceGroups) != 0 || len(j.shared.resourceGroups) != 0 {
				return false
			}
			continue
		}

		// 1.2 check schema exclusive objects
		if hasSchemaConflict(info.Database, info.Table, j.schemas) {
			return false
		}
		if hasSchemaConflict(info.Database, info.Table, j.shared.schemas) {
			return false
		}

		// 1.3 check placement policy exclusive objects
		if _, ok := j.placementPolicies[info.Policy]; ok {
			return false
		}
		if _, ok := j.shared.placementPolicies[info.Policy]; ok {
			return false
		}

		// 1.4 check resource group exclusive objects
		if _, ok := j.resourceGroups[info.ResourceGroup]; ok {
			return false
		}
		if _, ok := j.shared.resourceGroups[info.ResourceGroup]; ok {
			return false
		}

		// 2. check shared objects of involves. Shared objects conflicts with running
		// exclusive objects and pending exclusive objects.

		shared := info.Shared
		if shared == nil {
			continue
		}

		// 2.1 check schema shared objects
		if hasSchemaConflict(shared.Database, shared.Table, j.schemas) {
			return false
		}
		if hasSchemaConflict(shared.Database, shared.Table, j.pending.schemas) {
			return false
		}

		// 2.2 check placement policy exclusive objects
		if _, ok := j.placementPolicies[shared.Policy]; ok {
			return false
		}
		if _, ok := j.pending.placementPolicies[shared.Policy]; ok {
			return false
		}

		// 2.3 check resource group exclusive objects
		if _, ok := j.resourceGroups[shared.ResourceGroup]; ok {
			return false
		}
		if _, ok := j.pending.resourceGroups[shared.ResourceGroup]; ok {
			return false
		}
	}
	return true
}

func hasSchemaConflict(
	requestDatabase, requestTable string,
	schemas map[string]map[string]struct{},
) bool {
	tbls, ok := schemas[requestDatabase]
	if !ok {
		return false
	}
	if requestTable == model.InvolvingAll {
		// we rely on no zero-length map exists in table-level. So if the table-level
		// entry exists, it must conflict with InvolvingAll.
		return true
	}
	if _, ok2 := tbls[model.InvolvingAll]; ok2 {
		return true
	}
	if _, ok2 := tbls[requestTable]; ok2 {
		return true
	}
	return false
}

// addRunning should only add the argument that passed the last checkRunnable.
// The added jobs can be removed by removeRunning.
func (j *runningJobs) addRunning(jobID int64, involves []model.InvolvingSchemaInfo) {
	j.mu.Lock()
	defer j.mu.Unlock()

	j.ids[jobID] = struct{}{}
	j.updateIDsStrGetter()

	for _, info := range involves {
		if info.Database != model.InvolvingNone {
			if _, ok := j.schemas[info.Database]; !ok {
				j.schemas[info.Database] = make(map[string]struct{})
			}
			j.schemas[info.Database][info.Table] = struct{}{}
		}
		if info.Policy != model.InvolvingNone {
			j.placementPolicies[info.Policy] = struct{}{}
		}
		if info.ResourceGroup != model.InvolvingNone {
			j.resourceGroups[info.ResourceGroup] = struct{}{}
		}

		shared := info.Shared
		if shared == nil {
			continue
		}
		if shared.Database != model.InvolvingNone {
			if _, ok := j.shared.schemas[shared.Database]; !ok {
				j.shared.schemas[shared.Database] = make(map[string]struct{})
			}
			j.shared.schemas[shared.Database][shared.Table] = struct{}{}
		}
		if shared.Policy != model.InvolvingNone {
			j.shared.placementPolicies[shared.Policy] = struct{}{}
		}
		if shared.ResourceGroup != model.InvolvingNone {
			j.shared.resourceGroups[shared.ResourceGroup] = struct{}{}
		}
	}
}

// removeRunning can be concurrently called with add and checkRunnable.
func (j *runningJobs) removeRunning(jobID int64, involves []model.InvolvingSchemaInfo) {
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
		delete(j.placementPolicies, info.Policy)
		delete(j.resourceGroups, info.ResourceGroup)

		shared := info.Shared
		if shared == nil {
			continue
		}
		if db, ok := j.shared.schemas[shared.Database]; ok {
			delete(db, shared.Table)
		}
		if len(j.shared.schemas[shared.Database]) == 0 {
			delete(j.shared.schemas, shared.Database)
		}
		delete(j.shared.placementPolicies, shared.Policy)
		delete(j.shared.resourceGroups, shared.ResourceGroup)
	}
}

// addPending is used to record the exclusive objects of jobs that can not run,
// to block following jobs which has intersected shared objects with the pending
// jobs. So we can have a "fair lock" semantics.
//
// The pending jobs can be removed by resetAllPending.
func (j *runningJobs) addPending(involves []model.InvolvingSchemaInfo) {
	j.mu.Lock()
	defer j.mu.Unlock()

	for _, info := range involves {
		if info.Database != model.InvolvingNone {
			if _, ok := j.pending.schemas[info.Database]; !ok {
				j.pending.schemas[info.Database] = make(map[string]struct{})
			}
			j.pending.schemas[info.Database][info.Table] = struct{}{}
		}
		if info.Policy != model.InvolvingNone {
			j.pending.placementPolicies[info.Policy] = struct{}{}
		}
		if info.ResourceGroup != model.InvolvingNone {
			j.pending.resourceGroups[info.ResourceGroup] = struct{}{}
		}
	}
}

// resetAllPending should be called when caller finishes the round of getting a
// runnable DDL job.
func (j *runningJobs) resetAllPending() {
	j.mu.Lock()
	defer j.mu.Unlock()

	j.pending.schemas = make(map[string]map[string]struct{})
	j.pending.placementPolicies = make(map[string]struct{})
	j.pending.resourceGroups = make(map[string]struct{})
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
