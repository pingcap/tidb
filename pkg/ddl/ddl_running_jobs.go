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

	"github.com/pingcap/tidb/pkg/meta/model"
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

	exclusive *objects
	shared    *objects
	// to implement the fair lock semantics, we need to save the pending exclusive
	// object requests to block future shared object requests.
	pending *objects
}

// objects uses maps to count the involved number of objects. If the count is
// zero, the entry will be deleted from the map to keep map size small.
type objects struct {
	// database -> table -> struct{}
	//
	// if the job is only related to a database, the table-level entry key is
	// model.InvolvingAll. When remove a job, runningJobs will make sure no
	// zero-length map exists in table-level.
	schemas           map[string]map[string]int
	placementPolicies map[string]int
	resourceGroups    map[string]int
}

func newObjects() *objects {
	return &objects{
		schemas:           make(map[string]map[string]int),
		placementPolicies: make(map[string]int),
		resourceGroups:    make(map[string]int),
	}
}

func (o *objects) empty() bool {
	return len(o.schemas) == 0 && len(o.placementPolicies) == 0 && len(o.resourceGroups) == 0
}

func newRunningJobs() *runningJobs {
	return &runningJobs{
		ids:          make(map[int64]struct{}),
		idsStrGetter: func() string { return "" },
		exclusive:    newObjects(),
		shared:       newObjects(),
		pending:      newObjects(),
	}
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
	if _, ok := j.exclusive.schemas[model.InvolvingAll]; ok {
		return false
	}

	if j.exclusive.empty() && j.shared.empty() && j.pending.empty() {
		return true
	}

	for _, info := range involves {
		intest.Assert(
			!(info.Database == model.InvolvingNone && info.Table != model.InvolvingNone),
			"job %d is invalid. While database is empty, involved table name is not empty: %s",
			jobID, info.Table,
		)
		intest.Assert(
			!(info.Database != model.InvolvingNone && info.Table == model.InvolvingNone),
			"job %d is invalid. While table is empty, involved database name is not empty: %s",
			jobID, info.Database,
		)

		if info.Database == model.InvolvingAll && info.Table == model.InvolvingAll &&
			info.Mode == model.ExclusiveInvolving {
			// check for involving all databases and tables, where the only case is FLASHBACK
			// CLUSTER. Because now runningJobs is not totally empty, we can return false.
			return false
		}

		var toCheck []*objects
		switch info.Mode {
		case model.ExclusiveInvolving:
			// Exclusive objects conflicts with running exclusive and shared objects. And
			// because shared will be concurrently removed by removeRunning in another
			// goroutine, we also check pending objects.
			toCheck = []*objects{j.exclusive, j.shared, j.pending}
		case model.SharedInvolving:
			// Shared objects conflicts with running exclusive objects and pending exclusive
			// objects.
			toCheck = []*objects{j.exclusive, j.pending}
		default:
			panic(fmt.Sprintf("unknown involving mode: %d", info.Mode))
		}

		for _, checkingObj := range toCheck {
			if info.Database != model.InvolvingNone {
				if hasSchemaConflict(info.Database, info.Table, checkingObj.schemas) {
					return false
				}
				// model.InvolvingSchemaInfo is like an enumerate type
				intest.Assert(
					info.Policy == "" && info.ResourceGroup == "",
					"InvolvingSchemaInfo should be like an enumerate type: %#v",
					info,
				)
				continue
			}

			if info.Policy != "" {
				if _, ok := checkingObj.placementPolicies[info.Policy]; ok {
					return false
				}
				intest.Assert(
					info.ResourceGroup == "",
					"InvolvingSchemaInfo should be like an enumerate type: %#v",
					info,
				)
				continue
			}
			intest.Assert(
				info.ResourceGroup != "",
				"InvolvingSchemaInfo should be like an enumerate type: %#v",
				info,
			)
			if _, ok := checkingObj.resourceGroups[info.ResourceGroup]; ok {
				return false
			}
		}
	}
	return true
}

func hasSchemaConflict(
	requestDatabase, requestTable string,
	schemas map[string]map[string]int,
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
		var toAdd *objects
		switch info.Mode {
		case model.ExclusiveInvolving:
			toAdd = j.exclusive
		case model.SharedInvolving:
			toAdd = j.shared
		default:
			panic(fmt.Sprintf("unknown involving mode: %d", info.Mode))
		}

		if info.Database != model.InvolvingNone {
			if _, ok := toAdd.schemas[info.Database]; !ok {
				toAdd.schemas[info.Database] = make(map[string]int)
			}
			toAdd.schemas[info.Database][info.Table]++
		}
		if info.Policy != model.InvolvingNone {
			toAdd.placementPolicies[info.Policy]++
		}
		if info.ResourceGroup != model.InvolvingNone {
			toAdd.resourceGroups[info.ResourceGroup]++
		}
	}
}

func (j *runningJobs) finishOrPendJob(jobID int64, involves []model.InvolvingSchemaInfo, moveToPending bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.removeRunningWithoutLock(jobID, involves)
	if moveToPending {
		j.addPendingWithoutLock(involves)
	}
}

// removeRunning can be concurrently called with add and checkRunnable.
func (j *runningJobs) removeRunning(jobID int64, involves []model.InvolvingSchemaInfo) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.removeRunningWithoutLock(jobID, involves)
}

func (j *runningJobs) removeRunningWithoutLock(jobID int64, involves []model.InvolvingSchemaInfo) {
	if intest.InTest {
		if _, ok := j.ids[jobID]; !ok {
			panic(fmt.Sprintf("job %d is not running", jobID))
		}
	}
	delete(j.ids, jobID)
	j.updateIDsStrGetter()

	for _, info := range involves {
		var toRemove *objects
		switch info.Mode {
		case model.ExclusiveInvolving:
			toRemove = j.exclusive
		case model.SharedInvolving:
			toRemove = j.shared
		default:
			panic(fmt.Sprintf("unknown involving mode: %d", info.Mode))
		}

		if info.Database != model.InvolvingNone {
			if db, ok := toRemove.schemas[info.Database]; ok {
				if info.Table != model.InvolvingNone {
					db[info.Table]--
					if db[info.Table] == 0 {
						delete(db, info.Table)
					}
				}
			}
			if len(toRemove.schemas[info.Database]) == 0 {
				delete(toRemove.schemas, info.Database)
			}
		}

		if len(info.Policy) > 0 {
			toRemove.placementPolicies[info.Policy]--
			if toRemove.placementPolicies[info.Policy] == 0 {
				delete(toRemove.placementPolicies, info.Policy)
			}
		}

		if len(info.ResourceGroup) > 0 {
			toRemove.resourceGroups[info.ResourceGroup]--
			if toRemove.resourceGroups[info.ResourceGroup] == 0 {
				delete(toRemove.resourceGroups, info.ResourceGroup)
			}
		}
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

	j.addPendingWithoutLock(involves)
}

func (j *runningJobs) addPendingWithoutLock(involves []model.InvolvingSchemaInfo) {
	for _, info := range involves {
		if info.Database != model.InvolvingNone {
			if _, ok := j.pending.schemas[info.Database]; !ok {
				j.pending.schemas[info.Database] = make(map[string]int)
			}
			j.pending.schemas[info.Database][info.Table]++
		}
		if info.Policy != model.InvolvingNone {
			j.pending.placementPolicies[info.Policy]++
		}
		if info.ResourceGroup != model.InvolvingNone {
			j.pending.resourceGroups[info.ResourceGroup]++
		}
	}
}

// resetAllPending should be called when caller finishes the round of getting a
// runnable DDL job.
func (j *runningJobs) resetAllPending() {
	j.mu.Lock()
	defer j.mu.Unlock()

	j.pending = newObjects()
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
