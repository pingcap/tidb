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
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/tidb/pkg/parser/model"
)

type runningJobs struct {
	sync.RWMutex
	ids           map[int64]struct{}
	runningSchema map[string]map[string]struct{} // database -> table -> struct{}
	runningJobIDs string
}

func newRunningJobs() *runningJobs {
	return &runningJobs{
		ids:           make(map[int64]struct{}),
		runningSchema: make(map[string]map[string]struct{}),
	}
}

func (j *runningJobs) add(job *model.Job) {
	j.Lock()
	defer j.Unlock()
	j.ids[job.ID] = struct{}{}
	j.updateInternalRunningJobIDs()
	for _, info := range job.GetInvolvingSchemaInfo() {
		if _, ok := j.runningSchema[info.Database]; !ok {
			j.runningSchema[info.Database] = make(map[string]struct{})
		}
		j.runningSchema[info.Database][info.Table] = struct{}{}
	}
}

func (j *runningJobs) remove(job *model.Job) {
	j.Lock()
	defer j.Unlock()
	delete(j.ids, job.ID)
	j.updateInternalRunningJobIDs()
	for _, info := range job.GetInvolvingSchemaInfo() {
		if db, ok := j.runningSchema[info.Database]; ok {
			delete(db, info.Table)
		}
		if len(j.runningSchema[info.Database]) == 0 {
			delete(j.runningSchema, info.Database)
		}
	}
}

func (j *runningJobs) checkRunnable(job *model.Job) bool {
	j.RLock()
	defer j.RUnlock()
	for _, info := range job.GetInvolvingSchemaInfo() {
		if _, ok := j.runningSchema[model.InvolvingAll]; ok {
			return false
		}
		if info.Database == model.InvolvingNone {
			continue
		}
		if tbls, ok := j.runningSchema[info.Database]; ok {
			if _, ok := tbls[model.InvolvingAll]; ok {
				return false
			}
			if info.Table == model.InvolvingNone {
				continue
			}
			if _, ok := tbls[info.Table]; ok {
				return false
			}
		}
	}
	return true
}

func (j *runningJobs) allIDs() string {
	j.RLock()
	defer j.RUnlock()
	return j.runningJobIDs
}

func (j *runningJobs) updateInternalRunningJobIDs() {
	var sb strings.Builder
	i := 0
	for id := range j.ids {
		sb.WriteString(strconv.Itoa(int(id)))
		if i != len(j.ids)-1 {
			sb.WriteString(",")
		}
		i++
	}
	j.runningJobIDs = sb.String()
}
