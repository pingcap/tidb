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
	ids           map[int64][]model.InvolvingSchemaInfo
	runningJobIDs string
}

func newRunningJobs() *runningJobs {
	return &runningJobs{
		ids: make(map[int64][]model.InvolvingSchemaInfo),
	}
}

func (j *runningJobs) add(job *model.Job) {
	j.Lock()
	defer j.Unlock()
	j.ids[job.ID] = getInvolvingSchemaInfo(job)
	j.updateInternalRunningJobIDs()
}

func getInvolvingSchemaInfo(job *model.Job) []model.InvolvingSchemaInfo {
	if len(job.InvolvingSchemaInfo) > 0 {
		return job.InvolvingSchemaInfo
	}
	return []model.InvolvingSchemaInfo{
		{Database: job.SchemaName, Table: job.TableName},
	}
}

func (j *runningJobs) remove(job *model.Job) {
	j.Lock()
	defer j.Unlock()
	delete(j.ids, job.ID)
	j.updateInternalRunningJobIDs()
}

func (j *runningJobs) checkRunnable(job *model.Job) bool {
	j.RLock()
	defer j.RUnlock()
	for _, info := range j.ids {
		for i := 0; i < len(info); i++ {
			jobInfos := getInvolvingSchemaInfo(job)
			for _, jobInfo := range jobInfos {
				if checkConflict(info[i], jobInfo) {
					return false
				}
			}
		}
	}
	return true
}

func checkConflict(runningInvolveInfo, jobInvolveInfo model.InvolvingSchemaInfo) bool {
	dbName, tblName := runningInvolveInfo.Database, runningInvolveInfo.Table
	jobDBName, jobTblName := jobInvolveInfo.Database, jobInvolveInfo.Table
	if dbName == model.InvolvingAll {
		return jobDBName != model.InvolvingNone
	}
	if dbName == model.InvolvingNone {
		return false
	}
	if dbName != jobDBName {
		return false
	}
	if tblName == model.InvolvingAll {
		return jobTblName != model.InvolvingNone
	}
	if tblName == model.InvolvingNone {
		return false
	}
	return tblName == jobTblName
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
