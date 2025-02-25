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
	"time"

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type runningJobs struct {
	sync.RWMutex
	// processingIDs records the IDs of the jobs that are being processed by a worker.
	processingIDs    map[int64]struct{}
	processingIDsStr string

	// unfinishedIDs records the IDs of the jobs that are not finished yet.
	// It is not necessarily being processed by a worker.
	unfinishedIDs    map[int64]struct{}
	unfinishedSchema map[string]map[string]struct{} // database -> table -> struct{}

	// processingReorgJobID records the ID of the ingest job that is being processed by a worker.
	// TODO(tangenta): remove this when we support running multiple concurrent ingest jobs.
	processingIngestJobID int64
	lastLoggingTime       time.Time
}

func newRunningJobs() *runningJobs {
	return &runningJobs{
		processingIDs:    make(map[int64]struct{}),
		unfinishedSchema: make(map[string]map[string]struct{}),
		unfinishedIDs:    make(map[int64]struct{}),
	}
}

func (j *runningJobs) clear() {
	j.Lock()
	defer j.Unlock()
	j.unfinishedIDs = make(map[int64]struct{})
	j.unfinishedSchema = make(map[string]map[string]struct{})
}

func (j *runningJobs) add(job *model.Job) {
	j.Lock()
	defer j.Unlock()
	j.processingIDs[job.ID] = struct{}{}
	j.updateInternalRunningJobIDs()
	if isIngestJob(job) {
		j.processingIngestJobID = job.ID
	}

	if _, ok := j.unfinishedIDs[job.ID]; ok {
		// Already exists, no need to add it again.
		return
	}
	j.unfinishedIDs[job.ID] = struct{}{}
	for _, info := range job.GetInvolvingSchemaInfo() {
		if _, ok := j.unfinishedSchema[info.Database]; !ok {
			j.unfinishedSchema[info.Database] = make(map[string]struct{})
		}
		j.unfinishedSchema[info.Database][info.Table] = struct{}{}
	}
}

func (j *runningJobs) remove(job *model.Job) {
	j.Lock()
	defer j.Unlock()
	delete(j.processingIDs, job.ID)
	j.updateInternalRunningJobIDs()
	if isIngestJob(job) && job.ID == j.processingIngestJobID {
		j.processingIngestJobID = 0
	}

	if job.IsFinished() || job.IsSynced() {
		delete(j.unfinishedIDs, job.ID)
		for _, info := range job.GetInvolvingSchemaInfo() {
			if db, ok := j.unfinishedSchema[info.Database]; ok {
				delete(db, info.Table)
			}
			if len(j.unfinishedSchema[info.Database]) == 0 {
				delete(j.unfinishedSchema, info.Database)
			}
		}
	}
}

func (j *runningJobs) allIDs() string {
	j.RLock()
	defer j.RUnlock()
	return j.processingIDsStr
}

func (j *runningJobs) updateInternalRunningJobIDs() {
	var sb strings.Builder
	i := 0
	for id := range j.processingIDs {
		sb.WriteString(strconv.Itoa(int(id)))
		if i != len(j.processingIDs)-1 {
			sb.WriteString(",")
		}
		i++
	}
	j.processingIDsStr = sb.String()
}

func (j *runningJobs) checkRunnable(job *model.Job) bool {
	j.RLock()
	defer j.RUnlock()
	if _, ok := j.processingIDs[job.ID]; ok {
		// Already processing by a worker. Skip running it again.
		return false
	}
	if isIngestJob(job) && j.processingIngestJobID != 0 {
		// We only allow one task to use ingest at the same time in order to limit the CPU/memory usage.
		if time.Since(j.lastLoggingTime) > 1*time.Minute {
			logutil.BgLogger().Info("ingest backfill worker is already in used by another DDL job",
				zap.String("category", "ddl-ingest"),
				zap.Int64("processing job ID", j.processingIngestJobID))
			j.lastLoggingTime = time.Now()
		}
		return false
	}
	for _, info := range job.GetInvolvingSchemaInfo() {
		if _, ok := j.unfinishedSchema[model.InvolvingAll]; ok {
			return false
		}
		if info.Database == model.InvolvingNone {
			continue
		}
		if tbls, ok := j.unfinishedSchema[info.Database]; ok {
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

func isIngestJob(job *model.Job) bool {
	return (job.Type == model.ActionAddIndex || job.Type == model.ActionAddPrimaryKey) &&
		job.ReorgMeta != nil &&
		IsEnableFastReorg()
}
