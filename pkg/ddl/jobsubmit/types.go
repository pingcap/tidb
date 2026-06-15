// Copyright 2026 PingCAP, Inc.
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

// Package jobsubmit contains synchronous DDL job construction and durable
// enqueue primitives shared by DDL submitters.
package jobsubmit

import (
	"github.com/pingcap/tidb/pkg/ddl/serverstate"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/ddl/systable"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
)

// JobSpec wraps the durable job metadata and typed job arguments needed by the
// direct submit path.
type JobSpec struct {
	Job         *model.Job
	Args        model.JobArgs
	IDAllocated bool
}

// SubmitOptions lists the external dependencies needed to enqueue DDL jobs.
type SubmitOptions struct {
	Store             kv.Storage
	SessPool          *sess.Pool
	SysTblMgr         systable.Manager
	MinJobIDRefresher *systable.MinJobIDRefresher
	ServerStateSyncer serverstate.Syncer

	// BeforeInsertWithAssignedIDs runs after job IDs are assigned and before the
	// insert attempt. The returned cleanup runs if that transaction attempt fails.
	BeforeInsertWithAssignedIDs func(specs []*JobSpec) (cleanup func())
}
