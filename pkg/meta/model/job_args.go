// Copyright 2024 PingCAP, Inc.
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

package model

import (
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// AutoIDGroup represents a group of auto IDs of a specific table.
type AutoIDGroup struct {
	RowID       int64
	IncrementID int64
	RandomID    int64
}

// RecoverTableInfo contains information needed by DDL.RecoverTable.
type RecoverTableInfo struct {
	SchemaID      int64
	TableInfo     *TableInfo
	DropJobID     int64
	SnapshotTS    uint64
	AutoIDs       AutoIDGroup
	OldSchemaName string
	OldTableName  string
}

// RecoverSchemaInfo contains information needed by DDL.RecoverSchema.
type RecoverSchemaInfo struct {
	*DBInfo
	RecoverTableInfos []*RecoverTableInfo
	// LoadTablesOnExecute is the new logic to avoid a large RecoverTabsInfo can't be
	// persisted. If it's true, DDL owner will recover RecoverTabsInfo instead of the
	// job submit node.
	LoadTablesOnExecute bool
	DropJobID           int64
	SnapshotTS          uint64
	OldSchemaName       ast.CIStr
}

// getOrDecodeArgsV1 get the args v1 from job, if the job.Args is nil, decode job.RawArgs
// and cache in job.Args.
// as there is no way to create a generic struct with a type parameter in Go, we
// have to pass one instance of the struct to the function.
func getOrDecodeArgsV1[T JobArgs](args T, job *Job) (T, error) {
	intest.Assert(job.Version == JobVersion1, "job version is not v1")
	var v T
	if err := args.decodeV1(job); err != nil {
		return v, errors.Trace(err)
	}
	return args, nil
}

// getOrDecodeArgsV2 get the args v2 from job, if the job.Args is nil, decode job.RawArgs
// and cache in job.Args.
func getOrDecodeArgsV2[T JobArgs](job *Job) (T, error) {
	intest.Assert(job.Version == JobVersion2, "job version is not v2")
	if len(job.args) > 0 {
		intest.Assert(len(job.args) == 1, "job args length is not 1")
		return job.args[0].(T), nil
	}
	var v T
	if err := json.Unmarshal(job.RawArgs, &v); err != nil {
		return v, errors.Trace(err)
	}
	job.args = []any{v}
	return v, nil
}

func getOrDecodeArgs[T JobArgs](args T, job *Job) (T, error) {
	if job.Version == JobVersion1 {
		return getOrDecodeArgsV1[T](args, job)
	}
	return getOrDecodeArgsV2[T](job)
}

// JobArgs is the interface for job arguments.
type JobArgs interface {
	// getArgsV1 gets the job args for v1. we make it private to avoid calling it
	// directly, use Job.FillArgs to fill the job args.
	getArgsV1(job *Job) []any
	decodeV1(job *Job) error
}

// FinishedJobArgs is the interface for finished job arguments.
// in most cases, job args are cleared out after the job is finished, but some jobs
// will write some args back to the job for other components.
type FinishedJobArgs interface {
	JobArgs
	// getFinishedArgsV1 fills the job args for finished job. we make it private
	// to avoid calling it directly, use Job.FillFinishedArgs to fill the job args.
	getFinishedArgsV1(job *Job) []any
}

// EmptyArgs is the args for ddl job with no args.
type EmptyArgs struct{}

func (*EmptyArgs) getArgsV1(*Job) []any {
	return nil
}

func (*EmptyArgs) decodeV1(*Job) error {
	return nil
}



