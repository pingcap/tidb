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

package staticrecordset

import (
	"context"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

var _ sqlexec.RecordSet = &staticRecordSet{}

type staticRecordSet struct {
	fields   []*resolve.ResultField
	executor exec.Executor

	sqlText string
}

// New creates a new staticRecordSet
func New(fields []*resolve.ResultField, executor exec.Executor, sqlText string) sqlexec.RecordSet {
	return &staticRecordSet{
		fields:   fields,
		executor: executor,
		sqlText:  sqlText,
	}
}

func (s *staticRecordSet) Fields() []*resolve.ResultField {
	return s.fields
}

func (s *staticRecordSet) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		err = util.GetRecoverError(r)
		logutil.Logger(ctx).Error("execute sql panic", zap.String("sql", s.sqlText), zap.Stack("stack"))
	}()

	return exec.Next(ctx, s.executor, req)
}

// NewChunk create a chunk base on top-level executor's exec.NewFirstChunk().
func (s *staticRecordSet) NewChunk(alloc chunk.Allocator) *chunk.Chunk {
	if alloc == nil {
		return exec.NewFirstChunk(s.executor)
	}

	return alloc.Alloc(s.executor.RetFieldTypes(), s.executor.InitCap(), s.executor.MaxChunkSize())
}

// Close closes the executor.
func (s *staticRecordSet) Close() error {
	err := exec.Close(s.executor)
	s.executor = nil

	return err
}

// GetExecutor4Test exports the internal executor for test purpose.
func (s *staticRecordSet) GetExecutor4Test() any {
	return s.executor
}
