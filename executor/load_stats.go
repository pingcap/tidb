// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/util/chunk"
	goctx "golang.org/x/net/context"
)

var _ Executor = &LoadStatsExec{}

// LoadStatsExec represents a load statistic executor.
type LoadStatsExec struct {
	baseExecutor
	info *LoadStatsInfo
}

// LoadStatsInfo saves the information of loading statistic operation.
type LoadStatsInfo struct {
	Path string
	Ctx  context.Context
}

// loadStatsVarKeyType is a dummy type to avoid naming collision in context.
type loadStatsVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k loadStatsVarKeyType) String() string {
	return "load_stats_var"
}

// LoadStatsVarKey is a variable key for load statistic.
const LoadStatsVarKey loadStatsVarKeyType = 0

func (e *LoadStatsExec) exec(goCtx goctx.Context) (Row, error) {
	if len(e.info.Path) == 0 {
		return nil, errors.New("Load Stats: file path is empty")
	}
	ctx := e.ctx
	val := ctx.Value(LoadStatsVarKey)
	if val != nil {
		ctx.SetValue(LoadStatsVarKey, nil)
		return nil, errors.New("Load Stats: previous load stats option isn't closed normally")
	}
	ctx.SetValue(LoadStatsVarKey, e.info)

	return nil, nil
}

// Next implements the Executor Next interface.
func (e *LoadStatsExec) Next(goCtx goctx.Context) (Row, error) {
	return e.exec(goCtx)
}

// NextChunk implements the Executor NextChunk interface.
func (e *LoadStatsExec) NextChunk(goCtx goctx.Context, chk *chunk.Chunk) error {
	chk.Reset()
	_, err := e.exec(goCtx)
	return errors.Trace(err)
}

// Close implements the Executor Close interface.
func (e *LoadStatsExec) Close() error {
	return nil
}

// Open implements the Executor Open interface.
func (e *LoadStatsExec) Open(goCtx goctx.Context) error {
	return nil
}
