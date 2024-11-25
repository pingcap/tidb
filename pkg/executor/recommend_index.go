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

package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/indexadvisor"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// RecommendIndexExec represents a recommend index executor.
type RecommendIndexExec struct {
	exec.BaseExecutor

	Action   string
	SQL      string
	AdviseID int64
	Options  []ast.RecommendIndexOption
	done     bool
}

var _ exec.Executor = &RecommendIndexExec{}

// Next implements the Executor Next interface.
func (e *RecommendIndexExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	e.done = true

	if e.Action == "set" {
		return indexadvisor.SetOptions(e.Ctx(), e.Options...)
	}
	if e.Action == "show" {
		return e.showOptions(req)
	}

	if e.Action != "run" {
		return fmt.Errorf("unsupported action: %s", e.Action)
	}

	var sqls []string
	if e.SQL != "" {
		tmp := strings.Split(e.SQL, ";")
		for _, s := range tmp {
			s = strings.TrimSpace(s)
			if s != "" {
				sqls = append(sqls, s)
			}
		}
		if len(sqls) == 0 {
			return errors.New("empty SQLs")
		}
	}
	results, err := indexadvisor.AdviseIndexes(ctx, e.Ctx(), sqls, e.Options)

	for _, r := range results {
		req.AppendString(0, r.Database)
		req.AppendString(1, r.Table)
		req.AppendString(2, r.IndexName)
		req.AppendString(3, strings.Join(r.IndexColumns, ","))
		req.AppendString(4, fmt.Sprintf("%v", r.IndexDetail.IndexSize))
		req.AppendString(5, r.IndexDetail.Reason)

		jData, err := json.Marshal(r.TopImpactedQueries)
		if err != nil {
			return err
		}
		req.AppendString(6, string(jData))
	}
	return err
}

func (e *RecommendIndexExec) showOptions(req *chunk.Chunk) error {
	vals, desc, err := indexadvisor.GetOptions(e.Ctx(), indexadvisor.AllOptions...)
	if err != nil {
		return err
	}
	for _, opt := range indexadvisor.AllOptions {
		if v, ok := vals[opt]; ok {
			req.AppendString(0, opt)
			req.AppendString(1, v)
			req.AppendString(2, desc[opt])
		}
	}
	return nil
}
