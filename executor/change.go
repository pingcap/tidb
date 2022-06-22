// Copyright 2019 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/tidb-binlog/node"
	"github.com/pingcap/tidb/util/chunk"
)

// ChangeExec represents a change executor.
type ChangeExec struct {
	baseExecutor
	*ast.ChangeStmt
}

// Next implements the Executor Next interface.
func (e *ChangeExec) Next(ctx context.Context, req *chunk.Chunk) error {
	kind := strings.ToLower(e.NodeType)
	urls := config.GetGlobalConfig().Path
	registry, err := createRegistry(urls)
	if err != nil {
		return err
	}
	nodes, _, err := registry.Nodes(ctx, node.NodePrefix[kind])
	if err != nil {
		return err
	}
	state := e.State
	nodeID := e.NodeID
	for _, n := range nodes {
		if n.NodeID != nodeID {
			continue
		}
		switch state {
		case node.Online, node.Pausing, node.Paused, node.Closing, node.Offline:
			n.State = state
			return registry.UpdateNode(ctx, node.NodePrefix[kind], n)
		default:
			return errors.Errorf("state %s is illegal", state)
		}
	}
	return errors.NotFoundf("node %s, id %s from etcd %s", kind, nodeID, urls)
}
