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

package operators

import (
	"github.com/pingcap/tidb/disttask/operator"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/chunk"
)

var _ operator.Operator = (*tableScanOperator)(nil)

type tableScanTask struct {
	id       int
	startKey kv.Key
	endKey   kv.Key
}

type idxRecResult struct {
	id    int
	chunk *chunk.Chunk
	err   error
	done  bool
}

type tableScanOperator struct {
	operator.AsyncOperator[tableScanTask, idxRecResult]
}

func newTableScanOperator() *tableScanOperator {
	return &tableScanOperator{
		AsyncOperator: operator.NewAsyncOperatorWithTransform[tableScanTask, idxRecResult](
			"tableScanOperator",
			1,

	}
}

func (t *tableScanOperator) transform(task tableScanTask) idxRecResult {
	return nil
}

func (t *tableScanOperator) Open() error {
	return nil
}

func (t *tableScanOperator) Close() error {
	return nil
}

func (t *tableScanOperator) String() string {
	return "tableScanOperator"
}

func (t *tableScanOperator) SetSource(channel operator.DataChannel[tableScanTask]) {

}

func (t *tableScanOperator) SetSink(channel operator.DataChannel[idxRecResult]) {

}
