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

package operator

import "strings"

// AsyncPipeline wraps a list of Operators.
// The dataflow is from the first operator to the last operator.
type AsyncPipeline struct {
	ops []Operator
}

// Execute opens all operators, it's run asynchronously.
func (p *AsyncPipeline) Execute() error {
	// Start running each operator.
	for i, op := range p.ops {
		err := op.Open()
		if err != nil {
			// Close all operators that have been opened.
			for j := i - 1; j >= 0; j-- {
				_ = p.ops[j].Close()
			}
			return err
		}
	}
	return nil
}

// Close waits all tasks done.
func (p *AsyncPipeline) Close() error {
	var firstErr error
	for _, op := range p.ops {
		err := op.Close()
		if firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// NewAsyncPipeline creates a new AsyncPipeline.
func NewAsyncPipeline(ops ...Operator) *AsyncPipeline {
	return &AsyncPipeline{
		ops: ops,
	}
}

// String shows the pipeline.
func (p *AsyncPipeline) String() string {
	opStrs := make([]string, len(p.ops))
	for i, op := range p.ops {
		opStrs[i] = op.String()
	}
	return "AsyncPipeline[" + strings.Join(opStrs, " -> ") + "]"
}
