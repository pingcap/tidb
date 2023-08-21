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

// AsyncPipeline wraps a list of Operators.
// The dataflow is from the first operator to the last operator.
type AsyncPipeline struct {
	ops []Operator
}

// Execute starts all operators waiting to handle tasks.
func (p *AsyncPipeline) Execute() error {
	// Start running each operator.
	for _, op := range p.ops {
		err := op.Open()
		if err != nil {
			return err
		}
	}
	return nil
}

// Close waits all tasks done.
func (p *AsyncPipeline) Close() {
	for _, op := range p.ops {
		op.Close()
	}
}

// NewAsyncPipeline creates a new AsyncPipeline.
func NewAsyncPipeline(ops ...Operator) *AsyncPipeline {
	return &AsyncPipeline{
		ops: ops,
	}
}

// Display shows the pipeline.
func (p *AsyncPipeline) Display() string {
	level := 0
	res := ""
	for i, op := range p.ops {
		for j := 0; j < level; j++ {
			res += " "
		}
		res += op.Display()
		if i != len(p.ops)-1 {
			res += "\n"
		}
		level++
	}
	return res
}
