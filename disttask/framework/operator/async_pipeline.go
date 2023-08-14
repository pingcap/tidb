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

// AsyncPipeline wraps a list of BaseAsyncOperatorImpl.
// The dataflow is from the first operator to the last operator.
type AsyncPipeline struct {
	ops []AsyncOperator
}

// Execute start all operators waiting to handle tasks.
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

// Close wait all tasks done.
func (p *AsyncPipeline) Close() {
	for _, op := range p.ops {
		op.Close()
	}
}

// AddOperator insert operator to the end of the list
func (p *AsyncPipeline) AddOperator(op AsyncOperator) {
	p.ops = append(p.ops, op)
}

// FirstOperator get the first operator.
func (p *AsyncPipeline) FirstOperator() AsyncOperator {
	return p.ops[0]
}

// LastOperator get the last operator.
func (p *AsyncPipeline) LastOperator() AsyncOperator {
	return p.ops[len(p.ops)-1]
}
