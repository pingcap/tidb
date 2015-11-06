// Copyright 2015 PingCAP, Inc.
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

package optimizer

// Aggregator is the interface to
// compute aggregate function result.
type Aggregator interface {
	// Input adds an input value to aggregator.
	// The input values are accumulated in the aggregator.
	Input(in ...interface{}) error
	// Output uses input values to compute the aggregated result.
	Output() interface{}
	// Clear clears the input values.
	Clear()
}
