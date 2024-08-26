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

package memo

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/cascades/mutil"
	"github.com/pingcap/tidb/pkg/planner/core/base"
)

// Hash64 is the interface for hashcode.
// It is used to calculate the lossy digest of an object to return uint64
// rather than compacted bytes from cascaded operators bottom-up.
type Hash64 interface {
	// Hash64 returns the uint64 digest of an object.
	Hash64(h mutil.Hasher)
}

// Equals is the interface for equality check.
// When we need to compare two objects when countering hash conflicts, we can
// use this interface to check whether they are equal.
type Equals[T any] interface {
	// Equals checks whether two base objects are equal.
	Equals(other T) bool
}

type HashEquals[T any] interface {
	Hash64
	Equals[T]
}

// LogicalOperator is the interface for logical operators.
// It is used to represent the logical operators in the cascades framework.
// Since the LogicalPlan are the only implementation of normal interface{}
// with generic. Once we introduce a new generic HashEquals[T] inside it,
// like:
//
//	type LogicalPlan interface {
//			Plan
//	 	HashEquals[T]
//	 	...
//	}
//
// it will make the every implementor of LogicalPlan changed a lot. That's
// why this upper kind of LogicalOperator interface is introduced. And it
// will be used in the cascades framework.
//
// With this interface, every original LogicalPlan implementor don't need
// to change any old implementations, only have to add the two functions of
// Hash64 and Equals to become a LogicalOperator.
type LogicalOperator[T any] interface {
	base.LogicalPlan
	HashEquals[T]
}

// ScalarOperator is the interface for scalar operators.
// It is used to represent the scalar operators in the cascades framework.
// It is named for expression.Expression with HashEquals[T] interface. For
// the same reason as above LogicalOperator, we introduce this interface to
// avoid changing the original implementation of expression.Expression.
type ScalarOperator[T any] interface {
	expression.Expression
	HashEquals[T]
}
