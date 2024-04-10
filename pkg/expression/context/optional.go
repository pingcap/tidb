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

package context

import (
	"fmt"
	"log"
	"unsafe"

	"github.com/pingcap/tidb/pkg/util/intest"
)

func init() {
	// Ensure the count of optional properties is less than the bits of OptionalEvalProps.
	if maxCnt := int64(unsafe.Sizeof(*new(OptionalEvalPropKeySet))) * 8; int64(OptPropsCnt) > maxCnt {
		log.Fatalf(
			"The count optional properties should less than the bits of OptionalEvalProps, but %d > %d",
			OptPropsCnt, maxCnt,
		)
	}

	// check optionalPropertyDescList are valid
	if len(optionalPropertyDescList) != OptPropsCnt {
		log.Fatalf(
			"The lenghth of OptionalPropertieDescs should be %d, but got %d",
			OptPropsCnt, len(optionalPropertyDescList),
		)
	}

	for i := 0; i < OptPropsCnt; i++ {
		if key := optionalPropertyDescList[i].Key(); key != OptionalEvalPropKey(i) {
			log.Fatalf(
				"Invalid optionalPropertyDescList[%d].Key, unexpected index: %d",
				i, key,
			)
		}
	}
}

// OptionalEvalPropKey is the key for optional evaluation properties in EvalContext.
type OptionalEvalPropKey int

// AsPropKeySet returns the set only contains the property key.
func (k OptionalEvalPropKey) AsPropKeySet() OptionalEvalPropKeySet {
	return 1 << k
}

// Desc returns the description for the property key.
func (k OptionalEvalPropKey) Desc() *OptionalEvalPropDesc {
	return &optionalPropertyDescList[k]
}

// String implements fmt.Stringer interface.
func (k OptionalEvalPropKey) String() string {
	if k < optPropsCnt {
		return k.Desc().str
	}
	return fmt.Sprintf("UnknownOptionalEvalPropKey(%d)", k)
}

const (
	// OptPropCurrentUser indicates to provide the current user property.
	OptPropCurrentUser OptionalEvalPropKey = iota
	// OptPropSessionVars indicates to provide `variable.SessionVariable`.
	OptPropSessionVars
	// OptPropInfoSchema indicates to provide the information schema.
	OptPropInfoSchema
	// OptPropKVStore indicates to provide the kv store.
	OptPropKVStore
	// OptPropSQLExecutor indicates to provide executors to execute sql.
	OptPropSQLExecutor
	// OptPropSequenceOperator indicates to provide sequence operators for sequences.
	OptPropSequenceOperator
	// OptPropAdvisoryLock indicates to provide advisory lock operations.
	OptPropAdvisoryLock
	// OptPropDDLOwnerInfo indicates to provide DDL owner information.
	OptPropDDLOwnerInfo
	// optPropsCnt is the count of optional properties. DO NOT use it as a property key.
	optPropsCnt
)

const allOptPropsMask = (1 << optPropsCnt) - 1

// OptPropsCnt is the count of optional properties.
const OptPropsCnt = int(optPropsCnt)

// OptionalEvalPropDesc is the description for optional evaluation properties in EvalContext.
type OptionalEvalPropDesc struct {
	key OptionalEvalPropKey
	str string
	// TODO: add more fields if needed
}

// Key returns the property key.
func (desc *OptionalEvalPropDesc) Key() OptionalEvalPropKey {
	return desc.key
}

// OptionalEvalPropProvider is the interface to provide optional properties in EvalContext.
type OptionalEvalPropProvider interface {
	Desc() *OptionalEvalPropDesc
}

// optionalPropertyDescList contains all optional property descriptions in EvalContext.
var optionalPropertyDescList = []OptionalEvalPropDesc{
	{
		key: OptPropCurrentUser,
		str: "OptPropCurrentUser",
	},
	{
		key: OptPropSessionVars,
		str: "OptPropSessionVars",
	},
	{
		key: OptPropInfoSchema,
		str: "OptPropInfoSchema",
	},
	{
		key: OptPropKVStore,
		str: "OptPropKVStore",
	},
	{
		key: OptPropSQLExecutor,
		str: "OptPropSQLExecutor",
	},
	{
		key: OptPropSequenceOperator,
		str: "OptPropDDLOwnerInfo",
	},
	{
		key: OptPropAdvisoryLock,
		str: "OptPropAdvisoryLock",
	},
	{
		key: OptPropDDLOwnerInfo,
		str: "OptPropDDLOwnerInfo",
	},
}

// OptionalEvalPropKeySet is a bit map for optional evaluation properties in EvalContext
// to indicate whether some properties are set.
type OptionalEvalPropKeySet uint64

// Add adds the property key to the set
func (b OptionalEvalPropKeySet) Add(key OptionalEvalPropKey) OptionalEvalPropKeySet {
	intest.Assert(key < optPropsCnt)
	if key >= optPropsCnt {
		return b
	}
	return b | key.AsPropKeySet()
}

// Remove removes the property key from the set
func (b OptionalEvalPropKeySet) Remove(key OptionalEvalPropKey) OptionalEvalPropKeySet {
	intest.Assert(key < optPropsCnt)
	if key >= optPropsCnt {
		return b
	}
	return b &^ key.AsPropKeySet()
}

// Contains checks whether the set contains the property
func (b OptionalEvalPropKeySet) Contains(key OptionalEvalPropKey) bool {
	intest.Assert(key < optPropsCnt)
	if key >= optPropsCnt {
		return false
	}
	return b&key.AsPropKeySet() != 0
}

// IsEmpty checks whether the bit map is empty.
func (b OptionalEvalPropKeySet) IsEmpty() bool {
	return b&allOptPropsMask == 0
}

// IsFull checks whether all optional properties are contained in the bit map.
func (b OptionalEvalPropKeySet) IsFull() bool {
	return b&allOptPropsMask == allOptPropsMask
}
