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

package pattern

// EngineType is determined by whether it's above or below `Gather`s.
// Plan will choose the different engine to be implemented/executed on according to its EngineType.
// Different engine may support different operators with different cost, so we should design
// different transformation and implementation rules for each engine.
type EngineType uint

const (
	// EngineTiDB stands for groups which is above `Gather`s and will be executed in TiDB layer.
	EngineTiDB EngineType = 1 << iota
	// EngineTiKV stands for groups which is below `Gather`s and will be executed in TiKV layer.
	EngineTiKV
	// EngineTiFlash stands for groups which is below `Gather`s and will be executed in TiFlash layer.
	EngineTiFlash
)

// EngineTypeSet is the bit set of EngineTypes.
type EngineTypeSet uint

const (
	// EngineTiDBOnly is the EngineTypeSet for EngineTiDB only.
	EngineTiDBOnly = EngineTypeSet(EngineTiDB)
	// EngineTiKVOnly is the EngineTypeSet for EngineTiKV only.
	EngineTiKVOnly = EngineTypeSet(EngineTiKV)
	// EngineTiFlashOnly is the EngineTypeSet for EngineTiFlash only.
	EngineTiFlashOnly = EngineTypeSet(EngineTiFlash)
	// EngineTiKVOrTiFlash is the EngineTypeSet for (EngineTiKV | EngineTiFlash).
	EngineTiKVOrTiFlash = EngineTypeSet(EngineTiKV | EngineTiFlash)
	// EngineAll is the EngineTypeSet for all of the EngineTypes.
	EngineAll = EngineTypeSet(EngineTiDB | EngineTiKV | EngineTiFlash)
)

// Contains checks whether the EngineTypeSet contains the EngineType.
func (e EngineTypeSet) Contains(tp EngineType) bool {
	return uint(e)&uint(tp) != 0
}

// String implements fmt.Stringer interface.
func (e EngineType) String() string {
	switch e {
	case EngineTiDB:
		return "EngineTiDB"
	case EngineTiKV:
		return "EngineTiKV"
	case EngineTiFlash:
		return "EngineTiFlash"
	}
	return "UnknownEngineType"
}
