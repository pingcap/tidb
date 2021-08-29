// Copyright 2021 PingCAP, Inc.
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

package kv

// KeyFlags are metadata associated with key
type KeyFlags uint8

const (
	flagPresumeKNE KeyFlags = 1 << iota
	flagNeedLocked
)

// HasPresumeKeyNotExists returns whether the associated key use lazy check.
func (f KeyFlags) HasPresumeKeyNotExists() bool {
	return f&flagPresumeKNE != 0
}

// HasNeedLocked returns whether the key needed to be locked
func (f KeyFlags) HasNeedLocked() bool {
	return f&flagNeedLocked != 0
}

// FlagsOp describes KeyFlags modify operation.
type FlagsOp uint16

const (
	// SetPresumeKeyNotExists marks the existence of the associated key is checked lazily.
	SetPresumeKeyNotExists FlagsOp = iota
	// SetNeedLocked marks the associated key need to be acquired lock.
	SetNeedLocked
)

// ApplyFlagsOps applys flagspos to origin.
func ApplyFlagsOps(origin KeyFlags, ops ...FlagsOp) KeyFlags {
	for _, op := range ops {
		switch op {
		case SetPresumeKeyNotExists:
			origin |= flagPresumeKNE
		case SetNeedLocked:
			origin |= flagNeedLocked
		}
	}
	return origin
}
