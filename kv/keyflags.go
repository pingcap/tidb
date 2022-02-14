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

	// The following are assertion related flags.
	// There are four choices of the two bits:
	// * 0: Assertion is not set and can be set later.
	// * flagAssertExists: We assert the key exists.
	// * flagAssertNotExists: We assert the key doesn't exist.
	// * flagAssertExists | flagAssertNotExists: Assertion cannot be made on this key (unknown).
	// Once either (or both) of the two flags is set, we say assertion is set (`HasAssertionFlags` becomes true), and
	// it's expected to be unchangeable within the current transaction.
	flagAssertExists
	flagAssertNotExists
)

// HasPresumeKeyNotExists returns whether the associated key use lazy check.
func (f KeyFlags) HasPresumeKeyNotExists() bool {
	return f&flagPresumeKNE != 0
}

// HasNeedLocked returns whether the key needed to be locked
func (f KeyFlags) HasNeedLocked() bool {
	return f&flagNeedLocked != 0
}

// HasAssertExists returns whether the key is asserted to already exist before the current transaction.
func (f KeyFlags) HasAssertExists() bool {
	return f&flagAssertExists != 0 && f&flagAssertNotExists == 0
}

// HasAssertNotExists returns whether the key is asserted not to exist before the current transaction.
func (f KeyFlags) HasAssertNotExists() bool {
	return f&flagAssertNotExists != 0 && f&flagAssertExists == 0
}

// HasAssertUnknown returns whether the key is unable to do any assertion.
func (f KeyFlags) HasAssertUnknown() bool {
	return f&flagAssertExists != 0 && f&flagAssertNotExists != 0
}

// HasAssertionFlags returns whether assertion is set on this key.
func (f KeyFlags) HasAssertionFlags() bool {
	return f&flagAssertExists != 0 || f&flagAssertNotExists != 0
}

// FlagsOp describes KeyFlags modify operation.
type FlagsOp uint16

const (
	// SetPresumeKeyNotExists marks the existence of the associated key is checked lazily.
	SetPresumeKeyNotExists FlagsOp = iota
	// SetNeedLocked marks the associated key need to be acquired lock.
	SetNeedLocked
	// SetAssertExist marks the associated key must exist.
	SetAssertExist
	// SetAssertNotExist marks the associated key must not exists.
	SetAssertNotExist
	// SetAssertUnknown marks the associated key is unknown and can not apply other assertion.
	SetAssertUnknown
	// SetAssertNone marks the associated key without any assert.
	SetAssertNone
)

// ApplyFlagsOps applys flagspos to origin.
func ApplyFlagsOps(origin KeyFlags, ops ...FlagsOp) KeyFlags {
	for _, op := range ops {
		switch op {
		case SetPresumeKeyNotExists:
			origin |= flagPresumeKNE
		case SetNeedLocked:
			origin |= flagNeedLocked
		case SetAssertExist:
			origin |= flagAssertExists
			origin &= ^flagAssertNotExists
		case SetAssertNotExist:
			origin |= flagAssertNotExists
			origin &= ^flagAssertExists
		case SetAssertUnknown:
			origin |= flagAssertExists
			origin |= flagAssertNotExists
		}
	}
	return origin
}
