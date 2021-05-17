// Copyright 2021 PingCAP, Inc.

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

package kv

// KeyFlags are metadata associated with key
type KeyFlags uint8

const (
	flagPresumeKNE KeyFlags = 1 << iota
	flagKeyLocked
	flagNeedLocked
	flagKeyLockedValExist
	flagNeedCheckExists
	flagPrewriteOnly
	flagIgnoredIn2PC

	persistentFlags = flagKeyLocked | flagKeyLockedValExist
)

// HasPresumeKeyNotExists returns whether the associated key use lazy check.
func (f KeyFlags) HasPresumeKeyNotExists() bool {
	return f&flagPresumeKNE != 0
}

// HasLocked returns whether the associated key has acquired pessimistic lock.
func (f KeyFlags) HasLocked() bool {
	return f&flagKeyLocked != 0
}

// HasNeedLocked return whether the key needed to be locked
func (f KeyFlags) HasNeedLocked() bool {
	return f&flagNeedLocked != 0
}

// HasLockedValueExists returns whether the value exists when key locked.
func (f KeyFlags) HasLockedValueExists() bool {
	return f&flagKeyLockedValExist != 0
}

// HasNeedCheckExists returns whether the key need to check existence when it has been locked.
func (f KeyFlags) HasNeedCheckExists() bool {
	return f&flagNeedCheckExists != 0
}

// HasPrewriteOnly returns whether the key should be used in 2pc commit phase.
func (f KeyFlags) HasPrewriteOnly() bool {
	return f&flagPrewriteOnly != 0
}

// HasIgnoredIn2PC returns whether the key will be ignored in 2pc.
func (f KeyFlags) HasIgnoredIn2PC() bool {
	return f&flagIgnoredIn2PC != 0
}

// AndPersistent returns the value of current flags&persistentFlags
func (f KeyFlags) AndPersistent() KeyFlags {
	return f & persistentFlags
}

// ApplyFlagsOps applys flagspos to origin.
func ApplyFlagsOps(origin KeyFlags, ops ...FlagsOp) KeyFlags {
	for _, op := range ops {
		switch op {
		case SetPresumeKeyNotExists:
			origin |= flagPresumeKNE | flagNeedCheckExists
		case DelPresumeKeyNotExists:
			origin &= ^(flagPresumeKNE | flagNeedCheckExists)
		case SetKeyLocked:
			origin |= flagKeyLocked
		case DelKeyLocked:
			origin &= ^flagKeyLocked
		case SetNeedLocked:
			origin |= flagNeedLocked
		case DelNeedLocked:
			origin &= ^flagNeedLocked
		case SetKeyLockedValueExists:
			origin |= flagKeyLockedValExist
		case DelNeedCheckExists:
			origin &= ^flagNeedCheckExists
		case SetKeyLockedValueNotExists:
			origin &= ^flagKeyLockedValExist
		case SetPrewriteOnly:
			origin |= flagPrewriteOnly
		case SetIgnoredIn2PC:
			origin |= flagIgnoredIn2PC
		}
	}
	return origin
}

// FlagsOp describes KeyFlags modify operation.
type FlagsOp uint16

const (
	// SetPresumeKeyNotExists marks the existence of the associated key is checked lazily.
	// Implies KeyFlags.HasNeedCheckExists() == true.
	SetPresumeKeyNotExists FlagsOp = 1 << iota
	// DelPresumeKeyNotExists reverts SetPresumeKeyNotExists.
	DelPresumeKeyNotExists
	// SetKeyLocked marks the associated key has acquired lock.
	SetKeyLocked
	// DelKeyLocked reverts SetKeyLocked.
	DelKeyLocked
	// SetNeedLocked marks the associated key need to be acquired lock.
	SetNeedLocked
	// DelNeedLocked reverts SetKeyNeedLocked.
	DelNeedLocked
	// SetKeyLockedValueExists marks the value exists when key has been locked in Transaction.LockKeys.
	SetKeyLockedValueExists
	// SetKeyLockedValueNotExists marks the value doesn't exists when key has been locked in Transaction.LockKeys.
	SetKeyLockedValueNotExists
	// DelNeedCheckExists marks the key no need to be checked in Transaction.LockKeys.
	DelNeedCheckExists
	// SetPrewriteOnly marks the key shouldn't be used in 2pc commit phase.
	SetPrewriteOnly
	// SetIgnoredIn2PC marks the key will be ignored in 2pc.
	SetIgnoredIn2PC
)
