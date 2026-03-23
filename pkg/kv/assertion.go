// Copyright 2026 PingCAP, Inc.
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

// AssertionOp describes assertion operations on keys.
// This is a separate type from FlagsOp to prevent misuse of assertion flags.
type AssertionOp uint8

const (
	// AssertExist marks the associated key must exist.
	AssertExist AssertionOp = iota
	// AssertNotExist marks the associated key must not exist.
	AssertNotExist
	// AssertUnknown marks the associated key is unknown and cannot apply other assertion.
	AssertUnknown
	// AssertNone marks the associated key without any assertion (no-op).
	AssertNone
)

// ApplyAssertionOp applies an assertion operation to KeyFlags.
func ApplyAssertionOp(origin KeyFlags, op AssertionOp) KeyFlags {
	switch op {
	case AssertExist:
		origin |= flagAssertExists
		origin &= ^flagAssertNotExists
	case AssertNotExist:
		origin |= flagAssertNotExists
		origin &= ^flagAssertExists
	case AssertUnknown:
		origin |= flagAssertExists
		origin |= flagAssertNotExists
	case AssertNone:
		// no-op
	}
	return origin
}
