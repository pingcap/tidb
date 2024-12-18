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

package base

// Hash64 is the interface for hashcode.
// It is used to calculate the lossy digest of an object to return uint64
// rather than compacted bytes from cascaded operators bottom-up.
type Hash64 interface {
	// Hash64 returns the uint64 digest of an object.
	Hash64(h Hasher)
}

// Equals is the interface for equality check.
// When we need to compare two objects when countering hash conflicts, we can
// use this interface to check whether they are equal.
type Equals interface {
	// Equals checks whether two base objects are equal.
	Equals(any) bool
}

// HashEquals is the interface for hash64 and equality check.
type HashEquals interface {
	Hash64
	Equals
}
