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

package types

import "strings"

// String is a lightweight string contract for time parsing APIs.
// It lets callers mark unsafe string sources (for example, zero-copy chunk buffers)
// so error arguments can be frozen by pingcap/errors before deferred formatting.
type String interface {
	String() string
}

// PlainStr is the default string wrapper for stable string sources.
type PlainStr string

// String returns the string value.
func (s PlainStr) String() string {
	return string(s)
}

// HackedStr marks strings that may alias mutable buffers. It implements
// errors.HackedStr so pingcap/errors freezes the argument on error construction
// and avoids later mutations showing up in warning/error messages.
type HackedStr string

// String returns the string value.
func (s HackedStr) String() string {
	return string(s)
}

// FreezeStr clones the string to detach from mutable backing storage.
func (s HackedStr) FreezeStr() string {
	return strings.Clone(string(s))
}
