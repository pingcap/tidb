// Copyright 2025 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package util

// IHasher is internal usage represent cascades/base.Hasher
type IHasher interface {
	HashBool(val bool)
	HashInt(val int)
	HashInt64(val int64)
	HashUint64(val uint64)
	HashFloat64(val float64)
	HashRune(val rune)
	HashString(val string)
	HashByte(val byte)
	HashBytes(val []byte)
	Reset()
	Sum64() uint64
}
