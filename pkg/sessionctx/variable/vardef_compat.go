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

package variable

import "github.com/pingcap/tidb/pkg/sessionctx/vardef"

// Deprecated: use vardef.ScopeFlag.
type ScopeFlag = vardef.ScopeFlag

// Deprecated: use vardef.TypeFlag.
type TypeFlag = vardef.TypeFlag

const (
	// Deprecated: use vardef.ScopeNone.
	ScopeNone = vardef.ScopeNone
	// Deprecated: use vardef.ScopeGlobal.
	ScopeGlobal = vardef.ScopeGlobal
	// Deprecated: use vardef.ScopeSession.
	ScopeSession = vardef.ScopeSession
	// Deprecated: use vardef.ScopeInstance.
	ScopeInstance = vardef.ScopeInstance

	// Deprecated: use vardef.TypeStr.
	TypeStr = vardef.TypeStr
	// Deprecated: use vardef.TypeBool.
	TypeBool = vardef.TypeBool
	// Deprecated: use vardef.TypeInt.
	TypeInt = vardef.TypeInt
	// Deprecated: use vardef.TypeEnum.
	TypeEnum = vardef.TypeEnum
	// Deprecated: use vardef.TypeFloat.
	TypeFloat = vardef.TypeFloat
	// Deprecated: use vardef.TypeUnsigned.
	TypeUnsigned = vardef.TypeUnsigned
	// Deprecated: use vardef.TypeTime.
	TypeTime = vardef.TypeTime
	// Deprecated: use vardef.TypeDuration.
	TypeDuration = vardef.TypeDuration
)
