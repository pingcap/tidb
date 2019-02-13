// Copyright 2019 PingCAP, Inc.
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

package infobind

import (
	"github.com/pingcap/tidb/sessionctx"
)

// BindManager is used to manage both global bind info and session bind info.
type BindManager struct {
	SessionHandle *Handle
	GlobalHandle  *Handle
}

type keyType int

func (k keyType) String() string {
	return "bind-key"
}

const key keyType = 0

// BindBinderManager binds Manager to context.
func BindBinderManager(ctx sessionctx.Context, pc *BindManager) {
	ctx.SetValue(key, pc)
}
