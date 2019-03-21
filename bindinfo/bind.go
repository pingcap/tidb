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

package bindinfo

import (
	"github.com/pingcap/tidb/sessionctx"
)

var _ Manager = (*BindManager)(nil)

// BindManager implements Manager inferface.
type BindManager struct {
	GlobalAccessor GlobalBindAccessor
}

type keyType int

func (k keyType) String() string {
	return "bind-key"
}

// Manager is the interface for providing bind related operations.
type Manager interface {
	AddGlobalBind(originSQL, bindSQL, defaultDB, charset, collation string) error
}

const key keyType = 0

// BindBinder2Manager binds Manager to context.
func BindBinder2Manager(ctx sessionctx.Context, pc Manager) {
	ctx.SetValue(key, pc)
}

// GetBindManager gets Checker from context.
func GetBindManager(ctx sessionctx.Context) Manager {
	if v, ok := ctx.Value(key).(Manager); ok {
		return v
	}
	return nil
}

//AddGlobalBind implements Manager's AddGlobalBind interface.
func (b *BindManager) AddGlobalBind(originSQL, bindSQL, defaultDB, charset, collation string) error {
	return b.GlobalAccessor.AddGlobalBind(originSQL, bindSQL, defaultDB, charset, collation)
}

// GlobalBindAccessor is the interface for accessing global bind info.
type GlobalBindAccessor interface {
	AddGlobalBind(originSQL string, bindSQL string, defaultDB string, charset string, collation string) error
}
