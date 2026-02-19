// Copyright 2016 PingCAP, Inc.
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

package privileges

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// Handle wraps MySQLPrivilege providing thread safe access.
type Handle struct {
	sctx util.SessionPool
	priv atomic.Pointer[MySQLPrivilege]
	// Only load the active user's data to save memory
	// username => struct{}
	activeUsers sync.Map
	fullData    atomic.Bool
	globalVars  variable.GlobalVarAccessor
}

// NewHandle returns a Handle.
func NewHandle(sctx util.SessionPool, globalVars variable.GlobalVarAccessor) *Handle {
	priv := newMySQLPrivilege()
	ret := &Handle{}
	ret.sctx = sctx
	ret.globalVars = globalVars
	ret.priv.Store(priv)
	return ret
}

// ensureActiveUser ensure that the specific user data is loaded in-memory.
func (h *Handle) ensureActiveUser(ctx context.Context, user string) error {
	if p := ctx.Value("mock"); p != nil {
		visited := p.(*bool)
		*visited = true
	}
	if h.fullData.Load() {
		// All users data are in-memory, nothing to do
		return nil
	}

	_, exist := h.activeUsers.Load(user)
	if exist {
		return nil
	}
	return h.updateUsers([]string{user})
}

func (h *Handle) merge(data *MySQLPrivilege, userList map[string]struct{}) {
	for {
		old := h.Get()
		swapped := h.priv.CompareAndSwap(old, old.merge(data, userList))
		if swapped {
			break
		}
	}
	for user := range userList {
		h.activeUsers.Store(user, struct{}{})
	}
}

// Get the MySQLPrivilege for read.
func (h *Handle) Get() *MySQLPrivilege {
	return h.priv.Load()
}

// UpdateAll loads all the users' privilege info from kv storage.
func (h *Handle) UpdateAll() error {
	priv := newMySQLPrivilege()
	res, err := h.sctx.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer h.sctx.Put(res)
	exec := res.(sqlexec.SQLExecutor)

	err = priv.LoadAll(exec)
	if err != nil {
		return errors.Trace(err)
	}
	h.priv.Store(priv)
	h.fullData.Store(true)
	return nil
}

// UpdateAllActive loads all the active users' privilege info from kv storage.
func (h *Handle) UpdateAllActive() error {
	h.fullData.Store(false)
	userList := make([]string, 0, 20)
	h.activeUsers.Range(func(key, _ any) bool {
		userList = append(userList, key.(string))
		return true
	})
	metrics.ActiveUser.Set(float64(len(userList)))
	return h.updateUsers(userList)
}

// Update loads the privilege info from kv storage for the list of users.
func (h *Handle) Update(userList []string) error {
	h.fullData.Store(false)
	if len(userList) > 100 {
		logutil.BgLogger().Warn("update user list is long", zap.Int("len", len(userList)))
	}
	needReload := false
	for _, user := range userList {
		if _, ok := h.activeUsers.Load(user); ok {
			needReload = true
			break
		}
	}
	if !needReload {
		return nil
	}

	return h.updateUsers(userList)
}

func (h *Handle) updateUsers(userList []string) error {
	res, err := h.sctx.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer h.sctx.Put(res)
	exec := res.(sqlexec.SQLExecutor)

	p := newMySQLPrivilege()
	p.globalVars = h.globalVars
	// Load the full role edge table first.
	p.roleGraph = make(map[auth.RoleIdentity]roleGraphEdgesTable)
	err = loadTable(exec, sqlLoadRoleGraph, p.decodeRoleEdgesTable)
	if err != nil {
		return errors.Trace(err)
	}

	// Including the user and also their roles
	userAndRoles := findUserAndAllRoles(userList, p.roleGraph)
	err = p.loadSomeUsers(exec, userAndRoles)
	if err != nil {
		return err
	}
	h.merge(p, userAndRoles)
	return nil
}
