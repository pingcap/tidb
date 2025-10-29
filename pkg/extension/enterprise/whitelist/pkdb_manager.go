// Copyright 2023-2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package whitelist

import (
	"context"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/atomic"
	"sync"
)

// AccessManager is used to manage client connections
type AccessManager struct {
	sync.Mutex

	ctx      context.Context
	cancel   func()
	sessPool extension.SessionPool

	connControllerRef atomic.Value
	items             []*accessItem
}

// Init inits a manager
func (a *AccessManager) Init() (err error) {
	a.Lock()
	defer a.Unlock()

	// a.ctx != nil means already inited
	if a.ctx != nil {
		return nil
	}

	a.ctx = kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	a.ctx, a.cancel = context.WithCancel(a.ctx)
	defer func() {
		if err != nil {
			a.Close()
		}
	}()

	return a.refreshConnRef()
}

// test parse whitelist table only
func (a *AccessManager) reloadTable() (err error) {
	err = a.LoadIPList()
	if err != nil {
		return err
	}

	failpoint.Inject("mock-whitelist-parse-host", func(val failpoint.Value) {
		host, ok := val.(string)
		if !ok {
			err = errors.New("mock-whitelist-parse-host: invalid value type")
			return
		}
		var h string
		if len(a.items[0].ipList) == 1 {
			h = a.items[0].ipList[0].String()
		} else if len(a.items[0].ipList) == 2 {
			h = a.items[0].ipList[1].String()
		}
		if h != host {
			panic("mock-whitelist-parse-host")
		}
	})
	return nil
}

// Bootstrap bootstraps access manager
func (a *AccessManager) Bootstrap(ctx extension.BootstrapContext) error {
	sessPool := ctx.SessionPool()

	a.Lock()
	if a.ctx == nil {
		a.Unlock()
		return errors.New("not initialized")
	}
	a.sessPool = sessPool
	a.Unlock()

	if err := a.bootstrapSQL(); err != nil {
		return err
	}

	return a.LoadIPList()
}

func (a *AccessManager) bootstrapSQL() error {
	var sqlList []string

	return a.runSQLInExecutor(func(exec sqlexec.SQLExecutor) error {
		for _, sql := range sqlList {
			if _, err := executeSQL(a.ctx, exec, sql); err != nil {
				return err
			}
		}
		return nil
	})
}

func (a *AccessManager) runSQLInExecutor(fn func(sqlexec.SQLExecutor) error) error {
	a.Lock()
	sessPool := a.sessPool
	a.Unlock()

	if sessPool == nil {
		return errors.New("sessPool is not initialized")
	}

	sess, err := sessPool.Get()
	if err != nil {
		return err
	}

	defer func() {
		if sess != nil {
			sessPool.Put(sess)
		}
	}()

	exec, ok := sess.(sqlexec.SQLExecutor)
	if !ok {
		return errors.Errorf("type %T cannot be casted to SQLExecutor", sess)
	}

	return fn(exec)
}

func (a *AccessManager) runInTxn(ctx context.Context, fn func(sqlexec.SQLExecutor) error) error {
	return a.runSQLInExecutor(func(exec sqlexec.SQLExecutor) (err error) {
		done := false
		defer func() {
			if done {
				_, err = executeSQL(ctx, exec, "COMMIT")
			} else {
				_, rollbackErr := exec.ExecuteInternal(ctx, "ROLLBACK")
				terror.Log(rollbackErr)
			}
		}()

		if _, err = executeSQL(ctx, exec, "BEGIN"); err != nil {
			return err
		}

		if err = fn(exec); err != nil {
			return err
		}

		done = true
		return nil
	})
}

// Close closes the whitelist manager
func (a *AccessManager) Close() {
	a.Lock()
	defer a.Unlock()
	a.ctx = nil
	a.connControllerRef.Store(&ConnectionController{})

	if a.cancel != nil {
		a.cancel()
		a.cancel = nil
	}
}

// SetIPList sets the filter
func (a *AccessManager) SetIPList(items []*accessItem) error {
	a.Lock()
	defer a.Unlock()

	a.items = items
	return a.refreshConnRef()
}

func (a *AccessManager) refreshConnRef() error {
	newConnection, err := ConnControllerWithConfig(a.items)
	if err != nil {
		return err
	}

	a.connControllerRef.Store(newConnection)
	return nil
}

// ConnControllerWithConfig returns a controller
func ConnControllerWithConfig(items []*accessItem) (_ *ConnectionController, err error) {
	return &ConnectionController{
		items: items,
	}, nil
}

// LoadIPList reloads filters from table
func (a *AccessManager) LoadIPList() error {
	return a.runInTxn(a.ctx, func(exec sqlexec.SQLExecutor) error {
		ipLists, err := loadIPLists(a.ctx, exec)
		if err != nil {
			return err
		}
		return a.SetIPList(ipLists)
	})
}

func (a *AccessManager) connectionController() *ConnectionController {
	if ref, ok := a.connControllerRef.Load().(*ConnectionController); ok {
		return ref
	}
	return nil
}

// GetSessionHandler returns `*extension.SessionHandler` to register
func (a *AccessManager) GetSessionHandler() *extension.SessionHandler {
	return &extension.SessionHandler{
		OnConnectionEvent: func(tp extension.ConnEventTp, info *extension.ConnEventInfo) {
			// todo only connection event is needed
			failpoint.Inject("mock-whitelist-reload-enable", func() {
				a.reloadTable()
			})
			if controller := a.connectionController(); controller != nil {
				controller.validate(info.ConnectionInfo)
			}
		},
	}
}
