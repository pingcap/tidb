// Copyright 2022 PingCAP, Inc.
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

package extension

import (
	"context"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/chunk"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// SessionPool is the pool for session
type SessionPool interface {
	Get() (pools.Resource, error)
	Put(pools.Resource)
}

// Option represents an option to initialize an extension
type Option func(m *Manifest)

// WithCustomSysVariables specifies custom variables of an extension
func WithCustomSysVariables(vars []*variable.SysVar) Option {
	return func(m *Manifest) {
		m.sysVariables = vars
	}
}

// WithCustomDynPrivs specifies dynamic privileges of an extension
func WithCustomDynPrivs(privs []string) Option {
	return func(m *Manifest) {
		m.dynPrivs = privs
	}
}

// WithCustomFunctions specifies custom functions
func WithCustomFunctions(funcs []*FunctionDef) Option {
	return func(m *Manifest) {
		m.funcs = funcs
	}
}

// AccessCheckFunc is a function that returns a dynamic privilege list for db/tbl/column access
type AccessCheckFunc func(db, tbl, column string, priv mysql.PrivilegeType, sem bool) []string

// WithCustomAccessCheck specifies the custom db/tbl/column dynamic privilege check
func WithCustomAccessCheck(fn AccessCheckFunc) Option {
	return func(m *Manifest) {
		m.accessCheckFunc = fn
	}
}

// WithSessionHandlerFactory specifies a factory function to handle session
func WithSessionHandlerFactory(factory func() *SessionHandler) Option {
	return func(m *Manifest) {
		m.sessionHandlerFactory = factory
	}
}

// WithClose specifies the close function of an extension.
// It will be invoked when `extension.Reset` is called
func WithClose(fn func()) Option {
	return func(m *Manifest) {
		m.close = fn
	}
}

// BootstrapContext is the context used by extension in bootstrap
type BootstrapContext interface {
	context.Context
	// ExecuteSQL is used to execute a sql
	ExecuteSQL(ctx context.Context, sql string) ([]chunk.Row, error)
	// EtcdClient returns the etcd client
	EtcdClient() *clientv3.Client
	// SessionPool returns the session pool of domain
	SessionPool() SessionPool
}

// WithBootstrap specifies the bootstrap func of an extension
func WithBootstrap(fn func(BootstrapContext) error) Option {
	return func(m *Manifest) {
		m.bootstrap = fn
	}
}

// WithBootstrapSQL the bootstrap SQL list
func WithBootstrapSQL(sqlList ...string) Option {
	return WithBootstrap(func(ctx BootstrapContext) error {
		for _, sql := range sqlList {
			if _, err := ctx.ExecuteSQL(ctx, sql); err != nil {
				return err
			}
		}
		return nil
	})
}

// Manifest is an extension's manifest
type Manifest struct {
	name                  string
	sysVariables          []*variable.SysVar
	dynPrivs              []string
	bootstrap             func(BootstrapContext) error
	funcs                 []*FunctionDef
	accessCheckFunc       AccessCheckFunc
	sessionHandlerFactory func() *SessionHandler
	close                 func()
}

// Name returns the extension's name
func (m *Manifest) Name() string {
	return m.name
}

func newManifestWithSetup(name string, factory func() ([]Option, error)) (_ *Manifest, _ func(), err error) {
	clearBuilder := &clearFuncBuilder{}
	defer func() {
		if err != nil {
			clearBuilder.Build()()
		}
	}()

	// new manifest with factory
	m := &Manifest{name: name}
	err = clearBuilder.DoWithCollectClear(func() (func(), error) {
		options, err := factory()
		if err != nil {
			return nil, err
		}

		for _, opt := range options {
			opt(m)
		}

		return m.close, nil
	})

	if err != nil {
		return nil, nil, err
	}

	// setup dynamic privileges
	for i := range m.dynPrivs {
		priv := m.dynPrivs[i]
		err = clearBuilder.DoWithCollectClear(func() (func(), error) {
			if err = RegisterDynamicPrivilege(priv); err != nil {
				return nil, err
			}
			return func() {
				RemoveDynamicPrivilege(priv)
			}, nil
		})
		if err != nil {
			return nil, nil, err
		}
	}

	// setup sys vars
	for i := range m.sysVariables {
		sysVar := m.sysVariables[i]
		err = clearBuilder.DoWithCollectClear(func() (func(), error) {
			if sysVar == nil {
				return nil, errors.New("system var should not be nil")
			}

			if sysVar.Name == "" {
				return nil, errors.New("system var name should not be empty")
			}

			if variable.GetSysVar(sysVar.Name) != nil {
				return nil, errors.Errorf("system var '%s' has already registered", sysVar.Name)
			}

			variable.RegisterSysVar(sysVar)
			return func() {
				variable.UnregisterSysVar(sysVar.Name)
			}, nil
		})

		if err != nil {
			return nil, nil, err
		}
	}

	// setup functions
	for i := range m.funcs {
		def := m.funcs[i]
		err = clearBuilder.DoWithCollectClear(func() (func(), error) {
			if err := RegisterExtensionFunc(def); err != nil {
				return nil, err
			}

			return func() {
				RemoveExtensionFunc(def.Name)
			}, nil
		})

		if err != nil {
			return nil, nil, err
		}
	}

	return m, clearBuilder.Build(), nil
}

// RegisterDynamicPrivilege is used to resolve dependency cycle
var RegisterDynamicPrivilege func(string) error

// RemoveDynamicPrivilege is used to resolve dependency cycle
var RemoveDynamicPrivilege func(string) bool
