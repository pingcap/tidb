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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx/variable"
)

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

// WithClose specifies the close function of an extension.
// It will be invoked when `extension.Reset` is called
func WithClose(fn func()) Option {
	return func(m *Manifest) {
		m.close = fn
	}
}

// Manifest is an extension's manifest
type Manifest struct {
	name         string
	sysVariables []*variable.SysVar
	dynPrivs     []string
	close        func()
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
	return m, clearBuilder.Build(), nil
}

// RegisterDynamicPrivilege is used to resolve dependency cycle
var RegisterDynamicPrivilege func(string) error

// RemoveDynamicPrivilege is used to resolve dependency cycle
var RemoveDynamicPrivilege func(string) bool
