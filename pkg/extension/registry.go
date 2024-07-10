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
	"sort"
	"sync"

	"github.com/pingcap/errors"
)

type registry struct {
	sync.RWMutex
	factories      map[string]func() ([]Option, error)
	extensionNames []string
	setup          bool
	extensions     *Extensions
	close          func()
}

// Setup sets up the extensions
func (r *registry) Setup() error {
	r.Lock()
	defer r.Unlock()

	if _, err := r.doSetup(); err != nil {
		return err
	}
	return nil
}

// Extensions returns the extensions after setup
func (r *registry) Extensions() (*Extensions, error) {
	r.RLock()
	if r.setup {
		extensions := r.extensions
		r.RUnlock()
		return extensions, nil
	}
	r.RUnlock()

	r.Lock()
	defer r.Unlock()
	return r.doSetup()
}

// RegisterFactory registers a new extension with a factory
func (r *registry) RegisterFactory(name string, factory func() ([]Option, error)) error {
	r.Lock()
	defer r.Unlock()

	if r.setup {
		return errors.New("Cannot register new extension because registry has already been setup")
	}

	if name == "" {
		return errors.New("extension name should not be empty")
	}

	if _, ok := r.factories[name]; ok {
		return errors.Errorf("extension with name '%s' already registered", name)
	}

	if r.factories == nil {
		r.factories = make(map[string]func() ([]Option, error))
	}

	r.factories[name] = factory
	r.extensionNames = append(r.extensionNames, name)
	sort.Strings(r.extensionNames)
	return nil
}

// Setup setups all extensions
func (r *registry) doSetup() (_ *Extensions, err error) {
	if r.setup {
		return r.extensions, nil
	}

	if len(r.factories) == 0 {
		r.extensions = nil
		r.setup = true
		return nil, nil
	}

	clearBuilder := &clearFuncBuilder{}
	defer func() {
		if err != nil {
			clearBuilder.Build()()
		}
	}()

	manifests := make([]*Manifest, 0, len(r.factories))
	for i := range r.extensionNames {
		name := r.extensionNames[i]
		err = clearBuilder.DoWithCollectClear(func() (func(), error) {
			factory := r.factories[name]
			m, clear, err := newManifestWithSetup(name, factory)
			if err != nil {
				return nil, err
			}
			manifests = append(manifests, m)
			return clear, nil
		})

		if err != nil {
			return nil, err
		}
	}
	r.extensions = &Extensions{manifests: manifests}
	r.setup = true
	r.close = clearBuilder.Build()
	return r.extensions, nil
}

// Reset resets the registry. It is only used by test
func (r *registry) Reset() {
	r.Lock()
	defer r.Unlock()

	if r.close != nil {
		r.close()
		r.close = nil
	}
	r.factories = nil
	r.extensionNames = nil
	r.extensions = nil
	r.setup = false
}

var globalRegistry registry

// RegisterFactory registers a new extension with a factory
func RegisterFactory(name string, factory func() ([]Option, error)) error {
	return globalRegistry.RegisterFactory(name, factory)
}

// Register registers a new extension with options
func Register(name string, options ...Option) error {
	return RegisterFactory(name, func() ([]Option, error) {
		return options, nil
	})
}

// Setup setups extensions
func Setup() error {
	return globalRegistry.Setup()
}

// GetExtensions returns all extensions after setup
func GetExtensions() (*Extensions, error) {
	return globalRegistry.Extensions()
}

// Reset resets the registry. It is only used by test
func Reset() {
	globalRegistry.Reset()
}
