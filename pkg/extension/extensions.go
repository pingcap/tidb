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

// Extensions contains all extensions that have already setup
type Extensions struct {
	manifests []*Manifest
}

// Manifests returns a extension manifests
func (es *Extensions) Manifests() []*Manifest {
	if es == nil {
		return nil
	}
	manifests := make([]*Manifest, len(es.manifests))
	copy(manifests, es.manifests)
	return manifests
}

// Bootstrap bootstraps all extensions
func (es *Extensions) Bootstrap(ctx BootstrapContext) error {
	if es == nil {
		return nil
	}

	for _, m := range es.manifests {
		if m.bootstrap != nil {
			if err := m.bootstrap(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

// GetAccessCheckFuncs returns spec functions of the custom access check
func (es *Extensions) GetAccessCheckFuncs() (funcs []AccessCheckFunc) {
	if es == nil {
		return nil
	}

	for _, m := range es.manifests {
		if m.accessCheckFunc != nil {
			funcs = append(funcs, m.accessCheckFunc)
		}
	}

	return funcs
}

// NewSessionExtensions creates a new ConnExtensions object
func (es *Extensions) NewSessionExtensions() *SessionExtensions {
	if es == nil {
		return nil
	}
	return newSessionExtensions(es)
}
