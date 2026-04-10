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

package register

import (
	"regexp"
	"strings"
	"sync"
	"testing"
)

// Registry manages test registration for the mega framework
type Registry struct {
	mu    sync.RWMutex
	tests map[string]map[string]func(*testing.T) // pkg -> name -> test function
}

// Global registry instance
var globalRegistry = &Registry{
	tests: make(map[string]map[string]func(*testing.T)),
}

// GlobalRegistry returns the global test registry
func GlobalRegistry() *Registry {
	return globalRegistry
}

// Register registers a test function
func Register(pkg, name string, fn func(*testing.T)) {
	globalRegistry.Register(pkg, name, fn)
}

// Register registers a test function
func (r *Registry) Register(pkg, name string, fn func(*testing.T)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.tests[pkg] == nil {
		r.tests[pkg] = make(map[string]func(*testing.T))
	}
	r.tests[pkg][name] = fn
}

// Get retrieves a test function by package and name
func (r *Registry) Get(pkg, name string) (func(*testing.T), bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if pkgTests, ok := r.tests[pkg]; ok {
		fn, exists := pkgTests[name]
		return fn, exists
	}
	return nil, false
}

// ListAll returns all registered tests in "pkg/name" format
func (r *Registry) ListAll() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []string
	for pkg, tests := range r.tests {
		for name := range tests {
			result = append(result, pkg+"/"+name)
		}
	}
	return result
}

// RunByPattern runs tests matching the given regex pattern (format: "pkg/name")
func (r *Registry) RunByPattern(t *testing.T, pattern string) {
	t.Helper()

	re, err := regexp.Compile(pattern)
	if err != nil {
		t.Fatalf("invalid pattern %q: %v", pattern, err)
	}

	r.mu.RLock()
	var matched []struct {
		pkg  string
		name string
	}
	for pkg, tests := range r.tests {
		for name := range tests {
			fullName := pkg + "/" + name
			if re.MatchString(fullName) {
				matched = append(matched, struct {
					pkg  string
					name string
				}{pkg, name})
			}
		}
	}
	r.mu.RUnlock()

	if len(matched) == 0 {
		t.Fatalf("no tests matched pattern %q", pattern)
	}

	// Run matched tests
	for _, m := range matched {
		t.Run(m.pkg+"/"+m.name, func(t *testing.T) {
			fn, ok := r.Get(m.pkg, m.name)
			if !ok {
				t.Fatalf("test %s/%s not found", m.pkg, m.name)
			}
			fn(t)
		})
	}
}

// RunAll runs all registered tests
func (r *Registry) RunAll(t *testing.T) {
	t.Helper()

	r.mu.RLock()
	testNames := make([]string, 0, len(r.tests))
	for pkg, tests := range r.tests {
		for name := range tests {
			testNames = append(testNames, pkg+"/"+name)
		}
	}
	r.mu.RUnlock()

	for _, testName := range testNames {
		t.Run(testName, func(t *testing.T) {
			// Parse test name
			parts := strings.SplitN(testName, "/", 2)
			if len(parts) != 2 {
				t.Fatalf("invalid test name: %s", testName)
			}
			pkg, name := parts[0], parts[1]

			fn, ok := r.Get(pkg, name)
			if !ok {
				t.Fatalf("test %s not found", testName)
			}
			fn(t)
		})
	}
}
