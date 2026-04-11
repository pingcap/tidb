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
	"sync"
	"testing"
)

// Registry manages test registration for the mega framework
type Registry struct {
	mu       sync.RWMutex
	tests    map[string]map[string]func(*testing.T) // pkg -> name -> test function
	onBefore []func(string)                         // hooks called before running tests, with pkg name
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

// RegisterOnBeforeRun registers a hook on the global registry.
func RegisterOnBeforeRun(fn func(string)) {
	globalRegistry.RegisterOnBeforeRun(fn)
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

// RegisterOnBeforeRun registers a hook to be called before running any test.
// This is used to ensure proper initialization order (e.g., parser driver registration).
func (r *Registry) RegisterOnBeforeRun(fn func(string)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onBefore = append(r.onBefore, fn)
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
	hooks := make([]func(string), len(r.onBefore))
	copy(hooks, r.onBefore)
	r.mu.RUnlock()

	if len(matched) == 0 {
		t.Fatalf("no tests matched pattern %q", pattern)
	}

	// Track which packages have had their hooks called
	pkgHookCalled := make(map[string]bool)
	for _, m := range matched {
		if !pkgHookCalled[m.pkg] {
			for _, hook := range hooks {
				hook(m.pkg)
			}
			pkgHookCalled[m.pkg] = true
		}
		// Call fn directly without t.Run wrapper so that runtime.Caller(1)
		// in testdata.LoadTestCases returns the correct function name.
		testFn, ok := r.Get(m.pkg, m.name)
		if !ok {
			t.Fatalf("test %s/%s not found", m.pkg, m.name)
		}
		testFn(t)
	}
}

// RunAll runs all registered tests
func (r *Registry) RunAll(t *testing.T) {
	t.Helper()

	r.mu.RLock()
	type testEntry struct {
		pkg  string
		name string
	}
	var entries []testEntry
	for pkg, tests := range r.tests {
		for name := range tests {
			entries = append(entries, testEntry{pkg, name})
		}
	}
	hooks := make([]func(string), len(r.onBefore))
	copy(hooks, r.onBefore)
	r.mu.RUnlock()

	pkgHookCalled := make(map[string]bool)
	for _, e := range entries {
		if !pkgHookCalled[e.pkg] {
			for _, fn := range hooks {
				fn(e.pkg)
			}
			pkgHookCalled[e.pkg] = true
		}
		// Call fn directly without t.Run wrapper so that runtime.Caller(1)
		// in testdata.LoadTestCases returns the correct function name.
		fn, ok := r.Get(e.pkg, e.name)
		if !ok {
			t.Fatalf("test %s/%s not found", e.pkg, e.name)
		}
		fn(t)
	}
}
