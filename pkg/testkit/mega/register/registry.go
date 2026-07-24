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
	"sort"
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

// OnBeforeRun registers a hook on the global registry.
func OnBeforeRun(fn func(string)) {
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

// testEntry holds a snapshot of a registered test, capturing the function
// pointer under the read lock to avoid redundant lock acquisition.
type testEntry struct {
	pkg  string
	name string
	fn   func(*testing.T)
}

// snapshotAll collects all registered tests into a sorted slice while holding
// the read lock, also snapshotting the onBefore hooks.
func (r *Registry) snapshotAll() ([]testEntry, []func(string)) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entries := make([]testEntry, 0, 64)
	for pkg, tests := range r.tests {
		for name, fn := range tests {
			entries = append(entries, testEntry{pkg, name, fn})
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].pkg != entries[j].pkg {
			return entries[i].pkg < entries[j].pkg
		}
		return entries[i].name < entries[j].name
	})

	hooks := make([]func(string), len(r.onBefore))
	copy(hooks, r.onBefore)
	return entries, hooks
}

// ListAll returns all registered tests in "pkg/name" format, sorted deterministically.
func (r *Registry) ListAll() []string {
	entries, _ := r.snapshotAll()
	result := make([]string, len(entries))
	for i, e := range entries {
		result[i] = e.pkg + "/" + e.name
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

	// Snapshot all tests and filter by pattern while under the read lock.
	r.mu.RLock()
	var matched []testEntry
	for pkg, tests := range r.tests {
		for name, fn := range tests {
			fullName := pkg + "/" + name
			if re.MatchString(fullName) {
				matched = append(matched, testEntry{pkg, name, fn})
			}
		}
	}
	hooks := make([]func(string), len(r.onBefore))
	copy(hooks, r.onBefore)
	r.mu.RUnlock()

	if len(matched) == 0 {
		t.Fatalf("no tests matched pattern %q", pattern)
	}

	sort.Slice(matched, func(i, j int) bool {
		if matched[i].pkg != matched[j].pkg {
			return matched[i].pkg < matched[j].pkg
		}
		return matched[i].name < matched[j].name
	})

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
		m.fn(t)
	}
}

// RunAll runs all registered tests
func (r *Registry) RunAll(t *testing.T) {
	t.Helper()

	entries, hooks := r.snapshotAll()

	pkgHookCalled := make(map[string]bool)
	for _, e := range entries {
		if !pkgHookCalled[e.pkg] {
			for _, hook := range hooks {
				hook(e.pkg)
			}
			pkgHookCalled[e.pkg] = true
		}
		// Call fn directly without t.Run wrapper so that runtime.Caller(1)
		// in testdata.LoadTestCases returns the correct function name.
		e.fn(t)
	}
}
