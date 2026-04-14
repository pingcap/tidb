// Copyright 2025 PingCAP, Inc.
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

package testsetup

import (
	"sync"
	"testing"

	"go.etcd.io/etcd/tests/v3/integration"
)

var etcdBeforeOnce sync.Once

// EnsureEtcdTestContext calls integration.BeforeTestExternal exactly once.
// This is needed when multiple tests that require etcd integration are
// combined into a single test binary (e.g. the mega coverage test runner).
// Calling BeforeTestExternal more than once in the same process panics with
// "already in test context", so we guard it with sync.Once.
func EnsureEtcdTestContext(t *testing.T) {
	etcdBeforeOnce.Do(func() {
		integration.BeforeTestExternal(t)
	})
}
