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

package tables

import (
	"github.com/pingcap/tidb/pkg/kv"
)

// Keep the interface local to the table layer so other packages don't directly
// depend on assertion APIs.
type assertionSetter interface {
	SetAssertion(key []byte, assertion kv.AssertionOp) error
}

func setAssertion(txn kv.Transaction, key []byte, assertion kv.AssertionOp) error {
	if s, ok := txn.(assertionSetter); ok {
		return s.SetAssertion(key, assertion)
	}
	// Optional capability: some Transaction implementations (e.g. Lightning's
	// in-memory txn) don't support assertions, and table writes should keep
	// working in those cases.
	return nil
}
