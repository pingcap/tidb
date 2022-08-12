// Copyright 2022 PingCAP, Inc.
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

package staleread

import (
	"fmt"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
)

// AssertStmtStaleness is used only for test
func AssertStmtStaleness(sctx sessionctx.Context, expected bool) {
	actual := IsStmtStaleness(sctx)
	if actual != expected {
		panic(fmt.Sprintf("stmtctx isStaleness wrong, expected:%v, got:%v", expected, actual))
	}

	if expected {
		provider := sessiontxn.GetTxnManager(sctx).GetContextProvider()
		if _, ok := provider.(*StalenessTxnContextProvider); !ok {
			panic(fmt.Sprintf("stale read should be StalenessTxnContextProvider but current provider is: %T", provider))
		}
	}
}
