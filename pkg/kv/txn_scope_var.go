// Copyright 2021 PingCAP, Inc.
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

package kv

import (
	"github.com/tikv/client-go/v2/oracle"
)

// Transaction scopes constants.
const (
	// GlobalTxnScope is synced with PD's define of global scope.
	// If we want to remove the dependency on store/tikv here, we need to map
	// the two GlobalTxnScopes in the driver layer.
	// TODO: remove the constant later
	GlobalTxnScope = oracle.GlobalTxnScope
)
