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

// Package domainctx allows retrieving the Domain instance from a session context.
// Useful to break circular dependencies.
package domainctx

import (
	contextutil "github.com/pingcap/tidb/pkg/util/context"
)

// domainKeyType is a dummy type to avoid naming collision in context.
type domainKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k domainKeyType) String() string {
	return "domain"
}

// DomainKey is the key used to store or retrieve the Domain instance in the session context.
// This is supposed to be used only in tests!!!
const DomainKey domainKeyType = 0

// BindDomain binds domain to context.
func BindDomain(ctx contextutil.ValueStoreContext, domain any) {
	ctx.SetValue(DomainKey, domain)
}

// GetDomain gets domain from context.
func GetDomain(ctx contextutil.ValueStoreContext) any {
	return ctx.Value(DomainKey)
}
