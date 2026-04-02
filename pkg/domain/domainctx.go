// Copyright 2015 PingCAP, Inc.
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

package domain

import (
	"github.com/pingcap/tidb/pkg/domain/domainctx"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
)

// BindDomain binds domain to context.
func BindDomain(ctx contextutil.ValueStoreContext, domain *Domain) {
	domainctx.BindDomain(ctx, domain)
}

// GetDomain gets domain from context.
func GetDomain(ctx contextutil.ValueStoreContext) *Domain {
	// Prefer the built-in method on ValueStoreContext. The domain stored in the
	// session context may not be bound via SetValue/Value.
	if ctx != nil {
		if v, ok := ctx.GetDomain().(*Domain); ok {
			return v
		}
	}

	v, ok := domainctx.GetDomain(ctx).(*Domain)
	if !ok {
		return nil
	}
	return v
}
