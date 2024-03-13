// Copyright 2024 PingCAP, Inc.
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

package contextopt

import (
	"github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// AdvisoryLockContext is the interface to operate advisory locks
type AdvisoryLockContext interface {
	// GetAdvisoryLock acquires an advisory lock (aka GET_LOCK()).
	GetAdvisoryLock(string, int64) error
	// IsUsedAdvisoryLock checks for existing locks (aka IS_USED_LOCK()).
	IsUsedAdvisoryLock(string) uint64
	// ReleaseAdvisoryLock releases an advisory lock (aka RELEASE_LOCK()).
	ReleaseAdvisoryLock(string) bool
	// ReleaseAllAdvisoryLocks releases all advisory locks that this session holds.
	ReleaseAllAdvisoryLocks() int
}

var _ context.OptionalEvalPropProvider = CurrentUserPropProvider(nil)
var _ RequireOptionalEvalProps = CurrentUserPropReader{}

// AdvisoryLockPropProvider is a provider to provide AdvisoryLockContext.
type AdvisoryLockPropProvider struct {
	AdvisoryLockContext
}

// NewAdvisoryLockPropProvider creates a new AdvisoryLockPropProvider
func NewAdvisoryLockPropProvider(ctx AdvisoryLockContext) *AdvisoryLockPropProvider {
	intest.AssertNotNil(ctx)
	return &AdvisoryLockPropProvider{
		AdvisoryLockContext: ctx,
	}
}

// Desc returns the description for the property key.
func (p *AdvisoryLockPropProvider) Desc() *context.OptionalEvalPropDesc {
	return context.OptPropAdvisoryLock.Desc()
}

// AdvisoryLockPropReader is used by expression to operate advisory lock
type AdvisoryLockPropReader struct{}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (r AdvisoryLockPropReader) RequiredOptionalEvalProps() context.OptionalEvalPropKeySet {
	return context.OptPropAdvisoryLock.AsPropKeySet()
}

// AdvisoryLockCtx returns the AdvisoryLockContext from the context
func (r AdvisoryLockPropReader) AdvisoryLockCtx(ctx context.EvalContext) (AdvisoryLockContext, error) {
	return getPropProvider[*AdvisoryLockPropProvider](ctx, context.OptPropAdvisoryLock)
}
