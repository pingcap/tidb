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

package ddl

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestReorgGoroutineRunning is only used in test to indicate the reorg goroutine has been started.
var TestReorgGoroutineRunning = make(chan interface{})

type TestInterceptor struct {
	*BaseInterceptor

	OnGetInfoSchemaExported func(ctx sessionctx.Context, is infoschema.InfoSchema) infoschema.InfoSchema
}

func (ti *TestInterceptor) OnGetInfoSchema(ctx sessionctx.Context, is infoschema.InfoSchema) infoschema.InfoSchema {
	if ti.OnGetInfoSchemaExported != nil {
		return ti.OnGetInfoSchemaExported(ctx, is)
	}

	return ti.BaseInterceptor.OnGetInfoSchema(ctx, is)
}

// TestDDLCallback is used to customize user callback themselves.
type TestDDLCallback struct {
	*BaseCallback
	// We recommended to pass the domain parameter to the test ddl callback, it will ensure
	// domain to reload schema before your ddl stepping into the next state change.
	Do DomainReloader

	onJobRunBefore         func(*model.Job)
	OnJobRunBeforeExported func(*model.Job)
	onJobUpdated           func(*model.Job)
	OnJobUpdatedExported   func(*model.Job)
	onWatched              func(ctx context.Context)
}

// OnChanged mock the same behavior with the main DDL hook.
func (tc *TestDDLCallback) OnChanged(err error) error {
	if err != nil {
		return err
	}
	logutil.BgLogger().Info("performing DDL change, must reload")
	if tc.Do != nil {
		err = tc.Do.Reload()
		if err != nil {
			logutil.BgLogger().Error("performing DDL change failed", zap.Error(err))
		}
	}
	return nil
}

// OnSchemaStateChanged mock the same behavior with the main ddl hook.
func (tc *TestDDLCallback) OnSchemaStateChanged() {
	if tc.Do != nil {
		if err := tc.Do.Reload(); err != nil {
			logutil.BgLogger().Warn("reload failed on schema state changed", zap.Error(err))
		}
	}
}

// OnJobRunBefore is used to run the user customized logic of `onJobRunBefore` first.
func (tc *TestDDLCallback) OnJobRunBefore(job *model.Job) {
	logutil.BgLogger().Info("on job run before", zap.String("job", job.String()))
	if tc.OnJobRunBeforeExported != nil {
		tc.OnJobRunBeforeExported(job)
		return
	}
	if tc.onJobRunBefore != nil {
		tc.onJobRunBefore(job)
		return
	}

	tc.BaseCallback.OnJobRunBefore(job)
}

// OnJobUpdated is used to run the user customized logic of `OnJobUpdated` first.
func (tc *TestDDLCallback) OnJobUpdated(job *model.Job) {
	logutil.BgLogger().Info("on job updated", zap.String("job", job.String()))
	if tc.OnJobUpdatedExported != nil {
		tc.OnJobUpdatedExported(job)
		return
	}
	if tc.onJobUpdated != nil {
		tc.onJobUpdated(job)
		return
	}

	tc.BaseCallback.OnJobUpdated(job)
}

// OnWatched is used to run the user customized logic of `OnWatched` first.
func (tc *TestDDLCallback) OnWatched(ctx context.Context) {
	if tc.onWatched != nil {
		tc.onWatched(ctx)
		return
	}

	tc.BaseCallback.OnWatched(ctx)
}

func TestCallback(t *testing.T) {
	cb := &BaseCallback{}
	require.Nil(t, cb.OnChanged(nil))
	cb.OnJobRunBefore(nil)
	cb.OnJobUpdated(nil)
	cb.OnWatched(context.TODO())
}
