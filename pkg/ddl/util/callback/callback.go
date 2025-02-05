// Copyright 2023 PingCAP, Inc.
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

package callback

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"go.uber.org/zap"
)

// TestInterceptor is a test interceptor in the ddl
type TestInterceptor struct {
	*ddl.BaseInterceptor

	OnGetInfoSchemaExported func(ctx sessionctx.Context, is infoschema.InfoSchema) infoschema.InfoSchema
}

// OnGetInfoSchema is to run when to call GetInfoSchema
func (ti *TestInterceptor) OnGetInfoSchema(ctx sessionctx.Context, is infoschema.InfoSchema) infoschema.InfoSchema {
	if ti.OnGetInfoSchemaExported != nil {
		return ti.OnGetInfoSchemaExported(ctx, is)
	}

	return ti.BaseInterceptor.OnGetInfoSchema(ctx, is)
}

// TestDDLCallback is used to customize user callback themselves.
type TestDDLCallback struct {
	*ddl.BaseCallback
	// We recommended to pass the domain parameter to the test ddl callback, it will ensure
	// domain to reload schema before your ddl stepping into the next state change.
	Do ddl.DomainReloader

	onJobRunBefore          func(*model.Job)
	OnJobRunBeforeExported  func(*model.Job)
	OnJobRunAfterExported   func(*model.Job)
	onJobUpdated            func(*model.Job)
	OnJobUpdatedExported    atomic.Pointer[func(*model.Job)]
	onWatched               func(ctx context.Context)
	OnGetJobBeforeExported  func(string)
	OnGetJobAfterExported   func(string, *model.Job)
	OnJobSchemaStateChanged func(int64)

	OnUpdateReorgInfoExported func(job *model.Job, pid int64)
}

// OnChanged mock the same behavior with the main DDL hook.
func (tc *TestDDLCallback) OnChanged(err error) error {
	if err != nil {
		return err
	}
	logutil.DDLLogger().Info("performing DDL change, must reload")
	if tc.Do != nil {
		err = tc.Do.Reload()
		if err != nil {
			logutil.DDLLogger().Error("performing DDL change failed", zap.Error(err))
		}
	}
	return nil
}

// OnSchemaStateChanged mock the same behavior with the main ddl hook.
func (tc *TestDDLCallback) OnSchemaStateChanged(schemaVer int64) {
	if tc.Do != nil {
		if err := tc.Do.Reload(); err != nil {
			logutil.DDLLogger().Warn("reload failed on schema state changed", zap.Error(err))
		}
	}

	if tc.OnJobSchemaStateChanged != nil {
		tc.OnJobSchemaStateChanged(schemaVer)
		return
	}
}

// OnJobRunBefore is used to run the user customized logic of `onJobRunBefore` first.
func (tc *TestDDLCallback) OnJobRunBefore(job *model.Job) {
	logutil.DDLLogger().Info("on job run before", zap.String("job", job.String()))
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

// OnJobRunAfter is used to run the user customized logic of `OnJobRunAfter` first.
func (tc *TestDDLCallback) OnJobRunAfter(job *model.Job) {
	logutil.DDLLogger().Info("on job run after", zap.String("job", job.String()))
	if tc.OnJobRunAfterExported != nil {
		tc.OnJobRunAfterExported(job)
		return
	}

	tc.BaseCallback.OnJobRunAfter(job)
}

// OnJobUpdated is used to run the user customized logic of `OnJobUpdated` first.
func (tc *TestDDLCallback) OnJobUpdated(job *model.Job) {
	logutil.DDLLogger().Info("on job updated", zap.String("job", job.String()))
	if onJobUpdatedExportedFunc := tc.OnJobUpdatedExported.Load(); onJobUpdatedExportedFunc != nil {
		(*onJobUpdatedExportedFunc)(job)
		return
	}
	if job.State == model.JobStateSynced {
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

// OnGetJobBefore implements Callback.OnGetJobBefore interface.
func (tc *TestDDLCallback) OnGetJobBefore(jobType string) {
	if tc.OnGetJobBeforeExported != nil {
		tc.OnGetJobBeforeExported(jobType)
		return
	}
	tc.BaseCallback.OnGetJobBefore(jobType)
}

// OnGetJobAfter implements Callback.OnGetJobAfter interface.
func (tc *TestDDLCallback) OnGetJobAfter(jobType string, job *model.Job) {
	if tc.OnGetJobAfterExported != nil {
		tc.OnGetJobAfterExported(jobType, job)
		return
	}
	tc.BaseCallback.OnGetJobAfter(jobType, job)
}

// Clone copies the callback and take its reference
func (tc *TestDDLCallback) Clone() *TestDDLCallback {
	return &*tc
}

// OnUpdateReorgInfo mock the same behavior with the main DDL reorg hook.
func (tc *TestDDLCallback) OnUpdateReorgInfo(job *model.Job, pid int64) {
	if tc.OnUpdateReorgInfoExported != nil {
		tc.OnUpdateReorgInfoExported(job, pid)
	}
}
