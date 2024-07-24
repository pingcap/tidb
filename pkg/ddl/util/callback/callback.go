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
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/parser/model"
	"go.uber.org/zap"
)

// TestDDLCallback is used to customize user callback themselves.
type TestDDLCallback struct {
	*ddl.BaseCallback
	// We recommended to pass the domain parameter to the test ddl callback, it will ensure
	// domain to reload schema before your ddl stepping into the next state change.
	Do ddl.SchemaLoader

	OnJobRunBeforeExported func(*model.Job)
	OnJobRunAfterExported  func(*model.Job)
	OnJobUpdatedExported   atomic.Pointer[func(*model.Job)]
}

// OnJobRunBefore is used to run the user customized logic of `onJobRunBefore` first.
func (tc *TestDDLCallback) OnJobRunBefore(job *model.Job) {
	logutil.DDLLogger().Info("on job run before", zap.String("job", job.String()))
	if tc.OnJobRunBeforeExported != nil {
		tc.OnJobRunBeforeExported(job)
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

	tc.BaseCallback.OnJobUpdated(job)
}

// Clone copies the callback and take its reference
func (tc *TestDDLCallback) Clone() *TestDDLCallback {
	return &*tc
}
