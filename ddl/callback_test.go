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
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/testleak"
	goctx "golang.org/x/net/context"
)

type TestDDLCallback struct {
	*BaseCallback

	onJobRunBefore       func(*model.Job)
	onJobUpdated         func(*model.Job)
	OnJobUpdatedExported func(*model.Job)
	onBgJobUpdated       func(*model.Job)
	onWatched            func(ctx goctx.Context)
}

func (tc *TestDDLCallback) OnJobRunBefore(job *model.Job) {
	if tc.onJobRunBefore != nil {
		tc.onJobRunBefore(job)
		return
	}

	tc.BaseCallback.OnJobRunBefore(job)
}

func (tc *TestDDLCallback) OnJobUpdated(job *model.Job) {
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

func (tc *TestDDLCallback) OnBgJobUpdated(job *model.Job) {
	if tc.onBgJobUpdated != nil {
		tc.onBgJobUpdated(job)
		return
	}

	tc.BaseCallback.OnBgJobUpdated(job)
}

func (tc *TestDDLCallback) OnWatched(ctx goctx.Context) {
	if tc.onWatched != nil {
		tc.onWatched(ctx)
		return
	}

	tc.BaseCallback.OnWatched(ctx)
}

func (s *testDDLSuite) TestCallback(c *C) {
	defer testleak.AfterTest(c)()
	cb := &BaseCallback{}
	c.Assert(cb.OnChanged(nil), IsNil)
	cb.OnJobRunBefore(nil)
	cb.OnJobUpdated(nil)
	cb.OnBgJobUpdated(nil)
	cb.OnWatched(nil)
}
