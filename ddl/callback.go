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
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// Interceptor is used for DDL.
type Interceptor interface {
	// OnGetInfoSchema is an intercept which is called in the function ddl.GetInfoSchema(). It is used in the tests.
	OnGetInfoSchema(ctx sessionctx.Context, is infoschema.InfoSchema) infoschema.InfoSchema
}

// BaseInterceptor implements Interceptor.
type BaseInterceptor struct{}

// OnGetInfoSchema implements Interceptor.OnGetInfoSchema interface.
func (bi *BaseInterceptor) OnGetInfoSchema(ctx sessionctx.Context, is infoschema.InfoSchema) infoschema.InfoSchema {
	return is
}

// Callback is used for DDL.
type Callback interface {
	// OnChanged is called after a ddl statement is finished.
	OnChanged(err error) error
	// OnSchemaStateChanged is called after a schema state is changed.
	OnSchemaStateChanged()
	// OnJobRunBefore is called before running job.
	OnJobRunBefore(job *model.Job)
	// OnJobUpdated is called after the running job is updated.
	OnJobUpdated(job *model.Job)
	// OnWatched is called after watching owner is completed.
	OnWatched(ctx context.Context)
}

// BaseCallback implements Callback.OnChanged interface.
type BaseCallback struct {
}

// OnChanged implements Callback interface.
func (c *BaseCallback) OnChanged(err error) error {
	return err
}

// OnSchemaStateChanged implements Callback interface.
func (c *BaseCallback) OnSchemaStateChanged() {
	// Nothing to do.
}

// OnJobRunBefore implements Callback.OnJobRunBefore interface.
func (c *BaseCallback) OnJobRunBefore(job *model.Job) {
	// Nothing to do.
}

// OnJobUpdated implements Callback.OnJobUpdated interface.
func (c *BaseCallback) OnJobUpdated(job *model.Job) {
	// Nothing to do.
}

// OnWatched implements Callback.OnWatched interface.
func (c *BaseCallback) OnWatched(ctx context.Context) {
	// Nothing to do.
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

var (
	// CustomizedDDLHook is used to store the user specified ddl hook when TiDB starts.
	//
	// ********************************* Special Customized Instance ****************************************
	CustomizedDDLHook *TestDDLCallback

	customizedCallBackRegisterMap = map[string]*TestDDLCallback{}
)

func init() {
	// init the callback action.
	columnTypeChangeTiCaseSpecialCallBack := &TestDDLCallback{}
	columnTypeChangeTiCaseSpecialCallBack.onJobRunBefore = func(job *model.Job) {
		// Only block the ctc type ddl here.
		if job.Type != model.ActionModifyColumn {
			return
		}
		switch job.SchemaState {
		case model.StateDeleteOnly, model.StateWriteOnly, model.StateWriteReorganization:
			logutil.BgLogger().Warn(fmt.Sprintf("[DDL_HOOK] Hang for 0.5 seconds on %s state triggered", job.SchemaState.String()))
			time.Sleep(500 * time.Millisecond)
		}
	}

	// init the callback register.
	customizedCallBackRegisterMap["ctc_hook"] = columnTypeChangeTiCaseSpecialCallBack

	// init the trigger
	if s := os.Getenv("DDL_HOOK"); len(s) > 0 {
		s = strings.ToLower(s)
		s = strings.TrimSpace(s)
		if hook, ok := customizedCallBackRegisterMap[s]; !ok {
			logutil.BgLogger().Error(fmt.Sprintf("bad ddl hook %s", s))
			os.Exit(1)
		} else {
			CustomizedDDLHook = hook
		}
	}
}

type DomainReloader interface {
	Reload() error
}

// OnChanged mock the same behavior with the main ddl hook.
func (tc *TestDDLCallback) OnChanged(err error) error {
	if err != nil {
		return err
	}
	logutil.BgLogger().Info("performing DDL change, must reload")

	err = tc.Do.Reload()
	if err != nil {
		logutil.BgLogger().Error("performing DDL change failed", zap.Error(err))
	}

	return nil
}

// OnSchemaStateChanged mock the same behavior with the main ddl hook.
func (tc *TestDDLCallback) OnSchemaStateChanged() {
	if tc.Do != nil {
		if err := tc.Do.Reload(); err != nil {
			log.Warn("reload failed on schema state changed", zap.Error(err))
		}
	}
}

func (tc *TestDDLCallback) OnJobRunBefore(job *model.Job) {
	log.Info("on job run before", zap.String("job", job.String()))
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

func (tc *TestDDLCallback) OnJobUpdated(job *model.Job) {
	log.Info("on job updated", zap.String("job", job.String()))
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

func (tc *TestDDLCallback) OnWatched(ctx context.Context) {
	if tc.onWatched != nil {
		tc.onWatched(ctx)
		return
	}

	tc.BaseCallback.OnWatched(ctx)
}
