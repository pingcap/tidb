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
	"strings"
	"time"

	"github.com/pingcap/errors"
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
	// SetDomain is called when the assign the domain to callback.
	SetDomain(do DomainReloader)
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

// SetDomain implements Callback.SetDomain interface.
func (c *BaseCallback) SetDomain(do DomainReloader) {
	// Nothing to do.
}

// DomainReloader is used to avoid import loop.
type DomainReloader interface {
	Reload() error
}

// ****************************** Start of Customized DDL Callback Instance ****************************************

// DefaultCallback is the default callback that TiDB will use.
type DefaultCallback struct {
	*BaseCallback
	do DomainReloader
}

// OnChanged overrides ddl Callback interface.
func (c *DefaultCallback) OnChanged(err error) error {
	if err != nil {
		return err
	}
	logutil.BgLogger().Info("performing DDL change, must reload")

	err = c.do.Reload()
	if err != nil {
		logutil.BgLogger().Error("performing DDL change failed", zap.Error(err))
	}

	return nil
}

// OnSchemaStateChanged overrides the ddl Callback interface.
func (c *DefaultCallback) OnSchemaStateChanged() {
	err := c.do.Reload()
	if err != nil {
		logutil.BgLogger().Error("domain callback failed on schema state changed", zap.Error(err))
	}
}

// SetDomain is used to assign the domain.
func (c *DefaultCallback) SetDomain(do DomainReloader) {
	c.do = do
}

func newDefaultCallBack(do DomainReloader) Callback {
	return &DefaultCallback{do: do}
}

// ****************************** End of Default DDL Callback Instance *********************************************

// ****************************** Start of CTC DDL Callback Instance ***********************************************

// CtcCallback is the customized callback that TiDB will use.
type ctcCallback struct {
	*BaseCallback
	do DomainReloader
}

// OnChanged overrides ddl Callback interface.
func (c *ctcCallback) OnChanged(err error) error {
	if err != nil {
		return err
	}
	logutil.BgLogger().Info("performing DDL change, must reload")

	err = c.do.Reload()
	if err != nil {
		logutil.BgLogger().Error("performing DDL change failed", zap.Error(err))
	}
	return nil
}

// OnSchemaStateChanged overrides the ddl Callback interface.
func (c *ctcCallback) OnSchemaStateChanged() {
	err := c.do.Reload()
	if err != nil {
		logutil.BgLogger().Error("domain callback failed on schema state changed", zap.Error(err))
	}
}

// OnJobRunBefore is used to run the user customized logic of `onJobRunBefore` first.
func (c *ctcCallback) OnJobRunBefore(job *model.Job) {
	log.Info("on job run before", zap.String("job", job.String()))
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

// SetDomain is used to assign the domain.
func (c *ctcCallback) SetDomain(do DomainReloader) {
	c.do = do
}

func newCtcCallBack(do DomainReloader) Callback {
	return &ctcCallback{do: do}
}

// ****************************** End of CTC DDL Callback Instance ***************************************************

var (
	customizedCallBackRegisterMap = map[string]func(do DomainReloader) Callback{}
)

func init() {
	// init the callback register map.
	customizedCallBackRegisterMap["default_hook"] = newDefaultCallBack
	customizedCallBackRegisterMap["ctc_hook"] = newCtcCallBack
}

// GetCustomizedHook get the hook registered in the hookMap.
func GetCustomizedHook(s string) (func(do DomainReloader) Callback, error) {
	s = strings.ToLower(s)
	s = strings.TrimSpace(s)
	fact, ok := customizedCallBackRegisterMap[s]
	if !ok {
		logutil.BgLogger().Error(fmt.Sprintf("bad ddl hook %s", s))
		return nil, errors.Errorf("ddl hook `%s` is not found in hook registered map", s)
	}
	return fact, nil
}
