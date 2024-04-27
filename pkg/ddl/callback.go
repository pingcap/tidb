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
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
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
func (*BaseInterceptor) OnGetInfoSchema(_ sessionctx.Context, is infoschema.InfoSchema) infoschema.InfoSchema {
	return is
}

// Callback is used for DDL.
type Callback interface {
	ReorgCallback
	// OnChanged is called after a ddl statement is finished.
	OnChanged(err error) error
	// OnSchemaStateChanged is called after a schema state is changed.
	// only called inside tests.
	OnSchemaStateChanged(schemaVer int64)
	// OnJobRunBefore is called before running job.
	OnJobRunBefore(job *model.Job)
	// OnJobRunAfter is called after running job.
	OnJobRunAfter(job *model.Job)
	// OnJobUpdated is called after the running job is updated.
	OnJobUpdated(job *model.Job)
	// OnWatched is called after watching owner is completed.
	OnWatched(ctx context.Context)
	// OnGetJobBefore is called before getting job.
	OnGetJobBefore(jobType string)
	// OnGetJobAfter is called after getting job.
	OnGetJobAfter(jobType string, job *model.Job)
}

// BaseCallback implements Callback.OnChanged interface.
type BaseCallback struct {
}

// OnChanged implements Callback interface.
func (*BaseCallback) OnChanged(err error) error {
	return err
}

// OnSchemaStateChanged implements Callback interface.
func (*BaseCallback) OnSchemaStateChanged(_ int64) {
	// Nothing to do.
}

// OnJobRunBefore implements Callback.OnJobRunBefore interface.
func (*BaseCallback) OnJobRunBefore(_ *model.Job) {
	// Nothing to do.
}

// OnJobRunAfter implements Callback.OnJobRunAfter interface.
func (*BaseCallback) OnJobRunAfter(_ *model.Job) {
	// Nothing to do.
}

// OnJobUpdated implements Callback.OnJobUpdated interface.
func (*BaseCallback) OnJobUpdated(_ *model.Job) {
	// Nothing to do.
}

// OnWatched implements Callback.OnWatched interface.
func (*BaseCallback) OnWatched(_ context.Context) {
	// Nothing to do.
}

// OnGetJobBefore implements Callback.OnGetJobBefore interface.
func (*BaseCallback) OnGetJobBefore(_ string) {
	// Nothing to do.
}

// OnGetJobAfter implements Callback.OnGetJobAfter interface.
func (*BaseCallback) OnGetJobAfter(_ string, _ *model.Job) {
	// Nothing to do.
}

// OnUpdateReorgInfo implements ReorgCallback interface.
func (*BaseCallback) OnUpdateReorgInfo(_ *model.Job, _ int64) {
}

// DomainReloader is used to avoid import loop.
type DomainReloader interface {
	Reload() error
}

// ReorgCallback is the callback for DDL reorganization.
type ReorgCallback interface {
	// OnUpdateReorgInfo is called after updating reorg info for partitions.
	OnUpdateReorgInfo(job *model.Job, pid int64)
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
	logutil.DDLLogger().Info("performing DDL change, must reload")

	err = c.do.Reload()
	if err != nil {
		logutil.DDLLogger().Error("performing DDL change failed", zap.Error(err))
	}

	return nil
}

// OnSchemaStateChanged overrides the ddl Callback interface.
func (c *DefaultCallback) OnSchemaStateChanged(_ int64) {
	err := c.do.Reload()
	if err != nil {
		logutil.DDLLogger().Error("domain callback failed on schema state changed", zap.Error(err))
	}
}

func newDefaultCallBack(do DomainReloader) Callback {
	return &DefaultCallback{BaseCallback: &BaseCallback{}, do: do}
}

// ****************************** End of Default DDL Callback Instance *********************************************

// ****************************** Start of CTC DDL Callback Instance ***********************************************

// ctcCallback is the customized callback that TiDB will use.
// ctc is named from column type change, here after we call them ctc for short.
type ctcCallback struct {
	*BaseCallback
	do DomainReloader
}

// OnChanged overrides ddl Callback interface.
func (c *ctcCallback) OnChanged(err error) error {
	if err != nil {
		return err
	}
	logutil.DDLLogger().Info("performing DDL change, must reload")

	err = c.do.Reload()
	if err != nil {
		logutil.DDLLogger().Error("performing DDL change failed", zap.Error(err))
	}
	return nil
}

// OnSchemaStateChanged overrides the ddl Callback interface.
func (c *ctcCallback) OnSchemaStateChanged(_ int64) {
	err := c.do.Reload()
	if err != nil {
		logutil.DDLLogger().Error("domain callback failed on schema state changed", zap.Error(err))
	}
}

// OnJobRunBefore is used to run the user customized logic of `onJobRunBefore` first.
func (*ctcCallback) OnJobRunBefore(job *model.Job) {
	log.Info("on job run before", zap.String("job", job.String()))
	// Only block the ctc type ddl here.
	if job.Type != model.ActionModifyColumn {
		return
	}
	switch job.SchemaState {
	case model.StateDeleteOnly, model.StateWriteOnly, model.StateWriteReorganization:
		logutil.DDLLogger().Warn(fmt.Sprintf("[DDL_HOOK] Hang for 0.5 seconds on %s state triggered", job.SchemaState.String()))
		time.Sleep(500 * time.Millisecond)
	}
}

func newCTCCallBack(do DomainReloader) Callback {
	return &ctcCallback{do: do}
}

// ****************************** End of CTC DDL Callback Instance ***************************************************

var (
	customizedCallBackRegisterMap = map[string]func(do DomainReloader) Callback{}
)

func init() {
	// init the callback register map.
	customizedCallBackRegisterMap["default_hook"] = newDefaultCallBack
	customizedCallBackRegisterMap["ctc_hook"] = newCTCCallBack
}

// GetCustomizedHook get the hook registered in the hookMap.
func GetCustomizedHook(s string) (func(do DomainReloader) Callback, error) {
	s = strings.ToLower(s)
	s = strings.TrimSpace(s)
	fact, ok := customizedCallBackRegisterMap[s]
	if !ok {
		logutil.DDLLogger().Error("bad ddl hook " + s)
		return nil, errors.Errorf("ddl hook `%s` is not found in hook registered map", s)
	}
	return fact, nil
}
