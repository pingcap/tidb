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
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/parser/model"
	"go.uber.org/zap"
)

// Callback is used for DDL.
type Callback interface {
	// OnJobRunBefore is called before running job.
	OnJobRunBefore(job *model.Job)
	// OnJobRunAfter is called after running job.
	OnJobRunAfter(job *model.Job)
	// OnJobUpdated is called after the running job is updated.
	OnJobUpdated(job *model.Job)
}

// BaseCallback implements Callback.OnChanged interface.
type BaseCallback struct {
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

// SchemaLoader is used to avoid import loop, the only impl is domain currently.
type SchemaLoader interface {
	Reload() error
}

// ****************************** Start of Customized DDL Callback Instance ****************************************

// DefaultCallback is the default callback that TiDB will use.
type DefaultCallback struct {
	*BaseCallback
	do SchemaLoader
}

func newDefaultCallBack(do SchemaLoader) Callback {
	return &DefaultCallback{BaseCallback: &BaseCallback{}, do: do}
}

// ****************************** End of Default DDL Callback Instance *********************************************

// ****************************** Start of CTC DDL Callback Instance ***********************************************

// ctcCallback is the customized callback that TiDB will use.
// ctc is named from column type change, here after we call them ctc for short.
type ctcCallback struct {
	*BaseCallback
	do SchemaLoader
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

func newCTCCallBack(do SchemaLoader) Callback {
	return &ctcCallback{do: do}
}

// ****************************** End of CTC DDL Callback Instance ***************************************************

var (
	customizedCallBackRegisterMap = map[string]func(do SchemaLoader) Callback{}
)

func init() {
	// init the callback register map.
	customizedCallBackRegisterMap["default_hook"] = newDefaultCallBack
	customizedCallBackRegisterMap["ctc_hook"] = newCTCCallBack
}

// GetCustomizedHook get the hook registered in the hookMap.
func GetCustomizedHook(s string) (func(do SchemaLoader) Callback, error) {
	s = strings.ToLower(s)
	s = strings.TrimSpace(s)
	fact, ok := customizedCallBackRegisterMap[s]
	if !ok {
		logutil.DDLLogger().Error("bad ddl hook " + s)
		return nil, errors.Errorf("ddl hook `%s` is not found in hook registered map", s)
	}
	return fact, nil
}
