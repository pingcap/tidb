// Copyright 2022 PingCAP, Inc.
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

package taskstop

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/stringutil"
)

type StopPoint struct {
	name  string
	value []any
}

func NewStopPoint(name string, value ...any) *StopPoint {
	return &StopPoint{
		name:  name,
		value: value,
	}
}

func (p *StopPoint) Name() string {
	return p.name
}

func (p *StopPoint) Value() []any {
	return p.value
}

type Chan struct {
	ch1 chan *StopPoint
	ch2 chan any
}

func NewChan() *Chan {
	return &Chan{
		ch1: make(chan *StopPoint),
		ch2: make(chan any),
	}
}

func (ch *Chan) SignalOnStopAt(stopName string) error {
	select {
	case ch.ch1 <- NewStopPoint(stopName):
		return nil
	default:
		return errors.New("Cannot signal stop at")
	}

}

func (ch *Chan) WaitOnStop() chan *StopPoint {
	return ch.ch1
}

func (ch *Chan) SignalStep() error {
	select {
	case ch.ch2 <- struct{}{}:
		return nil
	default:
		return errors.New("Cannot signal step")
	}
}

func (ch *Chan) WaitStepSignal() chan any {
	return ch.ch2
}

type sessionStopInjection struct {
	ch             *Chan
	stopEveryPoint bool
	stopList       []string
}

func EnableSessionStopPoint(sctx sessionctx.Context, c *Chan, stopList ...string) {
	sctx.SetValue(stringutil.StringerStr("sessionStopInjection"), &sessionStopInjection{
		ch:             c,
		stopEveryPoint: len(stopList) == 0,
		stopList:       stopList,
	})
}

func DisableSessionStopPoint(sctx sessionctx.Context) {
	sctx.SetValue(stringutil.StringerStr("sessionStopInjection"), nil)
}

func InjectSessionStopPoint(sctx sessionctx.Context, stopName string) {
	failpoint.Inject("sessionStop", func() {
		if inject, ok := sctx.Value(stringutil.StringerStr("sessionStopInjection")).(*sessionStopInjection); ok {
			if !inject.stopEveryPoint {
				shouldStop := false
				for _, stop := range inject.stopList {
					if stop == stopName {
						shouldStop = true
					}
				}

				if !shouldStop {
					return
				}
			}

			if err := inject.ch.SignalOnStopAt(stopName); err != nil {
				panic(err)
			}
			<-inject.ch.WaitStepSignal()
		}
	})
}

func EnableGlobalSessionStopFailPoint() error {
	return failpoint.Enable("github.com/pingcap/tidb/util/taskstop/sessionStop", "return")
}

func DisableGlobalSessionStopFailPoint() error {
	return failpoint.Disable("github.com/pingcap/tidb/util/taskstop/sessionStop")
}

func IsGlobalSessionStopFailPointEnabled() bool {
	status, err := failpoint.Status("github.com/pingcap/tidb/util/taskstop/sessionStop")
	return err == nil && status == "return"
}
