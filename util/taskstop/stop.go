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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/stringutil"
)

// StartPointName is the stop point for start
var StartPointName = "START"

// DonePointName is the stop point for done
var DonePointName = "DONE"

// StopPoint is stop point object
type StopPoint struct {
	name string
}

// NewStopPoint creates a new stop point
func NewStopPoint(name string) *StopPoint {
	return &StopPoint{
		name: name,
	}
}

// Name returns the name of the stop point
func (p *StopPoint) Name() string {
	return p.name
}

// Chan is used to communicate with between stepped task and other thread
type Chan struct {
	ch1 chan *StopPoint
	ch2 chan any
}

// NewChan creates a new Chan
func NewChan() *Chan {
	return &Chan{
		ch1: make(chan *StopPoint),
		ch2: make(chan any),
	}
}

// SignalOnStopAt writes the chan to indicate that task now stopped at a point
func (ch *Chan) SignalOnStopAt(stopName string) error {
	select {
	case ch.ch1 <- NewStopPoint(stopName):
		return nil
	case <-time.After(time.Second * 10):
		return errors.New("Cannot signal stop at")
	}

}

// WaitOnStop returns a chan to wait on stop signal
func (ch *Chan) WaitOnStop() chan *StopPoint {
	return ch.ch1
}

// SignalStep writes the chan to tell the task to continue
func (ch *Chan) SignalStep(val any) error {
	select {
	case ch.ch2 <- val:
		return nil
	case <-time.After(time.Second * 10):
		return errors.New("Cannot signal step")
	}
}

// WaitStepSignal returns a chan to wait step signal
func (ch *Chan) WaitStepSignal() chan any {
	return ch.ch2
}

type sessionStopInjection struct {
	ch             *Chan
	stopEveryPoint bool
	stopList       []string
}

// EnableSessionStopPoint enables the stop points for a session
func EnableSessionStopPoint(sctx sessionctx.Context, c *Chan, stopList ...string) {
	sctx.SetValue(stringutil.StringerStr("sessionStopInjection"), &sessionStopInjection{
		ch:             c,
		stopEveryPoint: len(stopList) == 0,
		stopList:       stopList,
	})
}

// DisableSessionStopPoint disables the stop points for a session
func DisableSessionStopPoint(sctx sessionctx.Context) {
	sctx.SetValue(stringutil.StringerStr("sessionStopInjection"), nil)
}

// InjectSessionStopPoint injects a stop point
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

// EnableGlobalSessionStopFailPoint enables the global session stop fail point
func EnableGlobalSessionStopFailPoint() error {
	return failpoint.Enable("github.com/pingcap/tidb/util/taskstop/sessionStop", "return")
}

// DisableGlobalSessionStopFailPoint disables the global session stop fail point
func DisableGlobalSessionStopFailPoint() error {
	return failpoint.Disable("github.com/pingcap/tidb/util/taskstop/sessionStop")
}

// IsGlobalSessionStopFailPointEnabled returns whether the global session stop fail point is enabled
func IsGlobalSessionStopFailPointEnabled() bool {
	status, err := failpoint.Status("github.com/pingcap/tidb/util/taskstop/sessionStop")
	return err == nil && status == "return"
}
