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

package api

import (
	"context"
	"time"
)

// TimerShedEvent is an interface which gives the timer's schedule event's information.
type TimerShedEvent interface {
	// EventID returns the event ID the current event.
	EventID() string
	// Timer returns the timer record object of the current event.
	Timer() *TimerRecord
}

// PreSchedEventResult is the result of `OnPreSchedEvent`.
type PreSchedEventResult struct {
	// Delay indicates to delay the event after a while.
	// If `Delay` is 0, it means no delay, and then `OnSchedEvent` will be called.
	// Otherwise, after a while according to `Delay`, `OnPreSchedEvent` will be called again to
	// check whether to trigger the event.
	Delay time.Duration
	// EventData indicates the data should be passed to the event that should be triggered.
	// EventData can be used to store some pre-computed configurations of the next event.
	EventData []byte
}

// Hook is an interface which should be implemented by user to tell framework how to trigger an event.
// Several timers with a same hook class can share one hook in a runtime.
type Hook interface {
	// Start starts the hook.
	Start()
	// Stop stops the hook. When it is called, this means the framework is shutting down.
	Stop()
	// OnPreSchedEvent will be called before triggering a new event. It's return value tells the next action of the triggering.
	// For example, if `TimerShedEvent.Delay` is a non-zero value, the event triggering will be postponed.
	// Notice that `event.Timer().EventID` will be empty because the current event is not actually triggered,
	// use `event.EventID()` to get the event id instead.
	OnPreSchedEvent(ctx context.Context, event TimerShedEvent) (PreSchedEventResult, error)
	// OnSchedEvent will be called when a new event is triggered.
	OnSchedEvent(ctx context.Context, event TimerShedEvent) error
}

// HookFactory is the factory function to construct a new Hook object with `hookClass`.
type HookFactory func(hookClass string, cli TimerClient) Hook
