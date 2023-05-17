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

// GetTimerOption is the option to get timers
type GetTimerOption func(*TimerCond)

// WithKey indicates to get a timer with the specified key
func WithKey(key string) GetTimerOption {
	return func(cond *TimerCond) {
		cond.Key.Set(key)
		cond.KeyPrefix = false
	}
}

// WithKeyPrefix to get timers with the indicated key prefix
func WithKeyPrefix(keyPrefix string) GetTimerOption {
	return func(cond *TimerCond) {
		cond.Key.Set(keyPrefix)
		cond.KeyPrefix = true
	}
}

// WithID indicates to get a timer with the specified id
func WithID(id string) GetTimerOption {
	return func(cond *TimerCond) {
		cond.ID.Set(id)
	}
}

// UpdateTimerOption is the option to update the timer
type UpdateTimerOption func(*TimerUpdate)

// WithSetEnable indicates to set the timer's `Enable` field
func WithSetEnable(enable bool) UpdateTimerOption {
	return func(update *TimerUpdate) {
		update.Enable.Set(enable)
	}
}

// WithSetSchedExpr indicates to set the timer's schedule policy
func WithSetSchedExpr(tp SchedPolicyType, expr string) UpdateTimerOption {
	return func(update *TimerUpdate) {
		update.SchedPolicyType.Set(tp)
		update.SchedPolicyExpr.Set(expr)
	}
}

// WithSetWatermark indicates to set the timer's watermark
func WithSetWatermark(watermark time.Time) UpdateTimerOption {
	return func(update *TimerUpdate) {
		update.Watermark.Set(watermark)
	}
}

// WithSetSummaryData indicates to set the timer's summary
func WithSetSummaryData(summary []byte) UpdateTimerOption {
	return func(update *TimerUpdate) {
		update.SummaryData.Set(summary)
	}
}

// TimerClient is an interface exposed to user to manage timers
type TimerClient interface {
	// GetDefaultNamespace returns the default namespace of this client
	GetDefaultNamespace() string
	// CreateTimer creates a new timer
	CreateTimer(ctx context.Context, spec TimerSpec) (*TimerRecord, error)
	// GetTimerByID queries the timer by ID
	GetTimerByID(ctx context.Context, timerID string) (*TimerRecord, error)
	// GetTimerByKey queries the timer by key
	GetTimerByKey(ctx context.Context, key string) (*TimerRecord, error)
	// GetTimers queries timers by options
	GetTimers(ctx context.Context, opt ...GetTimerOption) ([]*TimerRecord, error)
	// UpdateTimer updates a timer
	UpdateTimer(ctx context.Context, timerID string, opt ...UpdateTimerOption) error
	// CloseTimerEvent closes the triggering event of a timer
	CloseTimerEvent(ctx context.Context, timerID string, eventID string, opts ...UpdateTimerOption) error
	// DeleteTimer deletes a timer
	DeleteTimer(ctx context.Context, timerID string) (bool, error)
}
