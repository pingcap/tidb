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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/duration"
)

// SchedPolicyType is the type of the event schedule policy
type SchedPolicyType string

const (
	// SchedEventInterval indicates to schedule events every fixed interval
	SchedEventInterval SchedPolicyType = "INTERVAL"
)

// SchedEventPolicy is an interface to tell the runtime how to schedule a timer's events
type SchedEventPolicy interface {
	// NextEventTime returns the time to schedule the next timer event. If the second return value is true,
	// it means we have a next event schedule after `watermark`. Otherwise, it means there is no more event after `watermark`.
	NextEventTime(watermark time.Time) (time.Time, bool)
}

// SchedIntervalPolicy implements SchedEventPolicy, it is the policy of type `SchedEventInterval`
type SchedIntervalPolicy struct {
	expr     string
	interval time.Duration
}

// NewSchedIntervalPolicy creates a new SchedIntervalPolicy
func NewSchedIntervalPolicy(expr string) (*SchedIntervalPolicy, error) {
	interval, err := duration.ParseDuration(expr)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid schedule event expr '%s'", expr)
	}

	return &SchedIntervalPolicy{
		expr:     expr,
		interval: interval,
	}, nil
}

// NextEventTime returns the next time of the timer event
// A next event should be triggered after a time indicated by `interval` after watermark
func (p *SchedIntervalPolicy) NextEventTime(watermark time.Time) (time.Time, bool) {
	if watermark.IsZero() {
		return watermark, true
	}
	return watermark.Add(p.interval), true
}

// TimerSpec is the specification of a timer without any runtime status
type TimerSpec struct {
	// Namespace is the namespace of the timer
	Namespace string
	// Key is the key of the timer. Key is unique in each namespace
	Key string
	// Data is a binary which is defined by user
	Data []byte
	// SchedPolicyType is the type of the event schedule policy
	SchedPolicyType SchedPolicyType
	// SchedPolicyExpr is the expression of event schedule policy with the type specified by SchedPolicyType
	SchedPolicyExpr string
	// HookClass is the class of the hook
	HookClass string
	// Watermark indicates the progress the timer's event schedule
	Watermark time.Time
	// Enable indicated whether the timer is enabled.
	// If it is false, the new timer event will not be scheduled even it is up to time.
	Enable bool
}

// Clone returns a cloned TimerSpec
func (t *TimerSpec) Clone() *TimerSpec {
	clone := *t
	return &clone
}

// Validate validates the TimerSpec
func (t *TimerSpec) Validate() error {
	if t.Namespace == "" {
		return errors.New("field 'Namespace' should not be empty")
	}

	if t.Key == "" {
		return errors.New("field 'Key' should not be empty")
	}

	if t.SchedPolicyType == "" {
		return errors.New("field 'SchedPolicyType' should not be empty")
	}

	if _, err := t.CreateSchedEventPolicy(); err != nil {
		return errors.Wrap(err, "schedule event configuration is not valid")
	}

	return nil
}

// CreateSchedEventPolicy creates a SchedEventPolicy according to `SchedPolicyType` and `SchedPolicyExpr`
func (t *TimerSpec) CreateSchedEventPolicy() (SchedEventPolicy, error) {
	return CreateSchedEventPolicy(t.SchedPolicyType, t.SchedPolicyExpr)
}

// CreateSchedEventPolicy creates a SchedEventPolicy according to `SchedPolicyType` and `SchedPolicyExpr`
func CreateSchedEventPolicy(tp SchedPolicyType, expr string) (SchedEventPolicy, error) {
	switch tp {
	case SchedEventInterval:
		return NewSchedIntervalPolicy(expr)
	default:
		return nil, errors.Errorf("invalid schedule event type: '%s'", tp)
	}
}

// SchedEventStatus is the current schedule status of timer's event
type SchedEventStatus string

const (
	// SchedEventIdle means the timer is not in trigger state currently
	SchedEventIdle SchedEventStatus = "IDLE"
	// SchedEventTrigger means the timer is in trigger state
	SchedEventTrigger SchedEventStatus = "TRIGGER"
)

// TimerRecord is the timer record saved in the timer store
type TimerRecord struct {
	TimerSpec
	// ID is the id of timer, it is unique and auto assigned by the store when created.
	ID string
	// EventStatus indicates the current schedule status of the timer's event
	EventStatus SchedEventStatus
	// EventID indicates the id of current triggered event
	// If the `EventStatus` is `IDLE`, this value should be empty
	EventID string
	// EventData indicates the data of current triggered event
	// If the `EventStatus` is `IDLE`, this value should be empty
	EventData []byte
	// EventStart indicates the start time of current triggered event
	// If the `EventStatus` is `IDLE`, `EventStart.IsZero()` should returns true
	EventStart time.Time
	// SummaryData is a binary which is used to store some summary information of the timer.
	// User can update it when closing a timer's event to update the summary.
	SummaryData []byte
	// CreateTime is the creation time of the timer
	CreateTime time.Time
	// Version is the version of the record, when the record updated, version will be increased.
	Version uint64
}

// Clone returns a cloned TimerRecord
func (r *TimerRecord) Clone() *TimerRecord {
	cloned := *r
	cloned.TimerSpec = *r.TimerSpec.Clone()
	return &cloned
}
