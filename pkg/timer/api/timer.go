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
	"github.com/pingcap/tidb/pkg/parser/duration"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/robfig/cron/v3"
)

// SchedPolicyType is the type of the event schedule policy.
type SchedPolicyType string

const (
	// SchedEventInterval indicates to schedule events every fixed interval.
	SchedEventInterval SchedPolicyType = "INTERVAL"
	// SchedEventCron indicates to schedule events by cron expression.
	SchedEventCron SchedPolicyType = "CRON"
)

// SchedEventPolicy is an interface to tell the runtime how to schedule a timer's events.
type SchedEventPolicy interface {
	// NextEventTime returns the time to schedule the next timer event. If the second return value is true,
	// it means we have a next event schedule after `watermark`. Otherwise, it means there is no more event after `watermark`.
	NextEventTime(watermark time.Time) (time.Time, bool)
}

// SchedIntervalPolicy implements SchedEventPolicy, it is the policy of type `SchedEventInterval`.
type SchedIntervalPolicy struct {
	expr     string
	interval time.Duration
}

// NewSchedIntervalPolicy creates a new SchedIntervalPolicy.
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

// NextEventTime returns the next time of the timer event.
// A next event should be triggered after a time indicated by `interval` after watermark.
func (p *SchedIntervalPolicy) NextEventTime(watermark time.Time) (time.Time, bool) {
	if watermark.IsZero() {
		return watermark, true
	}
	return watermark.Add(p.interval), true
}

// CronPolicy implements SchedEventPolicy, it is the policy of type `SchedEventCron`.
type CronPolicy struct {
	cronSchedule cron.Schedule
}

// NewCronPolicy creates a new CronPolicy.
func NewCronPolicy(expr string) (*CronPolicy, error) {
	cronSchedule, err := cron.ParseStandard(expr)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid cron expr '%s'", expr)
	}

	return &CronPolicy{
		cronSchedule: cronSchedule,
	}, nil
}

// NextEventTime returns the next time of the timer event.
func (p *CronPolicy) NextEventTime(watermark time.Time) (time.Time, bool) {
	next := p.cronSchedule.Next(watermark)
	return next, !next.IsZero()
}

// ManualRequest is the request info to trigger timer manually.
type ManualRequest struct {
	// ManualRequestID is the id of manual request.
	ManualRequestID string
	// ManualRequestTime is the request time.
	ManualRequestTime time.Time
	// ManualTimeout is the timeout for the request, if the timer is not triggered after timeout, processed will be set to true
	// with empty event id.
	ManualTimeout time.Duration
	// ManualProcessed indicates the request is processed (triggered or timeout).
	ManualProcessed bool
	// ManualEventID means the triggered event id for the current request.
	ManualEventID string
}

// IsManualRequesting indicates that whether this timer is requesting to trigger event manually.
func (r *ManualRequest) IsManualRequesting() bool {
	return r.ManualRequestID != "" && !r.ManualProcessed
}

// SetProcessed sets the timer's manual request to processed.
func (r *ManualRequest) SetProcessed(eventID string) ManualRequest {
	newManual := *r
	newManual.ManualProcessed = true
	newManual.ManualEventID = eventID
	return newManual
}

// EventExtra stores some extra attributes for event.
type EventExtra struct {
	// EventManualRequestID is the related request id of the manual trigger.
	// If current event is not triggered manually, it is empty.
	EventManualRequestID string
	// EventWatermark is the watermark when event triggers.
	EventWatermark time.Time
}

// TimerSpec is the specification of a timer without any runtime status.
type TimerSpec struct {
	// Namespace is the namespace of the timer.
	Namespace string
	// Key is the key of the timer. Key is unique in each namespace.
	Key string
	// Tags is used to tag a timer.
	Tags []string
	// Data is a binary which is defined by user.
	Data []byte
	// TimeZone is the time zone name of the timer to evaluate the schedule policy.
	// If TimeZone is empty, it means to use the tidb cluster's time zone.
	TimeZone string
	// SchedPolicyType is the type of the event schedule policy.
	SchedPolicyType SchedPolicyType
	// SchedPolicyExpr is the expression of event schedule policy with the type specified by SchedPolicyType.
	SchedPolicyExpr string
	// HookClass is the class of the hook.
	HookClass string
	// Watermark indicates the progress the timer's event schedule.
	Watermark time.Time
	// Enable indicated whether the timer is enabled.
	// If it is false, the new timer event will not be scheduled even it is up to time.
	Enable bool
}

// Clone returns a cloned TimerSpec.
func (t *TimerSpec) Clone() *TimerSpec {
	clone := *t
	return &clone
}

// Validate validates the TimerSpec.
func (t *TimerSpec) Validate() error {
	if t.Namespace == "" {
		return errors.New("field 'Namespace' should not be empty")
	}

	if t.Key == "" {
		return errors.New("field 'Key' should not be empty")
	}

	if err := ValidateTimeZone(t.TimeZone); err != nil {
		return err
	}

	if t.SchedPolicyType == "" {
		return errors.New("field 'SchedPolicyType' should not be empty")
	}

	if _, err := t.CreateSchedEventPolicy(); err != nil {
		return errors.Wrap(err, "schedule event configuration is not valid")
	}

	return nil
}

// CreateSchedEventPolicy creates a SchedEventPolicy according to `SchedPolicyType` and `SchedPolicyExpr`.
func (t *TimerSpec) CreateSchedEventPolicy() (SchedEventPolicy, error) {
	return CreateSchedEventPolicy(t.SchedPolicyType, t.SchedPolicyExpr)
}

// CreateSchedEventPolicy creates a SchedEventPolicy according to `SchedPolicyType` and `SchedPolicyExpr`.
func CreateSchedEventPolicy(tp SchedPolicyType, expr string) (SchedEventPolicy, error) {
	switch tp {
	case SchedEventInterval:
		return NewSchedIntervalPolicy(expr)
	case SchedEventCron:
		return NewCronPolicy(expr)
	default:
		return nil, errors.Errorf("invalid schedule event type: '%s'", tp)
	}
}

// SchedEventStatus is the current schedule status of timer's event.
type SchedEventStatus string

const (
	// SchedEventIdle means the timer is not in trigger state currently.
	SchedEventIdle SchedEventStatus = "IDLE"
	// SchedEventTrigger means the timer is in trigger state.
	SchedEventTrigger SchedEventStatus = "TRIGGER"
)

// TimerRecord is the timer record saved in the timer store.
type TimerRecord struct {
	TimerSpec
	// ID is the id of timer, it is unique and auto assigned by the store when created.
	ID string
	// ManualRequest is the request to trigger timer event manually.
	ManualRequest
	// EventStatus indicates the current schedule status of the timer's event.
	EventStatus SchedEventStatus
	// EventID indicates the id of current triggered event.
	// If the `EventStatus` is `IDLE`, this value should be empty.
	EventID string
	// EventData indicates the data of current triggered event.
	// If the `EventStatus` is `IDLE`, this value should be empty.
	EventData []byte
	// EventStart indicates the start time of current triggered event.
	// If the `EventStatus` is `IDLE`, `EventStart.IsZero()` should returns true.
	EventStart time.Time
	// EventExtra stores some extra attributes for event
	EventExtra
	// SummaryData is a binary which is used to store some summary information of the timer.
	// User can update it when closing a timer's event to update the summary.
	SummaryData []byte
	// CreateTime is the creation time of the timer.
	CreateTime time.Time
	// Version is the version of the record, when the record updated, version will be increased.
	Version uint64
	// Location is used to get the alias of TiDB timezone.
	Location *time.Location
}

// NextEventTime returns the next time for timer to schedule
func (r *TimerRecord) NextEventTime() (tm time.Time, _ bool, _ error) {
	if !r.Enable {
		return tm, false, nil
	}

	watermark := r.Watermark
	if loc := r.Location; loc != nil {
		watermark = watermark.In(loc)
	}

	policy, err := CreateSchedEventPolicy(r.SchedPolicyType, r.SchedPolicyExpr)
	if err != nil {
		return tm, false, err
	}

	tm, ok := policy.NextEventTime(watermark)
	return tm, ok, nil
}

// Clone returns a cloned TimerRecord.
func (r *TimerRecord) Clone() *TimerRecord {
	cloned := *r
	cloned.TimerSpec = *r.TimerSpec.Clone()
	return &cloned
}

// ValidateTimeZone validates the TimeZone field.
func ValidateTimeZone(tz string) error {
	if tz != "" {
		if _, err := timeutil.ParseTimeZone(tz); err != nil {
			return err
		}
	}
	return nil
}
