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
	"reflect"
	"slices"
	"strings"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
)

type optionalVal interface {
	optionalVal()
	Present() bool
	Clear()
}

// OptionalVal is used by `TimerCond` and `TimerUpdate` to indicate.
// whether some field's condition has been set or whether is field should be updated.
type OptionalVal[T any] struct {
	v       T
	present bool
}

// NewOptionalVal creates a new OptionalVal.
func NewOptionalVal[T any](val T) (o OptionalVal[T]) {
	o.Set(val)
	return
}

func (*OptionalVal[T]) optionalVal() {}

// Present indicates whether the field value is set.
func (o *OptionalVal[T]) internalPresent() bool {
	return o.present
}

// Present indicates whether the field value is set.
func (o *OptionalVal[T]) Present() bool {
	return o.present
}

// Get returns the current value, if the second return value is false, this means it is not set.
func (o *OptionalVal[T]) Get() (v T, present bool) {
	return o.v, o.present
}

// Set sets the value.
func (o *OptionalVal[T]) Set(v T) {
	o.v, o.present = v, true
}

// Clear clears the value. The `Present()` will return false after `Clear` is called.
func (o *OptionalVal[T]) Clear() {
	var v T
	o.v, o.present = v, false
}

func iterOptionalFields(v reflect.Value, excludes []unsafe.Pointer, fn func(name string, val optionalVal) bool) {
	tp := v.Type()
loop:
	for i := 0; i < v.NumField(); i++ {
		fieldVal := v.Field(i).Addr()
		optVal, ok := fieldVal.Interface().(optionalVal)
		if !ok {
			continue
		}

		for _, ex := range excludes {
			if fieldVal.UnsafePointer() == ex {
				continue loop
			}
		}

		if !fn(tp.Field(i).Name, optVal) {
			break loop
		}
	}
}

// TimerCond is the condition to filter a timer record.
type TimerCond struct {
	// ID indicates to filter the timer record with ID.
	ID OptionalVal[string]
	// Namespace indicates to filter the timer by Namespace.
	Namespace OptionalVal[string]
	// Key indicates to filter the timer record with ID.
	// The filter behavior is defined by `KeyPrefix`.
	Key OptionalVal[string]
	// KeyPrefix indicates how to filter with timer's key if `Key` is set.
	// If `KeyPrefix is` true, it will check whether the timer's key is prefixed with `TimerCond.Key`.
	// Otherwise, it will check whether the timer's key equals `TimerCond.Key`.
	KeyPrefix bool
	// Tags indicates to filter the timer record with specified tags.
	Tags OptionalVal[[]string]
}

// Match will return whether the condition match the timer record.
func (c *TimerCond) Match(t *TimerRecord) bool {
	if val, ok := c.ID.Get(); ok && t.ID != val {
		return false
	}

	if val, ok := c.Namespace.Get(); ok && t.Namespace != val {
		return false
	}

	if val, ok := c.Key.Get(); ok {
		if c.KeyPrefix && !strings.HasPrefix(t.Key, val) {
			return false
		}

		if !c.KeyPrefix && t.Key != val {
			return false
		}
	}

	if vals, ok := c.Tags.Get(); ok {
		for _, val := range vals {
			if !slices.Contains(t.Tags, val) {
				return false
			}
		}
	}

	return true
}

// FieldsSet returns all fields that has been set exclude excludes.
func (c *TimerCond) FieldsSet(excludes ...unsafe.Pointer) (fields []string) {
	iterOptionalFields(reflect.ValueOf(c).Elem(), excludes, func(name string, val optionalVal) bool {
		if val.Present() {
			fields = append(fields, name)
		}
		return true
	})
	return
}

// Clear clears all fields.
func (c *TimerCond) Clear() {
	iterOptionalFields(reflect.ValueOf(c).Elem(), nil, func(_ string, val optionalVal) bool {
		val.Clear()
		return true
	})
	c.KeyPrefix = false
}

// TimerUpdate indicates how to update a timer.
type TimerUpdate struct {
	// Tags indicates to set all tags for a timer.
	Tags OptionalVal[[]string]
	// Enable indicates to set the timer's `Enable` field.
	Enable OptionalVal[bool]
	// TimeZone indicates to set the timer's `TimeZone` field.
	TimeZone OptionalVal[string]
	// SchedPolicyType indicates to set the timer's `SchedPolicyType` field.
	SchedPolicyType OptionalVal[SchedPolicyType]
	// SchedPolicyExpr indicates to set the timer's `SchedPolicyExpr` field.
	SchedPolicyExpr OptionalVal[string]
	// ManualRequest indicates to set the timer's manual request.
	ManualRequest OptionalVal[ManualRequest]
	// EventStatus indicates the event status.
	EventStatus OptionalVal[SchedEventStatus]
	// EventID indicates to set the timer event id.
	EventID OptionalVal[string]
	// EventData indicates to set the timer event data.
	EventData OptionalVal[[]byte]
	// EventStart indicates the start time of event.
	EventStart OptionalVal[time.Time]
	// EventExtra indicates to set the `EventExtra` field.
	EventExtra OptionalVal[EventExtra]
	// Watermark indicates to set the timer's `Watermark` field.
	Watermark OptionalVal[time.Time]
	// SummaryData indicates to set the timer's `Summary` field.
	SummaryData OptionalVal[[]byte]
	// CheckVersion indicates to check the timer's version when updated.
	CheckVersion OptionalVal[uint64]
	// CheckEventID indicates to check the timer's eventID.
	CheckEventID OptionalVal[string]
}

// apply applies the update to a timer.
func (u *TimerUpdate) apply(record *TimerRecord) (*TimerRecord, error) {
	if v, ok := u.CheckVersion.Get(); ok && record.Version != v {
		return nil, errors.Trace(ErrVersionNotMatch)
	}

	if v, ok := u.CheckEventID.Get(); ok && record.EventID != v {
		return nil, errors.Trace(ErrEventIDNotMatch)
	}

	record = record.Clone()
	if v, ok := u.Tags.Get(); ok {
		if len(v) == 0 {
			v = nil
		}
		record.Tags = v
	}

	if v, ok := u.Enable.Get(); ok {
		record.Enable = v
	}

	if v, ok := u.TimeZone.Get(); ok {
		record.TimeZone = v
		record.Location = getMemStoreTimeZoneLoc(record.TimeZone)
	}

	if v, ok := u.SchedPolicyType.Get(); ok {
		record.SchedPolicyType = v
	}

	if v, ok := u.SchedPolicyExpr.Get(); ok {
		record.SchedPolicyExpr = v
	}

	if v, ok := u.ManualRequest.Get(); ok {
		record.ManualRequest = v
	}

	if v, ok := u.EventStatus.Get(); ok {
		record.EventStatus = v
	}

	if v, ok := u.EventID.Get(); ok {
		record.EventID = v
	}

	if v, ok := u.EventData.Get(); ok {
		record.EventData = v
	}

	if v, ok := u.EventStart.Get(); ok {
		record.EventStart = v
	}

	if v, ok := u.EventExtra.Get(); ok {
		record.EventExtra = v
	}

	if v, ok := u.Watermark.Get(); ok {
		record.Watermark = v
	}

	if v, ok := u.SummaryData.Get(); ok {
		record.SummaryData = v
	}

	return record, nil
}

// FieldsSet returns all fields that has been set exclude excludes.
func (u *TimerUpdate) FieldsSet(excludes ...unsafe.Pointer) (fields []string) {
	iterOptionalFields(reflect.ValueOf(u).Elem(), excludes, func(name string, val optionalVal) bool {
		if val.Present() {
			fields = append(fields, name)
		}
		return true
	})
	return
}

// Clear clears all fields.
func (u *TimerUpdate) Clear() {
	iterOptionalFields(reflect.ValueOf(u).Elem(), nil, func(_ string, val optionalVal) bool {
		val.Clear()
		return true
	})
}

// Cond is an interface to match a timer record.
type Cond interface {
	// Match returns whether a record match the condition.
	Match(timer *TimerRecord) bool
}

// OperatorTp is the operator type of the condition.
type OperatorTp int8

const (
	// OperatorAnd means 'AND' operator.
	OperatorAnd OperatorTp = iota
	// OperatorOr means 'OR' operator.
	OperatorOr
)

// Operator implements Cond.
type Operator struct {
	// Op indicates the operator type.
	Op OperatorTp
	// Not indicates whether to apply 'NOT' to the condition.
	Not bool
	// Children is the children of the operator.
	Children []Cond
}

// And returns a condition `AND(child1, child2, ...)`.
func And(children ...Cond) *Operator {
	return &Operator{
		Op:       OperatorAnd,
		Children: children,
	}
}

// Or returns a condition `OR(child1, child2, ...)`.
func Or(children ...Cond) *Operator {
	return &Operator{
		Op:       OperatorOr,
		Children: children,
	}
}

// Not returns a condition `NOT(cond)`.
func Not(cond Cond) *Operator {
	if c, ok := cond.(*Operator); ok {
		newCriteria := *c
		newCriteria.Not = !c.Not
		return &newCriteria
	}
	return &Operator{
		Op:       OperatorAnd,
		Not:      true,
		Children: []Cond{cond},
	}
}

// Match will return whether the condition match the timer record.
func (c *Operator) Match(t *TimerRecord) bool {
	switch c.Op {
	case OperatorAnd:
		for _, item := range c.Children {
			if !item.Match(t) {
				return c.Not
			}
		}
		return !c.Not
	case OperatorOr:
		for _, item := range c.Children {
			if item.Match(t) {
				return !c.Not
			}
		}
		return c.Not
	}
	return false
}

// WatchTimerEventType is the type of the watch event.
type WatchTimerEventType int8

const (
	// WatchTimerEventCreate indicates that a new timer is created.
	WatchTimerEventCreate WatchTimerEventType = 1 << iota
	// WatchTimerEventUpdate indicates that a timer is updated.
	WatchTimerEventUpdate
	// WatchTimerEventDelete indicates that a timer is deleted.
	WatchTimerEventDelete
)

// WatchTimerEvent is the watch event object.
type WatchTimerEvent struct {
	// Tp indicates the event type.
	Tp WatchTimerEventType
	// TimerID indicates the timer id for the event.
	TimerID string
}

// WatchTimerResponse is the response of watch.
type WatchTimerResponse struct {
	// Events contains all events in the response.
	Events []*WatchTimerEvent
}

// WatchTimerChan is the chan of the watch timer.
type WatchTimerChan <-chan WatchTimerResponse

// TimerStoreCore is an interface, it contains several core methods of store.
type TimerStoreCore interface {
	// Create creates a new record. If `record.ID` is empty, an id will be assigned automatically.
	// The first return value is the final id of the timer.
	Create(ctx context.Context, record *TimerRecord) (string, error)
	// List lists the timers that match the condition.
	List(ctx context.Context, cond Cond) ([]*TimerRecord, error)
	// Update updates a timer.
	Update(ctx context.Context, timerID string, update *TimerUpdate) error
	// Delete deletes a timer.
	Delete(ctx context.Context, timerID string) (bool, error)
	// WatchSupported indicates whether watch supported for this store.
	WatchSupported() bool
	// Watch watches all changes of the store. A chan will be returned to receive `WatchTimerResponse`
	// The returned chan be closed when the context in the argument is done.
	Watch(ctx context.Context) WatchTimerChan
	// Close closes the store
	Close()
}

// TimerStore extends TimerStoreCore to provide some extra methods for timer operations.
type TimerStore struct {
	TimerStoreCore
}

// GetByID gets a timer by ID. If the timer with the specified ID not exists,
// an error `ErrTimerNotExist` will be returned.
func (s *TimerStore) GetByID(ctx context.Context, timerID string) (record *TimerRecord, err error) {
	return s.getOneRecord(ctx, &TimerCond{ID: NewOptionalVal(timerID)})
}

// GetByKey gets a timer by key. If the timer with the specified key not exists in the namespace,
// an error `ErrTimerNotExist` will be returned.
func (s *TimerStore) GetByKey(ctx context.Context, namespace, key string) (record *TimerRecord, err error) {
	return s.getOneRecord(ctx, &TimerCond{Namespace: NewOptionalVal(namespace), Key: NewOptionalVal(key)})
}

func (s *TimerStore) getOneRecord(ctx context.Context, cond Cond) (record *TimerRecord, err error) {
	records, err := s.List(ctx, cond)
	if err != nil {
		return nil, err
	}

	if len(records) == 0 {
		return nil, errors.Trace(ErrTimerNotExist)
	}

	return records[0], nil
}

// TimerWatchEventNotifier is used to notify timer watch events.
type TimerWatchEventNotifier interface {
	// Watch watches all changes of the store. A chan will be returned to receive `WatchTimerResponse`
	// The returned chan be closed when the context in the argument is done.
	Watch(ctx context.Context) WatchTimerChan
	// Notify sends the event to watchers.
	Notify(tp WatchTimerEventType, timerID string)
	// Close closes the notifier.
	Close()
}
