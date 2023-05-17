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
	"reflect"
	"strings"
	"time"
	"unsafe"
)

type optionalVal interface {
	optionalVal()
	Present() bool
	Clear()
}

// OptionalVal is used by `TimerCond` and `TimerUpdate` to indicate
// whether some field's condition has been set or whether is field should be update
type OptionalVal[T any] struct {
	v       T
	present bool
}

// NewOptionalVal creates a new OptionalVal
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

// Set sets the value
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

// TimerCond is the condition to filter a timer record
type TimerCond struct {
	// ID indicates to filter the timer record with ID
	ID OptionalVal[string]
	// Namespace indicates to filter the timer by Namespace
	Namespace OptionalVal[string]
	// Key indicates to filter the timer record with ID
	// The filter behavior is defined by `KeyPrefix`
	Key OptionalVal[string]
	// KeyPrefix indicates how to filter with timer's key if `Key` is set
	// If `KeyPrefix is` true, it will check whether the timer's key is prefixed with `TimerCond.Key`.
	// Otherwise, it will check whether the timer's key equals `TimerCond.Key`.
	KeyPrefix bool
}

// Match will return whether the condition match the timer record
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

	return true
}

// FieldsSet returns all fields that has been set exclude excludes
func (c *TimerCond) FieldsSet(excludes ...unsafe.Pointer) (fields []string) {
	iterOptionalFields(reflect.ValueOf(c).Elem(), excludes, func(name string, val optionalVal) bool {
		if val.Present() {
			fields = append(fields, name)
		}
		return true
	})
	return
}

// Clear clears all fields
func (c *TimerCond) Clear() {
	iterOptionalFields(reflect.ValueOf(c).Elem(), nil, func(name string, val optionalVal) bool {
		val.Clear()
		return true
	})
	c.KeyPrefix = false
}

// TimerUpdate indicates how to update a timer
type TimerUpdate struct {
	// Enable indicates to set the timer's `Enable` field
	Enable OptionalVal[bool]
	// SchedPolicyType indicates to set the timer's `SchedPolicyType` field
	SchedPolicyType OptionalVal[SchedPolicyType]
	// SchedPolicyExpr indicates to set the timer's `SchedPolicyExpr` field
	SchedPolicyExpr OptionalVal[string]
	// Watermark indicates to set the timer's `Watermark` field
	Watermark OptionalVal[time.Time]
	// SummaryData indicates to set the timer's `Summary` field
	SummaryData OptionalVal[[]byte]
}

// Apply applies the update to a timer
func (u *TimerUpdate) Apply(record *TimerRecord) error {
	if v, ok := u.Enable.Get(); ok {
		record.Enable = v
	}

	if v, ok := u.SchedPolicyType.Get(); ok {
		record.SchedPolicyType = v
	}

	if v, ok := u.SchedPolicyExpr.Get(); ok {
		record.SchedPolicyExpr = v
	}

	if v, ok := u.Watermark.Get(); ok {
		record.Watermark = v
	}

	if v, ok := u.SummaryData.Get(); ok {
		record.SummaryData = v
	}

	return nil
}

// FieldsSet returns all fields that has been set exclude excludes
func (u *TimerUpdate) FieldsSet(excludes ...unsafe.Pointer) (fields []string) {
	iterOptionalFields(reflect.ValueOf(u).Elem(), excludes, func(name string, val optionalVal) bool {
		if val.Present() {
			fields = append(fields, name)
		}
		return true
	})
	return
}

// Clear clears all fields
func (u *TimerUpdate) Clear() {
	iterOptionalFields(reflect.ValueOf(u).Elem(), nil, func(name string, val optionalVal) bool {
		val.Clear()
		return true
	})
}
