// Copyright 2026 PingCAP, Inc.
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

import "time"

// Exported types and functions for testing

// ExportedTimerCond exports TimerCond for testing
type ExportedTimerCond = TimerCond

// ExportedTimerUpdate exports TimerUpdate for testing
type ExportedTimerUpdate = TimerUpdate

// ExportedTimerStore exports TimerStore for testing
type ExportedTimerStore = TimerStore

// ExportedWithKey exports WithKey for testing
func ExportedWithKey(key string) GetTimerOption {
	return WithKey(key)
}

// ExportedWithKeyPrefix exports WithKeyPrefix for testing
func ExportedWithKeyPrefix(keyPrefix string) GetTimerOption {
	return WithKeyPrefix(keyPrefix)
}

// ExportedWithID exports WithID for testing
func ExportedWithID(id string) GetTimerOption {
	return WithID(id)
}

// ExportedWithTag exports WithTag for testing
func ExportedWithTag(tags ...string) GetTimerOption {
	return WithTag(tags...)
}

// ExportedWithSetEnable exports WithSetEnable for testing
func ExportedWithSetEnable(enable bool) UpdateTimerOption {
	return WithSetEnable(enable)
}

// ExportedWithSetTimeZone exports WithSetTimeZone for testing
func ExportedWithSetTimeZone(name string) UpdateTimerOption {
	return WithSetTimeZone(name)
}

// ExportedWithSetSchedExpr exports WithSetSchedExpr for testing
func ExportedWithSetSchedExpr(tp SchedPolicyType, expr string) UpdateTimerOption {
	return WithSetSchedExpr(tp, expr)
}

// ExportedWithSetWatermark exports WithSetWatermark for testing
func ExportedWithSetWatermark(watermark time.Time) UpdateTimerOption {
	return WithSetWatermark(watermark)
}

// ExportedWithSetSummaryData exports WithSetSummaryData for testing
func ExportedWithSetSummaryData(summary []byte) UpdateTimerOption {
	return WithSetSummaryData(summary)
}

// ExportedWithSetTags exports WithSetTags for testing
func ExportedWithSetTags(tags []string) UpdateTimerOption {
	return WithSetTags(tags)
}

// ExportedSchedEventInterval exports SchedEventInterval for testing
const ExportedSchedEventInterval = SchedEventInterval

// ExportedSchedEventCron exports SchedEventCron for testing
const ExportedSchedEventCron = SchedEventCron

// ExportedSchedEventIdle exports SchedEventIdle for testing
const ExportedSchedEventIdle = SchedEventIdle

// ExportedSchedEventTrigger exports SchedEventTrigger for testing
const ExportedSchedEventTrigger = SchedEventTrigger

// ExportedNewMemoryTimerStore exports NewMemoryTimerStore for testing
func ExportedNewMemoryTimerStore() *TimerStore {
	return NewMemoryTimerStore()
}

// ExportedNewDefaultTimerClient exports NewDefaultTimerClient for testing
func ExportedNewDefaultTimerClient(store *TimerStore) TimerClient {
	return NewDefaultTimerClient(store)
}

// ExportedTimerSpec exports TimerSpec for testing
type ExportedTimerSpec = TimerSpec

// ExportedTimerRecord exports TimerRecord for testing
type ExportedTimerRecord = TimerRecord

// ExportedNewOptionalVal exports NewOptionalVal for testing
func ExportedNewOptionalVal[T any](v T) OptionalVal[T] {
	return NewOptionalVal(v)
}

// ExportedManualRequest exports ManualRequest for testing
type ExportedManualRequest = ManualRequest

// ExportedEventExtra exports EventExtra for testing
type ExportedEventExtra = EventExtra

// ExportedErrEventIDNotMatch exports ErrEventIDNotMatch for testing
var ExportedErrEventIDNotMatch = ErrEventIDNotMatch

// ExportedOptionalVal exports OptionalVal for testing
type ExportedOptionalVal[T any] = OptionalVal[T]

// ExportedCreateSchedEventPolicy exports CreateSchedEventPolicy for testing
func ExportedCreateSchedEventPolicy(tp SchedPolicyType, expr string) (SchedEventPolicy, error) {
	return CreateSchedEventPolicy(tp, expr)
}

// ExportedSchedIntervalPolicy exports SchedIntervalPolicy for testing
type ExportedSchedIntervalPolicy = SchedIntervalPolicy

// ExportedCronPolicy exports CronPolicy for testing
type ExportedCronPolicy = CronPolicy

// ExportedSchedEventPolicy exports SchedEventPolicy for testing
type ExportedSchedEventPolicy = SchedEventPolicy

// ExportedAnd exports And for testing
func ExportedAnd(children ...Cond) *Operator {
	return And(children...)
}

// ExportedOr exports Or for testing
func ExportedOr(children ...Cond) *Operator {
	return Or(children...)
}

// ExportedNot exports Not for testing
func ExportedNot(cond Cond) *Operator {
	return Not(cond)
}

// ExportedOperator exports Operator for testing
type ExportedOperator = Operator

// ExportedCond exports Cond for testing
type ExportedCond = Cond

// ExportedErrVersionNotMatch exports ErrVersionNotMatch for testing
var ExportedErrVersionNotMatch = ErrVersionNotMatch

// ExportedApply exports the apply method for testing
func ExportedApply(u *TimerUpdate, record *TimerRecord) (*TimerRecord, error) {
	return u.apply(record)
}
