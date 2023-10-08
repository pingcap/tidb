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
	"encoding/hex"
	"strings"
	"time"
	"unsafe"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	clientMaxRetry     = 5
	clientRetryBackoff = uint64(1000)
)

// GetTimerOption is the option to get timers.
type GetTimerOption func(*TimerCond)

// WithKey indicates to get a timer with the specified key.
func WithKey(key string) GetTimerOption {
	return func(cond *TimerCond) {
		cond.Key.Set(key)
		cond.KeyPrefix = false
	}
}

// WithKeyPrefix to get timers with the indicated key prefix.
func WithKeyPrefix(keyPrefix string) GetTimerOption {
	return func(cond *TimerCond) {
		cond.Key.Set(keyPrefix)
		cond.KeyPrefix = true
	}
}

// WithID indicates to get a timer with the specified id.
func WithID(id string) GetTimerOption {
	return func(cond *TimerCond) {
		cond.ID.Set(id)
	}
}

// WithTag indicates to get a timer with the specified tags.
func WithTag(tags ...string) GetTimerOption {
	return func(cond *TimerCond) {
		cond.Tags.Set(tags)
	}
}

// UpdateTimerOption is the option to update the timer.
type UpdateTimerOption func(*TimerUpdate)

// WithSetEnable indicates to set the timer's `Enable` field.
func WithSetEnable(enable bool) UpdateTimerOption {
	return func(update *TimerUpdate) {
		update.Enable.Set(enable)
	}
}

// WithSetTimeZone sets the timezone of the timer
func WithSetTimeZone(name string) UpdateTimerOption {
	return func(update *TimerUpdate) {
		update.TimeZone.Set(name)
	}
}

// WithSetSchedExpr indicates to set the timer's schedule policy.
func WithSetSchedExpr(tp SchedPolicyType, expr string) UpdateTimerOption {
	return func(update *TimerUpdate) {
		update.SchedPolicyType.Set(tp)
		update.SchedPolicyExpr.Set(expr)
	}
}

// WithSetWatermark indicates to set the timer's watermark.
func WithSetWatermark(watermark time.Time) UpdateTimerOption {
	return func(update *TimerUpdate) {
		update.Watermark.Set(watermark)
	}
}

// WithSetSummaryData indicates to set the timer's summary.
func WithSetSummaryData(summary []byte) UpdateTimerOption {
	return func(update *TimerUpdate) {
		update.SummaryData.Set(summary)
	}
}

// WithSetTags indicates to set the timer's tags.
func WithSetTags(tags []string) UpdateTimerOption {
	return func(update *TimerUpdate) {
		update.Tags.Set(tags)
	}
}

// TimerClient is an interface exposed to user to manage timers.
type TimerClient interface {
	// GetDefaultNamespace returns the default namespace of this client.
	GetDefaultNamespace() string
	// CreateTimer creates a new timer.
	CreateTimer(ctx context.Context, spec TimerSpec) (*TimerRecord, error)
	// GetTimerByID queries the timer by ID.
	GetTimerByID(ctx context.Context, timerID string) (*TimerRecord, error)
	// GetTimerByKey queries the timer by key.
	GetTimerByKey(ctx context.Context, key string) (*TimerRecord, error)
	// GetTimers queries timers by options.
	GetTimers(ctx context.Context, opts ...GetTimerOption) ([]*TimerRecord, error)
	// UpdateTimer updates a timer.
	UpdateTimer(ctx context.Context, timerID string, opts ...UpdateTimerOption) error
	// ManualTriggerEvent triggers event manually.
	ManualTriggerEvent(ctx context.Context, timerID string) (string, error)
	// CloseTimerEvent closes the triggering event of a timer.
	CloseTimerEvent(ctx context.Context, timerID string, eventID string, opts ...UpdateTimerOption) error
	// DeleteTimer deletes a timer.
	DeleteTimer(ctx context.Context, timerID string) (bool, error)
}

// DefaultStoreNamespace is the default namespace.
const DefaultStoreNamespace = "default"

// defaultTimerClient is the default implement of timer client.
type defaultTimerClient struct {
	namespace    string
	store        *TimerStore
	retryBackoff uint64
}

// NewDefaultTimerClient creates a new defaultTimerClient.
func NewDefaultTimerClient(store *TimerStore) TimerClient {
	return &defaultTimerClient{
		namespace:    DefaultStoreNamespace,
		store:        store,
		retryBackoff: clientRetryBackoff,
	}
}

func (c *defaultTimerClient) GetDefaultNamespace() string {
	return c.namespace
}

func (c *defaultTimerClient) CreateTimer(ctx context.Context, spec TimerSpec) (*TimerRecord, error) {
	if spec.Namespace == "" {
		spec.Namespace = c.namespace
	}

	timerID, err := c.store.Create(ctx, &TimerRecord{
		TimerSpec: spec,
	})

	if err != nil {
		return nil, err
	}
	return c.store.GetByID(ctx, timerID)
}

func (c *defaultTimerClient) GetTimerByID(ctx context.Context, timerID string) (*TimerRecord, error) {
	return c.store.GetByID(ctx, timerID)
}

func (c *defaultTimerClient) GetTimerByKey(ctx context.Context, key string) (*TimerRecord, error) {
	return c.store.GetByKey(ctx, c.namespace, key)
}

func (c *defaultTimerClient) GetTimers(ctx context.Context, opts ...GetTimerOption) ([]*TimerRecord, error) {
	cond := &TimerCond{}
	for _, opt := range opts {
		opt(cond)
	}
	return c.store.List(ctx, cond)
}

func (c *defaultTimerClient) UpdateTimer(ctx context.Context, timerID string, opts ...UpdateTimerOption) error {
	update := &TimerUpdate{}
	for _, opt := range opts {
		opt(update)
	}
	return c.store.Update(ctx, timerID, update)
}

func (c *defaultTimerClient) ManualTriggerEvent(ctx context.Context, timerID string) (string, error) {
	reqUUID := uuid.New()
	requestID := hex.EncodeToString(reqUUID[:])

	err := util.RunWithRetry(clientMaxRetry, c.retryBackoff, func() (bool, error) {
		timer, err := c.store.GetByID(ctx, timerID)
		if err != nil {
			return false, err
		}

		if timer.EventID != "" {
			return false, errors.New("manual trigger is not allowed when event is not closed")
		}

		if !timer.Enable {
			return false, errors.New("manual trigger is not allowed when timer is disabled")
		}

		err = c.store.Update(ctx, timerID, &TimerUpdate{
			ManualRequest: NewOptionalVal(ManualRequest{
				ManualRequestID:   requestID,
				ManualRequestTime: time.Now(),
				ManualTimeout:     2 * time.Minute,
			}),
			CheckVersion: NewOptionalVal(timer.Version),
		})

		if errors.ErrorEqual(ErrVersionNotMatch, err) {
			logutil.BgLogger().Warn("failed to update timer for version not match, retry", zap.String("timerID", timerID))
			return true, err
		}

		return false, err
	})

	if err != nil {
		return "", err
	}

	return requestID, nil
}

func (c *defaultTimerClient) CloseTimerEvent(ctx context.Context, timerID string, eventID string, opts ...UpdateTimerOption) error {
	update := &TimerUpdate{}
	for _, opt := range opts {
		opt(update)
	}

	fields := update.FieldsSet(unsafe.Pointer(&update.Watermark), unsafe.Pointer(&update.SummaryData))
	if len(fields) > 0 {
		return errors.Errorf("The field(s) [%s] are not allowed to update when close event", strings.Join(fields, ", "))
	}

	timer, err := c.GetTimerByID(ctx, timerID)
	if err != nil {
		return err
	}

	var zeroTime time.Time
	update.CheckEventID.Set(eventID)
	update.EventStatus.Set(SchedEventIdle)
	update.EventID.Set("")
	update.EventData.Set(nil)
	update.EventStart.Set(zeroTime)
	if !update.Watermark.Present() {
		update.Watermark.Set(timer.EventStart)
	}
	update.EventExtra.Set(EventExtra{})
	return c.store.Update(ctx, timerID, update)
}

func (c *defaultTimerClient) DeleteTimer(ctx context.Context, timerID string) (bool, error) {
	return c.store.Delete(ctx, timerID)
}
