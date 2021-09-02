// Copyright 2021 PingCAP, Inc.
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

package backoff

import (
	"context"

	"github.com/pingcap/tidb/kv"
	derr "github.com/pingcap/tidb/store/driver/error"
	"github.com/tikv/client-go/v2/tikv"
)

// Backoffer wraps tikv.Backoffer and converts the error which returns by the functions of tikv.Backoffer to tidb error.
type Backoffer struct {
	b *tikv.Backoffer
}

// NewBackofferWithVars creates a Backoffer with maximum sleep time(in ms) and kv.Variables.
func NewBackofferWithVars(ctx context.Context, maxSleep int, vars *kv.Variables) *Backoffer {
	b := tikv.NewBackofferWithVars(ctx, maxSleep, vars)
	return &Backoffer{b: b}
}

// NewBackoffer creates a Backoffer with maximum sleep time(in ms).
func NewBackoffer(ctx context.Context, maxSleep int) *Backoffer {
	b := tikv.NewBackoffer(ctx, maxSleep)
	return &Backoffer{b: b}
}

// TiKVBackoffer returns tikv.Backoffer.
func (b *Backoffer) TiKVBackoffer() *tikv.Backoffer {
	return b.b
}

// Backoff sleeps a while base on the BackoffConfig and records the error message.
// It returns a retryable error if total sleep time exceeds maxSleep.
func (b *Backoffer) Backoff(cfg *tikv.BackoffConfig, err error) error {
	e := b.b.Backoff(cfg, err)
	return derr.ToTiDBErr(e)
}

// BackoffWithMaxSleepTxnLockFast sleeps a while for the operation TxnLockFast and records the error message
// and never sleep more than maxSleepMs for each sleep.
func (b *Backoffer) BackoffWithMaxSleepTxnLockFast(maxSleepMs int, err error) error {
	e := b.b.BackoffWithMaxSleepTxnLockFast(maxSleepMs, err)
	return derr.ToTiDBErr(e)
}

// GetBackoffTimes returns a map contains backoff time count by type.
func (b *Backoffer) GetBackoffTimes() map[string]int {
	return b.b.GetBackoffTimes()
}

// GetCtx returns the binded context.
func (b *Backoffer) GetCtx() context.Context {
	return b.b.GetCtx()
}

// GetVars returns the binded vars.
func (b *Backoffer) GetVars() *tikv.Variables {
	return b.b.GetVars()
}

// GetBackoffSleepMS returns a map contains backoff sleep time by type.
func (b *Backoffer) GetBackoffSleepMS() map[string]int {
	return b.b.GetBackoffSleepMS()
}

// GetTotalSleep returns total sleep time.
func (b *Backoffer) GetTotalSleep() int {
	return b.b.GetTotalSleep()
}
