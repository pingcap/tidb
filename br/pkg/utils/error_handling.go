// Copyright 2024 PingCAP, Inc.
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

package utils

import (
	"fmt"
	"strings"
	"sync"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// UNSAFE! TODO: remove and map them to error types
var retryableErrorMsg = []string{
	"server closed",
	"connection refused",
	"connection reset by peer",
	"channel closed",
	"error trying to connect",
	"connection closed before message completed",
	"body write aborted",
	"error during dispatch",
	"put object timeout",
	"timeout after",
	"internalerror",
	"not read from or written to within the timeout period",
	"<code>requesttimeout</code>",
	"<code>invalidpart</code>",
	"end of file before message length reached",
}

// non-retryable error messages
// UNSAFE! TODO: remove and map them to error types
const (
	ioMsg               = "io"
	notFoundMsg         = "notfound"
	permissionDeniedMsg = "permissiondenied"
)

// error messages
const (
	unreachableRetryMsg      = "unreachable retry"
	retryOnKvErrorMsg        = "retry on kv error"
	retryOnRegionErrorMsg    = "retry on region error"
	clusterIdMismatchMsg     = "cluster id mismatch"
	unknownErrorMsg          = "unknown error"
	contextCancelledMsg      = "context canceled"
	retryOnUnknownErrorMsg   = "unknown error, retry it for a few times"
	noRetryOnUnknownErrorMsg = "unknown error, retried too many times, give up"
	retryableStorageErrorMsg = "retryable storage error"
)

type ErrorHandlingResult struct {
	Strategy ErrorHandlingStrategy
	Reason   string
}

type ErrorHandlingStrategy int

const (
	// StrategyRetry error can be retried but will consume the backoff attempt quota.
	StrategyRetry ErrorHandlingStrategy = iota
	// StrategyGiveUp means unrecoverable error happened and the BR should exit
	// for example:
	// 1. permission not valid.
	// 2. data not found.
	// 3. retry too many times
	StrategyGiveUp
	// StrategyUnknown for StrategyUnknown error
	StrategyUnknown
)

type ErrorContext struct {
	mu sync.Mutex
	// encounter times for one context on a store
	// we may use this value to determine the retry policy
	encounterTimes map[uint64]int
	// unknown error retry limitation.
	// encounter many times error makes Retry to GiveUp.
	encounterTimesLimitation int
	description              string
}

func NewErrorContext(scenario string, limitation int) *ErrorContext {
	return &ErrorContext{
		description:              scenario,
		encounterTimes:           make(map[uint64]int),
		encounterTimesLimitation: limitation,
	}
}

func NewDefaultContext() *ErrorContext {
	return &ErrorContext{
		description:              "default",
		encounterTimes:           make(map[uint64]int),
		encounterTimesLimitation: 1,
	}
}

func NewZeroRetryContext(scenario string) *ErrorContext {
	return &ErrorContext{
		description:              scenario,
		encounterTimes:           make(map[uint64]int),
		encounterTimesLimitation: 0,
	}
}

func HandleBackupError(err *backuppb.Error, storeId uint64, ec *ErrorContext) ErrorHandlingResult {
	if err == nil {
		return ErrorHandlingResult{StrategyRetry, unreachableRetryMsg}
	}
	res := handleBackupProtoError(err)
	// try the best effort handle unknown error based on their error message
	if res.Strategy == StrategyUnknown && len(err.Msg) != 0 {
		return HandleUnknownBackupError(err.Msg, storeId, ec)
	}
	return res
}

func handleBackupProtoError(e *backuppb.Error) ErrorHandlingResult {
	switch e.Detail.(type) {
	case *backuppb.Error_KvError:
		return ErrorHandlingResult{StrategyRetry, retryOnKvErrorMsg}
	case *backuppb.Error_RegionError:
		return ErrorHandlingResult{StrategyRetry, retryOnRegionErrorMsg}
	case *backuppb.Error_ClusterIdError:
		return ErrorHandlingResult{StrategyGiveUp, clusterIdMismatchMsg}
	}
	return ErrorHandlingResult{StrategyUnknown, unknownErrorMsg}
}

// HandleUnknownBackupError UNSAFE! TODO: remove this method and map all the current unknown errors to error types
func HandleUnknownBackupError(msg string, uuid uint64, ec *ErrorContext) ErrorHandlingResult {
	// UNSAFE! TODO: use meaningful error code instead of unstructured message to find failed to write error.
	logger := log.L().With(zap.String("description", ec.description))
	if messageIsNotFoundStorageError(msg) {
		reason := fmt.Sprintf("File or directory not found on TiKV Node (store id: %v). "+
			"workaround: please ensure br and tikv nodes share a same storage and the user of br and tikv has same uid.",
			uuid)
		return ErrorHandlingResult{StrategyGiveUp, reason}
	}
	if messageIsPermissionDeniedStorageError(msg) {
		reason := fmt.Sprintf("I/O permission denied error occurs on TiKV Node(store id: %v). "+
			"workaround: please ensure tikv has permission to read from & write to the storage.",
			uuid)
		return ErrorHandlingResult{StrategyGiveUp, reason}
	}
	msgLower := strings.ToLower(msg)
	if strings.Contains(msgLower, contextCancelledMsg) {
		return ErrorHandlingResult{StrategyGiveUp, contextCancelledMsg}
	}

	if MessageIsRetryableStorageError(msg) {
		logger.Warn(retryableStorageErrorMsg, zap.String("error", msg))
		return ErrorHandlingResult{StrategyRetry, retryableStorageErrorMsg}
	}

	// retry enough on same store
	ec.mu.Lock()
	defer ec.mu.Unlock()
	ec.encounterTimes[uuid]++
	if ec.encounterTimes[uuid] <= ec.encounterTimesLimitation {
		return ErrorHandlingResult{StrategyRetry, retryOnUnknownErrorMsg}
	}
	return ErrorHandlingResult{StrategyGiveUp, noRetryOnUnknownErrorMsg}
}

// messageIsNotFoundStorageError checks whether the message returning from TiKV is "NotFound" storage I/O error
func messageIsNotFoundStorageError(msg string) bool {
	msgLower := strings.ToLower(msg)
	return strings.Contains(msgLower, ioMsg) && strings.Contains(msgLower, notFoundMsg)
}

// MessageIsPermissionDeniedStorageError checks whether the message returning from TiKV is "PermissionDenied" storage I/O error
func messageIsPermissionDeniedStorageError(msg string) bool {
	msgLower := strings.ToLower(msg)
	return strings.Contains(msgLower, permissionDeniedMsg)
}

// MessageIsRetryableStorageError checks whether the message returning from TiKV is retryable ExternalStorageError.
func MessageIsRetryableStorageError(msg string) bool {
	msgLower := strings.ToLower(msg)
	// UNSAFE! TODO: Add a error type for retryable connection error.
	for _, errStr := range retryableErrorMsg {
		if strings.Contains(msgLower, errStr) {
			return true
		}
	}
	return false
}
