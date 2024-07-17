package utils

import (
	"fmt"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"strings"
	"sync"
)

// TODO: remove and mapped all to an error type
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
// TODO: remove and mapped all to an error type
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
	// Retry error can be retried but will consume the backoff attempt quota.
	Retry ErrorHandlingStrategy = iota
	// GiveUp means unrecoverable error happened and the BR should exit
	// for example:
	// 1. permission not valid.
	// 2. data not found.
	// 3. retry too many times
	GiveUp
	// Unknown for Unknown error
	Unknown
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

func HandleBackupError(err *backuppb.Error, storeId uint64, ec *ErrorContext) ErrorHandlingResult {
	if err == nil {
		return ErrorHandlingResult{Retry, unreachableRetryMsg}
	}
	res := handleBackupProtoError(err)
	// try the best effort handle unknown error based on their error message
	if res.Strategy == Unknown && len(err.Msg) != 0 {
		return HandleUnknownBackupError(err.Msg, storeId, ec)
	}
	return res
}

func handleBackupProtoError(e *backuppb.Error) ErrorHandlingResult {
	switch e.Detail.(type) {
	case *backuppb.Error_KvError:
		return ErrorHandlingResult{Retry, retryOnKvErrorMsg}
	case *backuppb.Error_RegionError:
		return ErrorHandlingResult{Retry, retryOnRegionErrorMsg}
	case *backuppb.Error_ClusterIdError:
		return ErrorHandlingResult{GiveUp, clusterIdMismatchMsg}
	}
	return ErrorHandlingResult{Unknown, unknownErrorMsg}
}

// HandleUnknownBackupError TODO: remove this method and map all the current unknown errors to an error type
func HandleUnknownBackupError(msg string, uuid uint64, ec *ErrorContext) ErrorHandlingResult {
	// UNSAFE! TODO: use meaningful error code instead of unstructured message to find failed to write error.
	logger := log.L().With(zap.String("description", ec.description))
	if messageIsNotFoundStorageError(msg) {
		reason := fmt.Sprintf("File or directory not found on TiKV Node (store id: %v). "+
			"workaround: please ensure br and tikv nodes share a same storage and the user of br and tikv has same uid.",
			uuid)
		return ErrorHandlingResult{GiveUp, reason}
	}
	if messageIsPermissionDeniedStorageError(msg) {
		reason := fmt.Sprintf("I/O permission denied error occurs on TiKV Node(store id: %v). "+
			"workaround: please ensure tikv has permission to read from & write to the storage.",
			uuid)
		return ErrorHandlingResult{GiveUp, reason}
	}
	msgLower := strings.ToLower(msg)
	if strings.Contains(msgLower, contextCancelledMsg) {
		return ErrorHandlingResult{GiveUp, contextCancelledMsg}
	}

	if MessageIsRetryableStorageError(msg) {
		logger.Warn(retryableStorageErrorMsg, zap.String("error", msg))
		return ErrorHandlingResult{Retry, retryableStorageErrorMsg}
	}

	// retry enough on same store
	ec.mu.Lock()
	defer ec.mu.Unlock()
	ec.encounterTimes[uuid]++
	if ec.encounterTimes[uuid] <= ec.encounterTimesLimitation {
		return ErrorHandlingResult{Retry, retryOnUnknownErrorMsg}
	}
	return ErrorHandlingResult{GiveUp, noRetryOnUnknownErrorMsg}
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
