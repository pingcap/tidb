// Copyright 2016 Michal Witkowski. All Rights Reserved.
// See LICENSE for licensing terms.

package grpc_retry

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var (
	// DefaultRetriableCodes is a set of well known types gRPC codes that should be retri-able.
	//
	// `ResourceExhausted` means that the user quota, e.g. per-RPC limits, have been reached.
	// `Unavailable` means that system is currently unavailable and the client should retry again.
	DefaultRetriableCodes = []codes.Code{codes.ResourceExhausted, codes.Unavailable}

	defaultOptions = &options{
		max:            0, // disabed
		perCallTimeout: 0, // disabled
		includeHeader:  true,
		codes:          DefaultRetriableCodes,
		backoffFunc:    BackoffLinearWithJitter(50*time.Millisecond /*jitter*/, 0.10),
	}
)

// BackoffFunc denotes a family of functions that controll the backoff duration between call retries.
//
// They are called with an identifier of the attempt, and should return a time the system client should
// hold off for. If the time returned is longer than the `context.Context.Deadline` of the request
// the deadline of the request takes precedence and the wait will be interrupted before proceeding
// with the next iteration.
type BackoffFunc func(attempt uint) time.Duration

// Disable disables the retry behaviour on this call, or this interceptor.
//
// Its semantically the same to `WithMax`
func Disable() CallOption {
	return WithMax(0)
}

// WithMax sets the maximum number of retries on this call, or this interceptor.
func WithMax(maxRetries uint) CallOption {
	return CallOption{applyFunc: func(o *options) {
		o.max = maxRetries
	}}
}

// WithBackoff sets the `BackoffFunc `used to control time between retries.
func WithBackoff(bf BackoffFunc) CallOption {
	return CallOption{applyFunc: func(o *options) {
		o.backoffFunc = bf
	}}
}

// WithCodes sets which codes should be retried.
//
// Please *use with care*, as you may be retrying non-idempotend calls.
//
// You cannot automatically retry on Cancelled and Deadline, please use `WithPerRetryTimeout` for these.
func WithCodes(retryCodes ...codes.Code) CallOption {
	return CallOption{applyFunc: func(o *options) {
		o.codes = retryCodes
	}}
}

// WithPerRetryTimeout sets the RPC timeout per call (including initial call) on this call, or this interceptor.
//
// The context.Deadline of the call takes precedence and sets the maximum time the whole invocation
// will take, but WithPerRetryTimeout can be used to limit the RPC time per each call.
//
// For example, with context.Deadline = now + 10s, and WithPerRetryTimeout(3 * time.Seconds), each
// of the retry calls (including the initial one) will have a deadline of now + 3s.
//
// A value of 0 disables the timeout overrides completely and returns to each retry call using the
// parent `context.Deadline`.
func WithPerRetryTimeout(timeout time.Duration) CallOption {
	return CallOption{applyFunc: func(o *options) {
		o.perCallTimeout = timeout
	}}
}

type options struct {
	max            uint
	perCallTimeout time.Duration
	includeHeader  bool
	codes          []codes.Code
	backoffFunc    BackoffFunc
}

// callOption is a grpc.CallOption that is local to grpc_retry.
type CallOption struct {
	grpc.EmptyCallOption // make sure we implement private after() and before() fields so we don't panic.
	applyFunc       func(opt *options)
}

func reuseOrNewWithCallOptions(opt *options, callOptions []CallOption) *options {
	if len(callOptions) == 0 {
		return opt
	}
	optCopy := &options{}
	*optCopy = *opt
	for _, f := range callOptions {
		f.applyFunc(optCopy)
	}
	return optCopy
}

func filterCallOptions(callOptions []grpc.CallOption) (grpcOptions []grpc.CallOption, retryOptions []CallOption) {
	for _, opt := range callOptions {
		if co, ok := opt.(CallOption); ok {
			retryOptions = append(retryOptions, co)
		} else {
			grpcOptions = append(grpcOptions, opt)
		}
	}
	return grpcOptions, retryOptions
}
