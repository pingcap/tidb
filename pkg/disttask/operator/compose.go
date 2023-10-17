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

package operator

// WithSource is an interface that can be used to set the source of an operator.
type WithSource[T any] interface {
	SetSource(channel DataChannel[T])
}

// WithSink is an interface that can be used to set the sink of an operator.
type WithSink[T any] interface {
	// SetSink sets the sink of the operator.
	// Operator implementations should call the Finish method of the sink when they are done.
	SetSink(channel DataChannel[T])
}

// Compose sets the sink of op1 and the source of op2.
func Compose[T any](op1 WithSink[T], op2 WithSource[T]) {
	ch := NewSimpleDataChannel(make(chan T))
	op1.SetSink(ch)
	op2.SetSource(ch)
}

// DataChannel is a channel that can be used to transfer data between operators.
type DataChannel[T any] interface {
	Channel() chan T
	Finish()
}

// SimpleDataChannel is a simple implementation of DataChannel.
type SimpleDataChannel[T any] struct {
	channel chan T
}

// NewSimpleDataChannel creates a new SimpleDataChannel.
func NewSimpleDataChannel[T any](ch chan T) *SimpleDataChannel[T] {
	return &SimpleDataChannel[T]{channel: ch}
}

// Channel returns the underlying channel of the SimpleDataChannel.
func (s *SimpleDataChannel[T]) Channel() chan T {
	return s.channel
}

// Finish closes the underlying channel of the SimpleDataChannel.
func (s *SimpleDataChannel[T]) Finish() {
	close(s.channel)
}
