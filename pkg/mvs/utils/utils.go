package utils

import "sync/atomic"

// Notifer works as the multiple producer & single consumer mode.
type Notifer struct {
	C     chan struct{}
	awake int32
}

// NewNotifer creates a new Notifer instance.
func NewNotifer() Notifer {
	return Notifer{
		C: make(chan struct{}, 1),
	}
}

// return previous awake status
func (n *Notifer) clear() bool {
	return atomic.SwapInt32(&n.awake, 0) != 0
}

// Wait for signal synchronously (consumer)
func (n *Notifer) Wait() {
	<-n.C
	n.clear()
}

// Wake the consumer
func (n *Notifer) Wake() {
	n.wake()
}

func (n *Notifer) wake() {
	// 1 -> 1: do nothing
	// 0 -> 1: send signal
	if atomic.SwapInt32(&n.awake, 1) == 0 {
		n.C <- struct{}{}
	}
}

func (n *Notifer) isAwake() bool {
	return atomic.LoadInt32(&n.awake) != 0
}

// WeakWake wakes the consumer if it is not awake (may loose signal under concurrent scenarios).
func (n *Notifer) WeakWake() {
	if n.isAwake() {
		return
	}
	n.wake()
}
