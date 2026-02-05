package utils

import "sync/atomic"

// Notifier works as the multiple producer & single consumer mode.
type Notifier struct {
	C     chan struct{}
	awake int32
}

// NewNotifier creates a new Notifier instance.
func NewNotifier() Notifier {
	return Notifier{
		C: make(chan struct{}, 1),
	}
}

// return previous awake status
func (n *Notifier) clear() bool {
	return atomic.SwapInt32(&n.awake, 0) != 0
}

// Wait for signal synchronously (consumer)
func (n *Notifier) Wait() {
	<-n.C
	n.clear()
}

// Wake the consumer
func (n *Notifier) Wake() {
	n.wake()
}

func (n *Notifier) wake() {
	// 1 -> 1: do nothing
	// 0 -> 1: send signal
	if atomic.SwapInt32(&n.awake, 1) == 0 {
		n.C <- struct{}{}
	}
}

func (n *Notifier) isAwake() bool {
	return atomic.LoadInt32(&n.awake) != 0
}

// WeakWake wakes the consumer if it is not awake (may loose signal under concurrent scenarios).
func (n *Notifier) WeakWake() {
	if n.isAwake() {
		return
	}
	n.wake()
}
