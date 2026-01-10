package driver

import (
	"errors"
	"sync"
)

var errStorePoolClosed = errors.New("store pool is closed")

type storePool struct {
	sem       chan struct{}
	closeCh   chan struct{}
	closeOnce sync.Once
}

func newStorePool(concurrency int) *storePool {
	if concurrency < 1 {
		concurrency = 1
	}
	return &storePool{
		sem:     make(chan struct{}, concurrency),
		closeCh: make(chan struct{}),
	}
}

func (p *storePool) Run(fn func()) error {
	if fn == nil {
		return nil
	}
	select {
	case <-p.closeCh:
		return errStorePoolClosed
	default:
	}

	select {
	case p.sem <- struct{}{}:
	case <-p.closeCh:
		return errStorePoolClosed
	}

	select {
	case <-p.closeCh:
		<-p.sem
		return errStorePoolClosed
	default:
	}

	go func() {
		defer func() { <-p.sem }()
		fn()
	}()
	return nil
}

func (p *storePool) Close() {
	p.closeOnce.Do(func() {
		close(p.closeCh)
	})
}
