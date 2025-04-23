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

package duplicate

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/util/extsort"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Detector is used to detect duplicate keys.
type Detector struct {
	sorter extsort.ExternalSorter
	logger log.Logger
}

// NewDetector creates a new Detector.
func NewDetector(sorter extsort.ExternalSorter, logger log.Logger) *Detector {
	logger = logger.With(zap.String("component", "duplicate.Detector"))
	return &Detector{sorter: sorter, logger: logger}
}

// KeyAdder returns a KeyAdder that can be used to add keys to the Detector.
func (d *Detector) KeyAdder(ctx context.Context) (*KeyAdder, error) {
	w, err := d.sorter.NewWriter(ctx)
	if err != nil {
		return nil, err
	}
	return &KeyAdder{w: w}, nil
}

// Detect detects duplicate keys that have been added to the Detector.
// It returns the number of duplicate keys detected and any error encountered.
func (d *Detector) Detect(ctx context.Context, opts *DetectOptions) (numDups int64, _ error) {
	if opts == nil {
		opts = &DetectOptions{}
	}
	opts.ensureDefaults()

	logTask := d.logger.Begin(zap.InfoLevel, "sort keys")
	err := d.sorter.Sort(ctx)
	logTask.End(zap.ErrorLevel, err)
	if err != nil {
		return 0, errors.Trace(err)
	}

	startKey, endKey, err := d.getRangeBounds(ctx)
	if err != nil {
		return 0, err
	}
	if compareInternalKey(startKey, endKey) >= 0 {
		return 0, nil
	}

	taskCh := make(chan task, 1)
	taskCh <- task{
		startKey: startKey,
		endKey:   endKey,
	}
	taskWg := &sync.WaitGroup{}
	taskWg.Add(1)

	var atomicNumDups atomic.Int64

	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < opts.Concurrency; i++ {
		g.Go(func() error {
			handler, err := opts.HandlerConstructor(ctx)
			if err != nil {
				return err
			}
			w := &worker{
				sorter:  d.sorter,
				taskCh:  taskCh,
				taskWg:  taskWg,
				numDups: &atomicNumDups,
				handler: handler,
				logger:  d.logger,
			}
			return w.run(ctx)
		})
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- g.Wait()
		for range taskCh {
			taskWg.Done()
		}
	}()
	taskWg.Wait()
	close(taskCh)

	return atomicNumDups.Load(), <-errCh
}

func (d *Detector) getRangeBounds(ctx context.Context) (startKey, endKey internalKey, _ error) {
	it, err := d.sorter.NewIterator(ctx)
	if err != nil {
		return internalKey{}, internalKey{}, err
	}
	defer func() {
		_ = it.Close()
	}()

	if it.First() {
		if err := decodeInternalKey(it.UnsafeKey(), &startKey); err != nil {
			return internalKey{}, internalKey{}, err
		}
	} else if err := it.Error(); err != nil {
		return internalKey{}, internalKey{}, err
	}

	if it.Last() {
		if err := decodeInternalKey(it.UnsafeKey(), &endKey); err != nil {
			return internalKey{}, internalKey{}, err
		}
		endKey.key = append(endKey.key, 0) // Make the end key exclusive.
	} else if err := it.Error(); err != nil {
		return internalKey{}, internalKey{}, err
	}

	return startKey, endKey, nil
}

// KeyAdder is used to add keys to the Detector.
type KeyAdder struct {
	w      extsort.Writer
	keyBuf []byte
}

// Add adds a key and its keyID to the KeyAdder.
//
// If key is duplicated, the keyID can be used to locate
// the duplicate key in the original data source.
func (k *KeyAdder) Add(key, keyID []byte) error {
	ikey := internalKey{key: key, keyID: keyID}
	k.keyBuf = encodeInternalKey(k.keyBuf[:0], ikey)
	return k.w.Put(k.keyBuf, nil)
}

// Flush flushes buffered keys to the detector.
func (k *KeyAdder) Flush() error {
	return k.w.Flush()
}

// Close closes the KeyAdder with buffered keys flushed.
func (k *KeyAdder) Close() error {
	return k.w.Close()
}

// DetectOptions holds optional arguments for the Detect method.
type DetectOptions struct {
	// Concurrency is the maximum number of concurrent workers.
	//
	// The default value is runtime.GOMAXPROCS(0).
	Concurrency int
	// HandlerConstructor specifies how to handle duplicate keys.
	// If it is nil, a nop handler constructor will be used.
	HandlerConstructor HandlerConstructor
}

func (o *DetectOptions) ensureDefaults() {
	if o.Concurrency <= 0 {
		o.Concurrency = runtime.GOMAXPROCS(0)
	}
	if o.HandlerConstructor == nil {
		o.HandlerConstructor = func(context.Context) (Handler, error) {
			return nopHandler{}, nil
		}
	}
}

// Handler is used to handle duplicate keys.
//
// Handler behaves like a state machine. It is called in the following order:
// Begin -> Append -> Append -> ... -> Append -> End -> Begin -> ... -> End -> Close
type Handler interface {
	// Begin is called when a new duplicate key is detected.
	Begin(key []byte) error
	// Append appends a keyID to the current duplicate key.
	// Multiple keyIDs are appended in lexicographical order.
	// NOTE: keyID may be changed after the call.
	Append(keyID []byte) error
	// End is called when all keyIDs of the current duplicate key have been appended.
	End() error
	// Close closes the Handler. It is called when the Detector finishes detecting.
	// It is guaranteed to be called after all Begin/Append/End calls.
	Close() error
}

// HandlerConstructor is used to construct a Handler to handle duplicate keys.
type HandlerConstructor func(ctx context.Context) (Handler, error)

type nopHandler struct{}

func (nopHandler) Begin(_ []byte) error  { return nil }
func (nopHandler) Append(_ []byte) error { return nil }
func (nopHandler) End() error            { return nil }
func (nopHandler) Close() error          { return nil }
