// Copyright 2026 PingCAP, Inc.
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

package testutil

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

// NewVersionCreatedEvent describes one upstream object version creation.
type NewVersionCreatedEvent struct {
	Path string
}

// CRRWorker simulates a CRR background worker.
//
// It only consumes NewVersionCreatedEvent from a channel. PullMessages moves
// available events into a local buffer. ReplicateBuffered replicates files from
// the buffer to downstream, and intentionally writes newest first.
type CRRWorker struct {
	upstream   storeapi.Storage
	downstream storeapi.Storage
	messages   <-chan NewVersionCreatedEvent

	buffer []NewVersionCreatedEvent
}

func NewCRRWorker(
	upstream storeapi.Storage,
	downstream storeapi.Storage,
	messages <-chan NewVersionCreatedEvent,
) *CRRWorker {
	return &CRRWorker{
		upstream:   upstream,
		downstream: downstream,
		messages:   messages,
	}
}

// PullMessages drains up to limit events from channel into local buffer.
//
// If limit <= 0, it drains all currently available events.
func (w *CRRWorker) PullMessages(limit int) int {
	if w.messages == nil {
		return 0
	}

	pulled := 0
	for limit <= 0 || pulled < limit {
		select {
		case event, ok := <-w.messages:
			if !ok {
				w.messages = nil
				return pulled
			}
			if event.Path == "" {
				continue
			}
			w.buffer = append(w.buffer, event)
			pulled++
		default:
			return pulled
		}
	}
	return pulled
}

// BufferedMessages returns a snapshot of not-yet-replicated events.
func (w *CRRWorker) BufferedMessages() []NewVersionCreatedEvent {
	out := make([]NewVersionCreatedEvent, len(w.buffer))
	copy(out, w.buffer)
	return out
}

// ReplicateBuffered replicates up to limit buffered events.
//
// If limit <= 0, it replicates all buffered events.
func (w *CRRWorker) ReplicateBuffered(ctx context.Context, limit int) (int, error) {
	if limit <= 0 || limit > len(w.buffer) {
		limit = len(w.buffer)
	}

	replicated := 0
	for replicated < limit {
		idx := len(w.buffer) - 1
		event := w.buffer[idx]

		if err := w.replicateOne(ctx, event.Path); err != nil {
			return replicated, err
		}
		w.buffer = w.buffer[:idx]
		replicated++
	}
	return replicated, nil
}

// ReplicateBufferedRandom replicates up to limit buffered events in random order.
func (w *CRRWorker) ReplicateBufferedRandom(
	ctx context.Context,
	limit int,
	intN func(int) int,
) (int, error) {
	if intN == nil {
		return 0, fmt.Errorf("random replicate buffered: nil intN")
	}
	if len(w.buffer) >= 2 {
		for i := len(w.buffer) - 1; i > 0; i-- {
			j := intN(i + 1)
			w.buffer[i], w.buffer[j] = w.buffer[j], w.buffer[i]
		}
	}
	return w.ReplicateBuffered(ctx, limit)
}

func (w *CRRWorker) replicateOne(ctx context.Context, name string) error {
	exists, err := w.upstream.FileExists(ctx, name)
	if err != nil {
		return fmt.Errorf("check upstream file %s: %w", name, err)
	}
	if !exists {
		return nil
	}

	payload, err := w.upstream.ReadFile(ctx, name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read upstream file %s: %w", name, err)
	}

	if err := w.downstream.WriteFile(ctx, name, payload); err != nil {
		return fmt.Errorf("write downstream file %s: %w", name, err)
	}
	return nil
}

// CRRUpstreamStorage writes to upstream and emits NewVersionCreatedEvent.
type CRRUpstreamStorage struct {
	storage storeapi.Storage
	events  chan<- NewVersionCreatedEvent
}

var _ storeapi.Storage = (*CRRUpstreamStorage)(nil)

func NewCRRUpstreamStorage(
	storage storeapi.Storage,
	events chan<- NewVersionCreatedEvent,
) *CRRUpstreamStorage {
	return &CRRUpstreamStorage{storage: storage, events: events}
}

func emitNewVersionEvent(
	ctx context.Context,
	events chan<- NewVersionCreatedEvent,
	name string,
) error {
	if events == nil {
		return fmt.Errorf("emit new-version event for %s: event channel is nil", name)
	}
	select {
	case events <- NewVersionCreatedEvent{Path: name}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("emit new-version event for %s: %w", name, ctx.Err())
	}
}

func (u *CRRUpstreamStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	if err := u.storage.WriteFile(ctx, name, data); err != nil {
		return err
	}
	return emitNewVersionEvent(ctx, u.events, name)
}

func (u *CRRUpstreamStorage) ReadFile(ctx context.Context, name string) ([]byte, error) {
	return u.storage.ReadFile(ctx, name)
}

func (u *CRRUpstreamStorage) FileExists(ctx context.Context, name string) (bool, error) {
	return u.storage.FileExists(ctx, name)
}

func (u *CRRUpstreamStorage) DeleteFile(ctx context.Context, name string) error {
	return u.storage.DeleteFile(ctx, name)
}

func (u *CRRUpstreamStorage) Open(
	ctx context.Context,
	name string,
	option *storeapi.ReaderOption,
) (objectio.Reader, error) {
	return u.storage.Open(ctx, name, option)
}

func (u *CRRUpstreamStorage) DeleteFiles(ctx context.Context, names []string) error {
	return u.storage.DeleteFiles(ctx, names)
}

func (u *CRRUpstreamStorage) WalkDir(
	ctx context.Context,
	opt *storeapi.WalkOption,
	fn func(path string, size int64) error,
) error {
	return u.storage.WalkDir(ctx, opt, fn)
}

func (u *CRRUpstreamStorage) URI() string {
	return u.storage.URI()
}

func (u *CRRUpstreamStorage) Create(
	ctx context.Context,
	name string,
	option *storeapi.WriterOption,
) (objectio.Writer, error) {
	inner, err := u.storage.Create(ctx, name, option)
	if err != nil {
		return nil, err
	}
	return &crrEventWriter{inner: inner, name: name, events: u.events}, nil
}

func (u *CRRUpstreamStorage) Rename(ctx context.Context, oldFileName, newFileName string) error {
	if err := u.storage.Rename(ctx, oldFileName, newFileName); err != nil {
		return err
	}
	return emitNewVersionEvent(ctx, u.events, newFileName)
}

func (u *CRRUpstreamStorage) PresignFile(ctx context.Context, fileName string, expire time.Duration) (string, error) {
	return u.storage.PresignFile(ctx, fileName, expire)
}

func (u *CRRUpstreamStorage) Close() {
	u.storage.Close()
}

type crrEventWriter struct {
	inner  objectio.Writer
	name   string
	events chan<- NewVersionCreatedEvent
}

func (w *crrEventWriter) Write(ctx context.Context, p []byte) (int, error) {
	return w.inner.Write(ctx, p)
}

func (w *crrEventWriter) Close(ctx context.Context) error {
	if err := w.inner.Close(ctx); err != nil {
		return err
	}
	return emitNewVersionEvent(ctx, w.events, w.name)
}
