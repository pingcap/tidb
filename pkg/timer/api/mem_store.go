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

package api

import (
	"context"
	"encoding/hex"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/timeutil"
)

type memStoreWatcher struct {
	ctx context.Context
	ch  chan WatchTimerResponse
}

type memoryStoreCore struct {
	mu         sync.RWMutex
	namespaces map[string]map[string]*TimerRecord
	id2Timers  map[string]*TimerRecord
	notifier   TimerWatchEventNotifier
}

// NewMemoryTimerStore creates a memory store for timers
func NewMemoryTimerStore() *TimerStore {
	return &TimerStore{
		TimerStoreCore: &memoryStoreCore{
			namespaces: make(map[string]map[string]*TimerRecord),
			id2Timers:  make(map[string]*TimerRecord),
			notifier:   NewMemTimerWatchEventNotifier(),
		},
	}
}

func (s *memoryStoreCore) Create(_ context.Context, record *TimerRecord) (string, error) {
	if record == nil {
		return "", errors.New("timer should not be nil")
	}

	if record.ID != "" {
		return "", errors.New("ID should not be specified when create record")
	}

	if record.Version != 0 {
		return "", errors.New("Version should not be specified when create record")
	}

	if !record.CreateTime.IsZero() {
		return "", errors.New("CreateTime should not be specified when create record")
	}

	if err := record.Validate(); err != nil {
		return "", err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record = record.Clone()
	uid := uuid.New()
	record.ID = hex.EncodeToString(uid[:])
	record.Location = getMemStoreTimeZoneLoc(record.TimeZone)
	record.Version = 1
	record.CreateTime = time.Now()

	if record.EventStatus == "" {
		record.EventStatus = SchedEventIdle
	}

	normalizeTimeFields(record)

	if _, ok := s.id2Timers[record.ID]; ok {
		return "", errors.Trace(ErrTimerExists)
	}

	ns, ok := s.namespaces[record.Namespace]
	if !ok {
		ns = make(map[string]*TimerRecord)
		s.namespaces[record.Namespace] = ns
	} else {
		if _, ok = ns[record.Key]; ok {
			return "", errors.Trace(ErrTimerExists)
		}
	}

	s.id2Timers[record.ID] = record
	ns[record.Key] = record
	s.notifier.Notify(WatchTimerEventCreate, record.ID)
	return record.ID, nil
}

func (s *memoryStoreCore) List(_ context.Context, cond Cond) ([]*TimerRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*TimerRecord, 0, 1)
	for _, ns := range s.namespaces {
		for _, t := range ns {
			if cond == nil || cond.Match(t) {
				result = append(result, t.Clone())
			}
		}
	}
	return result, nil
}

func (s *memoryStoreCore) Update(_ context.Context, timerID string, update *TimerUpdate) error {
	if update == nil {
		return errors.New("update should not be nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.id2Timers[timerID]
	if !ok {
		return ErrTimerNotExist
	}

	newRecord, err := update.apply(record)
	if err != nil {
		return err
	}

	normalizeTimeFields(newRecord)
	if err = newRecord.Validate(); err != nil {
		return err
	}

	newRecord.Version++
	s.id2Timers[timerID] = newRecord
	s.namespaces[record.Namespace][record.Key] = newRecord
	s.notifier.Notify(WatchTimerEventUpdate, timerID)
	return nil
}

func (s *memoryStoreCore) Delete(_ context.Context, timerID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.id2Timers[timerID]
	if !ok {
		return false, nil
	}

	delete(s.id2Timers, timerID)
	ns := s.namespaces[record.Namespace]
	delete(ns, record.Key)
	if len(ns) == 0 {
		delete(s.namespaces, record.Namespace)
	}
	s.notifier.Notify(WatchTimerEventDelete, timerID)
	return true, nil
}

func (*memoryStoreCore) WatchSupported() bool {
	return true
}

func (s *memoryStoreCore) Watch(ctx context.Context) WatchTimerChan {
	return s.notifier.Watch(ctx)
}

func (s *memoryStoreCore) Close() {
	s.notifier.Close()
}

type memTimerWatchEventNotifier struct {
	mu       sync.RWMutex
	ctx      context.Context
	wg       sync.WaitGroup
	cancel   func()
	watchers []*memStoreWatcher
}

// NewMemTimerWatchEventNotifier creates a notifier with memory implement
func NewMemTimerWatchEventNotifier() TimerWatchEventNotifier {
	ctx, cancel := context.WithCancel(context.Background())
	return &memTimerWatchEventNotifier{
		ctx:      ctx,
		cancel:   cancel,
		watchers: make([]*memStoreWatcher, 0, 8),
	}
}

func (n *memTimerWatchEventNotifier) Watch(ctx context.Context) WatchTimerChan {
	n.mu.Lock()
	defer n.mu.Unlock()
	ch := make(chan WatchTimerResponse)
	if n.cancel == nil {
		close(ch)
		return ch
	}

	watcher := &memStoreWatcher{
		ctx: ctx,
		ch:  make(chan WatchTimerResponse, 8),
	}
	n.watchers = append(n.watchers, watcher)
	n.wg.Add(1)
	go func() {
		defer func() {
			n.mu.Lock()
			defer n.mu.Unlock()
			close(ch)
			if i := slices.Index(n.watchers, watcher); i >= 0 {
				n.watchers = slices.Delete(n.watchers, i, i+1)
			}
			n.wg.Done()
		}()

		for {
			select {
			case <-n.ctx.Done():
				return
			case <-ctx.Done():
				return
			case resp := <-watcher.ch:
				select {
				case <-n.ctx.Done():
					return
				case <-ctx.Done():
					return
				case ch <- resp:
				}
			}
		}
	}()
	return ch
}

func (n *memTimerWatchEventNotifier) Notify(tp WatchTimerEventType, timerID string) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.cancel == nil {
		return
	}

	resp := WatchTimerResponse{
		Events: []*WatchTimerEvent{
			{
				Tp:      tp,
				TimerID: timerID,
			},
		},
	}

	for _, w := range n.watchers {
		select {
		case <-n.ctx.Done():
			return
		case <-w.ctx.Done():
			continue
		case w.ch <- resp:
		default:
			watcher := w
			n.wg.Add(1)
			go func() {
				defer n.wg.Done()
				select {
				case <-n.ctx.Done():
					return
				case <-watcher.ctx.Done():
					return
				case watcher.ch <- resp:
				}
			}()
		}
	}
}

func (n *memTimerWatchEventNotifier) Close() {
	n.mu.Lock()
	if n.cancel != nil {
		n.cancel()
		n.cancel = nil
	}
	n.mu.Unlock()
	n.wg.Wait()
}

func getMemStoreTimeZoneLoc(tz string) *time.Location {
	if tz == "" {
		return timeutil.SystemLocation()
	}

	if loc, err := timeutil.ParseTimeZone(tz); err == nil {
		return loc
	}

	return timeutil.SystemLocation()
}

func normalizeTimeFields(record *TimerRecord) {
	if record.Location == nil {
		return
	}

	if !record.Watermark.IsZero() {
		record.Watermark = record.Watermark.In(record.Location)
	}

	if !record.EventStart.IsZero() {
		record.EventStart = record.EventStart.In(record.Location)
	}

	if !record.CreateTime.IsZero() {
		record.CreateTime = record.CreateTime.In(record.Location)
	}
}
