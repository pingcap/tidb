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
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"golang.org/x/exp/slices"
)

type memStoreWatcher struct {
	ctx context.Context
	ch  chan WatchTimerResponse
}

type memoryStoreCore struct {
	mu         sync.Mutex
	namespaces map[string]map[string]*TimerRecord
	id2Timers  map[string]*TimerRecord
	watchers   []*memStoreWatcher
}

// NewMemoryTimerStore creates a memory store for timers
func NewMemoryTimerStore() *TimerStore {
	return &TimerStore{
		TimerStoreCore: &memoryStoreCore{
			namespaces: make(map[string]map[string]*TimerRecord),
			id2Timers:  make(map[string]*TimerRecord),
		},
	}
}

func (s *memoryStoreCore) Create(_ context.Context, record *TimerRecord) (string, error) {
	if record == nil {
		return "", errors.New("timer should not be nil")
	}

	if err := record.Validate(); err != nil {
		return "", err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record = record.Clone()
	if record.ID == "" {
		uid := uuid.New()
		record.ID = hex.EncodeToString(uid[:])
	}

	if record.Version == 0 {
		record.Version = 1
	}

	if record.EventStatus == "" {
		record.EventStatus = SchedEventIdle
	}

	if record.CreateTime.IsZero() {
		record.CreateTime = time.Now()
	}

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
	s.notify(WatchTimerEventCreate, record.ID)
	return record.ID, nil
}

func (s *memoryStoreCore) List(_ context.Context, cond Cond) ([]*TimerRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

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

	newRecord, err := update.Apply(record)
	if err != nil {
		return err
	}

	if err = newRecord.Validate(); err != nil {
		return err
	}

	newRecord.Version++
	s.id2Timers[timerID] = newRecord
	s.namespaces[record.Namespace][record.Key] = newRecord
	s.notify(WatchTimerEventUpdate, timerID)
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
	s.notify(WatchTimerEventDelete, timerID)
	return true, nil
}

func (*memoryStoreCore) WatchSupported() bool {
	return true
}

func (s *memoryStoreCore) Watch(ctx context.Context) WatchTimerChan {
	s.mu.Lock()
	defer s.mu.Unlock()
	watcher := &memStoreWatcher{
		ctx: ctx,
		ch:  make(chan WatchTimerResponse, 8),
	}
	s.watchers = append(s.watchers, watcher)
	ch := make(chan WatchTimerResponse)
	go func() {
		defer func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			close(ch)
			if i := slices.Index(s.watchers, watcher); i >= 0 {
				s.watchers = slices.Delete(s.watchers, i, i+1)
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case resp := <-watcher.ch:
				select {
				case <-ctx.Done():
					return
				case ch <- resp:
				}
			}
		}
	}()
	return ch
}

func (s *memoryStoreCore) notify(tp WatchTimerEventType, timerID string) {
	resp := WatchTimerResponse{
		Events: []*WatchTimerEvent{
			{
				Tp:      tp,
				TimerID: timerID,
			},
		},
	}

	for _, w := range s.watchers {
		select {
		case <-w.ctx.Done():
			return
		case w.ch <- resp:
		default:
			watcher := w
			go func() {
				select {
				case <-watcher.ctx.Done():
					return
				case watcher.ch <- resp:
				}
			}()
		}
	}
}
