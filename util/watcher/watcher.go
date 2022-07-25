// Copyright 2022 PingCAP, Inc.
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

package watcher

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"go.uber.org/atomic"
)

// errors created by Watcher
var (
	ErrWatcherStarted = errors.New("watcher already started")
	ErrWatcherClosed  = errors.New("watcher already closed")
)

// Watcher watches for files or directory changes by polling
// currently, if multi operations applied to one file or directory, only one event (with single Op) will be sent
// the priority of Op is:
//   1. Modify
//   2. Chmod
//   3. Rename / Move
//   4. Create / Remove
type Watcher struct {
	Events chan Event
	Errors chan error

	running atomic.Int32
	closed  chan struct{}
	wg      sync.WaitGroup
	mu      sync.Mutex

	names map[string]struct{}    // original added names needed to watch
	files map[string]os.FileInfo // all latest watching files
}

// NewWatcher creates a new Watcher instance
func NewWatcher() *Watcher {
	w := &Watcher{
		Events: make(chan Event),
		Errors: make(chan error),
		closed: make(chan struct{}),
		names:  make(map[string]struct{}),
		files:  make(map[string]os.FileInfo),
	}
	return w
}

// Start starts the watching
func (w *Watcher) Start(d time.Duration) error {
	if !w.running.CAS(0, 1) {
		return ErrWatcherStarted
	}

	select {
	case <-w.closed:
		return ErrWatcherClosed
	default:
	}

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.doWatch(d)
	}()
	return nil
}

// Close stops the watching
func (w *Watcher) Close() {
	if !w.running.CAS(1, 0) {
		return
	}

	close(w.closed)
	w.wg.Wait()

	close(w.Events)
	close(w.Errors)

	w.mu.Lock()
	w.names = make(map[string]struct{})
	w.files = make(map[string]os.FileInfo)
	w.mu.Unlock()
}

// Add adds named file or directory (non-recursively) to the watching list
// this can only be success if all events be sent (lock release)
func (w *Watcher) Add(name string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	select {
	case <-w.closed:
		return ErrWatcherClosed
	default:
	}

	fileList, err := listForName(name)
	if err != nil {
		return errors.Trace(err)
	}

	w.names[name] = struct{}{}
	for fp, fi := range fileList {
		w.files[fp] = fi
	}

	return nil
}

// Remove removes named file or directory (non-recursively) from the watching list
// this can only be success if all events be sent (lock release)
func (w *Watcher) Remove(name string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	select {
	case <-w.closed:
		return ErrWatcherClosed
	default:
	}

	w.doRemove(name)
	return nil
}

// doRemove does the actual remove operation
// it does not consider the mutex protection
func (w *Watcher) doRemove(name string) {
	delete(w.names, name)

	fi, ok := w.files[name]
	if !ok {
		return // not exists anymore
	}

	// remove itself
	delete(w.files, name)

	if !fi.IsDir() {
		// not a directory, return
		return
	}

	for fp := range w.files {
		// remove all files in this directory
		if filepath.Dir(fp) == name {
			delete(w.files, fp)
		}
	}
}

// doWatch does the watching
func (w *Watcher) doWatch(d time.Duration) {
	ticker := time.NewTicker(d)
	defer ticker.Stop()
	for {
		select {
		case <-w.closed:
			return
		case <-ticker.C:
			currFileList := w.listForAll()
			w.pollEvents(currFileList)

			// update file list
			w.mu.Lock()
			w.files = currFileList
			w.mu.Unlock()
		}
	}
}

// pollEvents polls events for files
func (w *Watcher) pollEvents(currFileList map[string]os.FileInfo) {
	w.mu.Lock()
	defer w.mu.Unlock()

	creates := make(map[string]os.FileInfo)
	removes := make(map[string]os.FileInfo)

	// check for remove
	for latestFp, latestFi := range w.files {
		if _, ok := currFileList[latestFp]; !ok {
			removes[latestFp] = latestFi
		}
	}

	// check for create / modify / chmod
	for fp, currFi := range currFileList {
		latestFi, ok := w.files[fp]
		if !ok {
			// create
			creates[fp] = currFi
			continue
		}

		// ModTime may return timestamp in second level on some file system
		// So use ModTime + Size to judge modify event will be more precisely
		if !latestFi.ModTime().Equal(currFi.ModTime()) ||
			latestFi.Size() != currFi.Size() {
			// modify
			select {
			case <-w.closed:
				return
			case w.Events <- Event{Path: fp, Op: Modify, FileInfo: currFi}:
			}
		}

		if latestFi.Mode() != currFi.Mode() {
			// chmod
			select {
			case <-w.closed:
				return
			case w.Events <- Event{Path: fp, Op: Chmod, FileInfo: currFi}:
			}
		}
	}

	// check for rename / move
	for removeFp, removeFi := range removes {
		for createFp, createFi := range creates {
			if os.SameFile(removeFi, createFi) {
				ev := Event{
					Path:     removeFp, // for Move, use from-path
					Op:       Move,
					FileInfo: removeFi,
				}
				if filepath.Dir(removeFp) == filepath.Dir(createFp) {
					ev.Op = Rename
				}

				delete(removes, removeFp)
				delete(creates, createFp)

				select {
				case <-w.closed:
					return
				case w.Events <- ev:
				}
			}
		}
	}

	// send events for create
	for fp, fi := range creates {
		select {
		case <-w.closed:
			return
		case w.Events <- Event{Path: fp, Op: Create, FileInfo: fi}:
		}
	}

	// send events for remove
	for fp, fi := range removes {
		select {
		case <-w.closed:
			return
		case w.Events <- Event{Path: fp, Op: Remove, FileInfo: fi}:
		}
	}
}

// listForAll returns a list of files info for all watching files and directories currently
func (w *Watcher) listForAll() map[string]os.FileInfo {
	w.mu.Lock()
	defer w.mu.Unlock()

	fileList := make(map[string]os.FileInfo)
	for name := range w.names {
		fl, err := listForName(name)
		if err != nil {
			if os.IsNotExist(errors.Cause(err)) {
				w.doRemove(name)
			}
			select {
			case <-w.closed:
				return nil
			case w.Errors <- err:
			}
		}
		for fp, fi := range fl {
			fileList[fp] = fi
		}
	}

	return fileList
}

// listForName returns a list of files info for this file or files in this directory
func listForName(name string) (map[string]os.FileInfo, error) {
	stat, err := os.Stat(name)
	if err != nil {
		return nil, errors.Annotatef(err, "name %s", name)
	}

	list := make(map[string]os.FileInfo)
	list[name] = stat

	if !stat.IsDir() {
		// not a directory, return
		return list, nil
	}

	fInfoList, err := ioutil.ReadDir(name)
	if err != nil {
		return nil, errors.Annotatef(err, "directory %s", name)
	}

	for _, fi := range fInfoList {
		fp := filepath.Join(name, fi.Name())
		list[fp] = fi
	}

	return list, nil
}
