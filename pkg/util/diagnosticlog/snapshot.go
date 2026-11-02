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

package diagnosticlog

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

var snapshotName = regexp.MustCompile(`^index-usage-[0-9]{8}T[0-9]{6}\.[0-9]{6}Z-[0-9a-f]{32}\.jsonl$`)

// Publisher owns one node's directory of immutable, complete snapshot files.
type Publisher struct {
	directory string
	mu        sync.Mutex
}

// NewPublisher binds a node's publication directory; it creates no file yet.
func NewPublisher(directory string) *Publisher { return &Publisher{directory: directory} }

// Publish writes and closes a temporary file before exposing a unique formal
// name. A failed attempt preserves previous successes; cleanup follows publication.
// write must emit details followed by the footer with the supplied timestamp.
func (p *Publisher) Publish(start time.Time, write func(io.Writer, time.Time) error) (string, error) {
	if !p.mu.TryLock() {
		observe("index_usage", "busy")
		return "", fmt.Errorf("index usage export already running")
	}
	defer p.mu.Unlock()
	name, err := p.publish(start, write)
	if err != nil {
		observe("index_usage", "export_failed")
		return "", err
	}
	observe("index_usage", "published")
	entries, err := os.ReadDir(p.directory)
	if err != nil {
		observe("index_usage", "cleanup_failed")
		return name, nil
	}
	for _, entry := range entries {
		if entry.Name() != filepath.Base(name) && snapshotName.MatchString(entry.Name()) && !entry.IsDir() {
			if err := os.Remove(filepath.Join(p.directory, entry.Name())); err != nil {
				observe("index_usage", "cleanup_failed")
			}
		}
	}
	return name, nil
}

func (p *Publisher) publish(start time.Time, write func(io.Writer, time.Time) error) (string, error) {
	start = start.UTC().Truncate(time.Microsecond)
	if err := os.MkdirAll(p.directory, 0o750); err != nil {
		return "", err
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	name := filepath.Join(p.directory, "index-usage-"+start.Format("20060102T150405.000000Z")+"-"+hex.EncodeToString(id[:])+".jsonl")
	tmp := name + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", err
	}
	defer func() {
		if err := os.Remove(tmp); err != nil && !os.IsNotExist(err) {
			observe("index_usage", "cleanup_failed")
		}
	}()
	// Also close on an unexpected panic in the callback; normal close errors are
	// checked below before publication.
	defer func() { _ = f.Close() }()
	buffer := bufio.NewWriter(f)
	writeErr := write(buffer, start)
	if writeErr == nil {
		writeErr = buffer.Flush()
	}
	if writeErr == nil {
		writeErr = f.Sync()
	}
	closeErr := f.Close()
	if writeErr != nil {
		return "", writeErr
	}
	if closeErr != nil {
		return "", closeErr
	}
	// A hard link exposes the fully closed inode atomically and, unlike rename,
	// cannot replace an existing formal file even on a random-name collision.
	if err := os.Link(tmp, name); err != nil {
		return "", err
	}
	return name, nil
}
