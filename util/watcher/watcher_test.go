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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWatcher(t *testing.T) {
	var (
		oldFilePath, newFilePath string
		oldFileName              = "mysql-bin.000001"
		newFileName              = "mysql-bin.000002"
		wg                       sync.WaitGroup
	)

	// create dir
	dir, err := ioutil.TempDir("", "test_watcher")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// join the path
	oldFilePath = filepath.Join(dir, oldFileName)
	newFilePath = filepath.Join(dir, newFileName)

	// create watcher
	w := NewWatcher()

	// watch directory
	err = w.Add(dir)
	require.NoError(t, err)

	// start watcher
	err = w.Start(10 * time.Millisecond)
	require.NoError(t, err)
	defer w.Close()

	// create file
	f, err := os.Create(oldFilePath)
	require.NoError(t, err)
	f.Close()

	// watch for create
	wg.Add(1)
	go func() {
		defer wg.Done()
		assertEvent(w, oldFilePath, Create, t)
	}()
	wg.Wait()

	// watch for write
	wg.Add(1)
	go func() {
		defer wg.Done()
		assertEvent(w, oldFilePath, Modify, t)
	}()

	f, err = os.OpenFile(oldFilePath, os.O_WRONLY, 0766)
	require.NoError(t, err)
	f.Write([]byte("meaningless content"))
	f.Close()
	wg.Wait()

	// watch for chmod
	wg.Add(1)
	go func() {
		defer wg.Done()
		assertEvent(w, oldFilePath, Chmod, t)
	}()

	err = os.Chmod(oldFilePath, 0777)
	require.NoError(t, err)
	wg.Wait()

	// watch for rename
	wg.Add(1)
	go func() {
		defer wg.Done()
		assertEvent(w, oldFilePath, Rename, t)
	}()

	err = os.Rename(oldFilePath, newFilePath)
	require.NoError(t, err)
	wg.Wait()

	// watch for remove
	wg.Add(1)
	go func() {
		defer wg.Done()
		assertEvent(w, newFilePath, Remove, t)
	}()

	err = os.Remove(newFilePath)
	require.NoError(t, err)
	wg.Wait()

	// watch for create again
	wg.Add(1)
	go func() {
		defer wg.Done()
		assertEvent(w, oldFilePath, Create, t)
	}()

	// create file again
	f, err = os.Create(oldFilePath)
	require.NoError(t, err)
	f.Close()
	wg.Wait()

	// create another dir
	dir2, err := ioutil.TempDir("", "test_watcher")
	require.NoError(t, err)
	defer os.RemoveAll(dir2)
	oldFilePath2 := filepath.Join(dir2, oldFileName)

	// add another directory for watching
	err = w.Add(dir2)
	require.NoError(t, err)

	// watch for move (rename to another directory)
	wg.Add(1)
	go func() {
		defer wg.Done()
		assertEvent(w, oldFilePath, Move, t)
	}()

	err = os.Rename(oldFilePath, oldFilePath2)
	require.NoError(t, err)
	wg.Wait()
}

func assertEvent(w *Watcher, path string, op Op, t *testing.T) {
	for {
		select {
		case ev := <-w.Events:
			if ev.IsDirEvent() {
				continue // skip event for directory
			}
			require.True(t, ev.HasOps(op))
			require.Equal(t, path, ev.Path)
			return
		case err2 := <-w.Errors:
			t.Fatal(err2)
			return
		}
	}
}
