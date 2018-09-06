// Copyright 2015 The etcd Authors
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

package snap

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/coreos/etcd/pkg/fileutil"
)

var ErrNoDBSnapshot = errors.New("snap: snapshot file doesn't exist")

// SaveDBFrom saves snapshot of the database from the given reader. It
// guarantees the save operation is atomic.
func (s *Snapshotter) SaveDBFrom(r io.Reader, id uint64) (int64, error) {
	f, err := ioutil.TempFile(s.dir, "tmp")
	if err != nil {
		return 0, err
	}
	var n int64
	n, err = io.Copy(f, r)
	if err == nil {
		err = fileutil.Fsync(f)
	}
	f.Close()
	if err != nil {
		os.Remove(f.Name())
		return n, err
	}
	fn := s.dbFilePath(id)
	if fileutil.Exist(fn) {
		os.Remove(f.Name())
		return n, nil
	}
	err = os.Rename(f.Name(), fn)
	if err != nil {
		os.Remove(f.Name())
		return n, err
	}

	plog.Infof("saved database snapshot to disk [total bytes: %d]", n)

	return n, nil
}

// DBFilePath returns the file path for the snapshot of the database with
// given id. If the snapshot does not exist, it returns error.
func (s *Snapshotter) DBFilePath(id uint64) (string, error) {
	if _, err := fileutil.ReadDir(s.dir); err != nil {
		return "", err
	}
	if fn := s.dbFilePath(id); fileutil.Exist(fn) {
		return fn, nil
	}
	return "", ErrNoDBSnapshot
}

func (s *Snapshotter) dbFilePath(id uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("%016x.snap.db", id))
}
