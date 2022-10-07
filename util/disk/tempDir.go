// Copyright 2020 PingCAP, Inc.
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

package disk

import (
	"os"
	"path/filepath"

	"github.com/danjacques/gofslock/fslock"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/terror"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

var (
	tempDirLock fslock.Handle
	sf          singleflight.Group
)

const (
	lockFile  = "_dir.lock"
	recordDir = "record"
)

// CheckAndInitTempDir check whether the temp directory is existed.
// If not, initializes the temp directory.
func CheckAndInitTempDir(path string) (err error) {
	_, err, _ = sf.Do("tempDir", func() (value interface{}, err error) {
		if !checkTempDirExist(path) {
			log.Info("Tmp-storage-path not found. Try to initialize TempDir.")
			err = initialzeTempDirWithPath(path)
		}
		return
	})
	return
}

func checkTempDirExist(path string) bool {
	_, err := os.Stat(path)
	if err != nil && !os.IsExist(err) {
		return false
	}
	return true
}

func initialzeTempDirWithPath(path string) error {
	_, err := os.Stat(path)
	if err != nil && !os.IsExist(err) {
		err = os.MkdirAll(path, 0750)
		if err != nil {
			return err
		}
	}
	CleanUp()
	tempDirLock, err = fslock.Lock(filepath.Join(path, lockFile))
	if err != nil {
		switch err {
		case fslock.ErrLockHeld:
			log.Error("The current temporary storage dir has been occupied by another instance, "+
				"check [instance].tmpdir config and make sure they are different.", zap.String("TempStoragePath", path), zap.Error(err))
		default:
			log.Error("Failed to acquire exclusive lock on the temporary storage dir.", zap.String("TempStoragePath", path), zap.Error(err))
		}
		return err
	}

	subDirs, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	// If it exists others files except lock file, creates another goroutine to clean them.
	if len(subDirs) > 2 {
		go func() {
			for _, subDir := range subDirs {
				// Do not remove the lock file.
				switch subDir.Name() {
				case lockFile, recordDir:
					continue
				}
				err := os.RemoveAll(filepath.Join(path, subDir.Name()))
				if err != nil {
					log.Warn("Remove temporary file error",
						zap.String("tempStorageSubDir", filepath.Join(path, subDir.Name())), zap.Error(err))
				}
			}
		}()
	}
	return nil
}

// InitializeTempDir initializes the temp directory.
func InitializeTempDir() error {
	return initialzeTempDirWithPath(config.GetGlobalConfig().Instance.TmpDir.Load())
}

// CleanUp releases the directory lock when exiting TiDB or changing tmpdir.
func CleanUp() {
	if tempDirLock != nil {
		err := tempDirLock.Unlock()
		terror.Log(errors.Trace(err))
	}
}

// CheckAndCreateDir check whether the directory is existed. If not, then create it.
func CheckAndCreateDir(path string) error {
	_, err := os.Stat(path)
	if err != nil && !os.IsExist(err) {
		err = os.MkdirAll(path, 0750)
		if err != nil {
			return err
		}
	}
	return nil
}
