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
// See the License for the specific language governing permissions and
// limitations under the License.

package disk

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/danjacques/gofslock/fslock"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

var (
	tempDirLock fslock.Handle
	sf          singleflight.Group
)

// CheckAndInitTempDir check whether the temp directory is existed.
// If not, initializes the temp directory.
func CheckAndInitTempDir() (err error) {
	_, err, _ = sf.Do("tempDir", func() (value interface{}, err error) {
		if !checkTempDirExist() {
			log.Info("Tmp-storage-path not found. Try to initialize TempDir.")
			err = InitializeTempDir()
		}
		return
	})
	return
}

func checkTempDirExist() bool {
	tempDir := config.GetGlobalConfig().TempStoragePath
	_, err := os.Stat(tempDir)
	if err != nil && !os.IsExist(err) {
		return false
	}
	return true
}

// InitializeTempDir initializes the temp directory.
func InitializeTempDir() error {
	tempDir := config.GetGlobalConfig().TempStoragePath
	_, err := os.Stat(tempDir)
	if err != nil && !os.IsExist(err) {
		err = os.MkdirAll(tempDir, 0755)
		if err != nil {
			return err
		}
	}
	lockFile := "_dir.lock"
	tempDirLock, err = fslock.Lock(filepath.Join(tempDir, lockFile))
	if err != nil {
		switch err {
		case fslock.ErrLockHeld:
			log.Error("The current temporary storage dir has been occupied by another instance, "+
				"check tmp-storage-path config and make sure they are different.", zap.String("TempStoragePath", tempDir), zap.Error(err))
		default:
			log.Error("Failed to acquire exclusive lock on the temporary storage dir.", zap.String("TempStoragePath", tempDir), zap.Error(err))
		}
		return err
	}

	subDirs, err := ioutil.ReadDir(tempDir)
	if err != nil {
		return err
	}

	for _, subDir := range subDirs {
		// Do not remove the lock file.
		if subDir.Name() == lockFile {
			continue
		}
		err = os.RemoveAll(filepath.Join(tempDir, subDir.Name()))
		if err != nil {
			log.Warn("Remove temporary file error",
				zap.String("tempStorageSubDir", filepath.Join(tempDir, subDir.Name())), zap.Error(err))
		}
	}
	return nil
}

// CleanUp releases the directory lock when exiting TiDB.
func CleanUp() {
	if tempDirLock != nil {
		err := tempDirLock.Unlock()
		terror.Log(errors.Trace(err))
	}
}
