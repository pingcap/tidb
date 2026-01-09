// Copyright 2025 PingCAP, Inc.
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

//go:build !codes && !windows

package testkit

import (
	"os"
	"syscall"
	"testing"
)

func tryMakeImageOnce(t testing.TB) (retry bool, err error) {
	const lockFile = "/tmp/tidb-unistore-bootstraped-image-lock-file"
	lock, err := os.Create(lockFile)
	if err != nil {
		return true, nil
	}
	defer func() { err = os.Remove(lockFile) }()
	defer lock.Close()

	// Prevent other process from creating the image concurrently
	err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return true, nil
	}
	defer func() { err = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()

	// Now this is the only instance to do the operation.
	// Use the shared platform-independent image creation logic
	return createMockStoreImage(t)
}
