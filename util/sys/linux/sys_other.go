// Copyright 2019 PingCAP, Inc.
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
//go:build !linux && !windows

package linux

import (
	"net"
	"runtime"

	"golang.org/x/sys/unix"
)

// OSVersion returns version info of operation system.
// for non-linux system will only return os and arch info.
func OSVersion() (osVersion string, err error) {
	osVersion = runtime.GOOS + "." + runtime.GOARCH
	return
}

// SetAffinity sets cpu affinity.
func SetAffinity(cpus []int) error {
	return nil
}

// GetSockUID gets the uid of the other end of the UNIX domain socket
func GetSockUID(uc net.UnixConn) (uid uint32, err error) {
	raw, err := uc.SyscallConn()
	if err != nil {
		return 0, err
	}

	var cred *unix.Xucred
	err = raw.Control(func(fd uintptr) {
		cred, err = unix.GetsockoptXucred(int(fd),
			unix.SOL_LOCAL,
			unix.LOCAL_PEERCRED)
	})
	if err != nil {
		return 0, err
	}

	return cred.Uid, nil
}
