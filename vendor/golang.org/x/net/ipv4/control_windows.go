// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ipv4

import "syscall"

func setControlMessage(s uintptr, opt *rawOpt, cf ControlFlags, on bool) error {
	// TODO(mikio): implement this
	return syscall.EWINDOWS
}

func newControlMessage(opt *rawOpt) []byte {
	// TODO(mikio): implement this
	return nil
}

func parseControlMessage(b []byte) (*ControlMessage, error) {
	// TODO(mikio): implement this
	return nil, syscall.EWINDOWS
}

func marshalControlMessage(cm *ControlMessage) []byte {
	// TODO(mikio): implement this
	return nil
}
