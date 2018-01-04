// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build nacl plan9

package ipv6

func setControlMessage(s uintptr, opt *rawOpt, cf ControlFlags, on bool) error {
	return errOpNoSupport
}

func newControlMessage(opt *rawOpt) (oob []byte) {
	return nil
}

func parseControlMessage(b []byte) (*ControlMessage, error) {
	return nil, errOpNoSupport
}

func marshalControlMessage(cm *ControlMessage) (oob []byte) {
	return nil
}
