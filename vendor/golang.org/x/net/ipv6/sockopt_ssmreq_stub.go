// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build !darwin,!freebsd,!linux,!solaris

package ipv6

import "net"

func setsockoptGroupReq(s uintptr, opt *sockOpt, ifi *net.Interface, grp net.IP) error {
	return errOpNoSupport
}

func setsockoptGroupSourceReq(s uintptr, opt *sockOpt, ifi *net.Interface, grp, src net.IP) error {
	return errOpNoSupport
}
