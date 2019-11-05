// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ipv4_test

import (
	"net"
	"runtime"
	"testing"

	"golang.org/x/net/ipv4"
	"golang.org/x/net/nettest"
)

var udpMultipleGroupListenerTests = []net.Addr{
	&net.UDPAddr{IP: net.IPv4(224, 0, 0, 249)}, // see RFC 4727
	&net.UDPAddr{IP: net.IPv4(224, 0, 0, 250)},
	&net.UDPAddr{IP: net.IPv4(224, 0, 0, 254)},
}

func TestUDPSinglePacketConnWithMultipleGroupListeners(t *testing.T) {
	switch runtime.GOOS {
	case "fuchsia", "hurd", "js", "nacl", "plan9", "windows":
		t.Skipf("not supported on %s", runtime.GOOS)
	}
	if testing.Short() {
		t.Skip("to avoid external network")
	}

	for _, gaddr := range udpMultipleGroupListenerTests {
		c, err := net.ListenPacket("udp4", "0.0.0.0:0") // wildcard address with no reusable port
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		p := ipv4.NewPacketConn(c)
		var mift []*net.Interface

		ift, err := net.Interfaces()
		if err != nil {
			t.Fatal(err)
		}
		for i, ifi := range ift {
			if _, err := nettest.MulticastSource("ip4", &ifi); err != nil {
				continue
			}
			if err := p.JoinGroup(&ifi, gaddr); err != nil {
				t.Fatal(err)
			}
			mift = append(mift, &ift[i])
		}
		for _, ifi := range mift {
			if err := p.LeaveGroup(ifi, gaddr); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestUDPMultiplePacketConnWithMultipleGroupListeners(t *testing.T) {
	switch runtime.GOOS {
	case "fuchsia", "hurd", "js", "nacl", "plan9", "windows":
		t.Skipf("not supported on %s", runtime.GOOS)
	}
	if testing.Short() {
		t.Skip("to avoid external network")
	}

	for _, gaddr := range udpMultipleGroupListenerTests {
		c1, err := net.ListenPacket("udp4", "224.0.0.0:0") // wildcard address with reusable port
		if err != nil {
			t.Fatal(err)
		}
		defer c1.Close()
		_, port, err := net.SplitHostPort(c1.LocalAddr().String())
		if err != nil {
			t.Fatal(err)
		}
		c2, err := net.ListenPacket("udp4", net.JoinHostPort("224.0.0.0", port)) // wildcard address with reusable port
		if err != nil {
			t.Fatal(err)
		}
		defer c2.Close()

		var ps [2]*ipv4.PacketConn
		ps[0] = ipv4.NewPacketConn(c1)
		ps[1] = ipv4.NewPacketConn(c2)
		var mift []*net.Interface

		ift, err := net.Interfaces()
		if err != nil {
			t.Fatal(err)
		}
		for i, ifi := range ift {
			if _, err := nettest.MulticastSource("ip4", &ifi); err != nil {
				continue
			}
			for _, p := range ps {
				if err := p.JoinGroup(&ifi, gaddr); err != nil {
					t.Fatal(err)
				}
			}
			mift = append(mift, &ift[i])
		}
		for _, ifi := range mift {
			for _, p := range ps {
				if err := p.LeaveGroup(ifi, gaddr); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}

func TestUDPPerInterfaceSinglePacketConnWithSingleGroupListener(t *testing.T) {
	switch runtime.GOOS {
	case "fuchsia", "hurd", "js", "nacl", "plan9", "windows":
		t.Skipf("not supported on %s", runtime.GOOS)
	}
	if testing.Short() {
		t.Skip("to avoid external network")
	}

	gaddr := net.IPAddr{IP: net.IPv4(224, 0, 0, 254)} // see RFC 4727
	type ml struct {
		c   *ipv4.PacketConn
		ifi *net.Interface
	}
	var mlt []*ml

	ift, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	port := "0"
	for i, ifi := range ift {
		ip, err := nettest.MulticastSource("ip4", &ifi)
		if err != nil {
			continue
		}
		c, err := net.ListenPacket("udp4", net.JoinHostPort(ip.String(), port)) // unicast address with non-reusable port
		if err != nil {
			// The listen may fail when the serivce is
			// already in use, but it's fine because the
			// purpose of this is not to test the
			// bookkeeping of IP control block inside the
			// kernel.
			t.Log(err)
			continue
		}
		defer c.Close()
		if port == "0" {
			_, port, err = net.SplitHostPort(c.LocalAddr().String())
			if err != nil {
				t.Fatal(err)
			}
		}
		p := ipv4.NewPacketConn(c)
		if err := p.JoinGroup(&ifi, &gaddr); err != nil {
			t.Fatal(err)
		}
		mlt = append(mlt, &ml{p, &ift[i]})
	}
	for _, m := range mlt {
		if err := m.c.LeaveGroup(m.ifi, &gaddr); err != nil {
			t.Fatal(err)
		}
	}
}

func TestIPSingleRawConnWithSingleGroupListener(t *testing.T) {
	switch runtime.GOOS {
	case "fuchsia", "hurd", "js", "nacl", "plan9", "windows":
		t.Skipf("not supported on %s", runtime.GOOS)
	}
	if testing.Short() {
		t.Skip("to avoid external network")
	}
	if !nettest.SupportsRawSocket() {
		t.Skipf("not supported on %s/%s", runtime.GOOS, runtime.GOARCH)
	}

	c, err := net.ListenPacket("ip4:icmp", "0.0.0.0") // wildcard address
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	r, err := ipv4.NewRawConn(c)
	if err != nil {
		t.Fatal(err)
	}
	gaddr := net.IPAddr{IP: net.IPv4(224, 0, 0, 254)} // see RFC 4727
	var mift []*net.Interface

	ift, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	for i, ifi := range ift {
		if _, err := nettest.MulticastSource("ip4", &ifi); err != nil {
			continue
		}
		if err := r.JoinGroup(&ifi, &gaddr); err != nil {
			t.Fatal(err)
		}
		mift = append(mift, &ift[i])
	}
	for _, ifi := range mift {
		if err := r.LeaveGroup(ifi, &gaddr); err != nil {
			t.Fatal(err)
		}
	}
}

func TestIPPerInterfaceSingleRawConnWithSingleGroupListener(t *testing.T) {
	switch runtime.GOOS {
	case "fuchsia", "hurd", "js", "nacl", "plan9", "windows":
		t.Skipf("not supported on %s", runtime.GOOS)
	}
	if testing.Short() {
		t.Skip("to avoid external network")
	}
	if !nettest.SupportsRawSocket() {
		t.Skipf("not supported on %s/%s", runtime.GOOS, runtime.GOARCH)
	}

	gaddr := net.IPAddr{IP: net.IPv4(224, 0, 0, 254)} // see RFC 4727
	type ml struct {
		c   *ipv4.RawConn
		ifi *net.Interface
	}
	var mlt []*ml

	ift, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	for i, ifi := range ift {
		ip, err := nettest.MulticastSource("ip4", &ifi)
		if err != nil {
			continue
		}
		c, err := net.ListenPacket("ip4:253", ip.String()) // unicast address
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		r, err := ipv4.NewRawConn(c)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.JoinGroup(&ifi, &gaddr); err != nil {
			t.Fatal(err)
		}
		mlt = append(mlt, &ml{r, &ift[i]})
	}
	for _, m := range mlt {
		if err := m.c.LeaveGroup(m.ifi, &gaddr); err != nil {
			t.Fatal(err)
		}
	}
}
