/*
 *
 * Copyright 2017 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package dns

import (
	"fmt"
	"net"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/test/leakcheck"
)

func TestMain(m *testing.M) {
	cleanup := replaceNetFunc()
	code := m.Run()
	cleanup()
	os.Exit(code)
}

const (
	txtBytesLimit = 255
)

type testClientConn struct {
	target string
	m1     sync.Mutex
	addrs  []resolver.Address
	a      int
	m2     sync.Mutex
	sc     string
	s      int
}

func (t *testClientConn) NewAddress(addresses []resolver.Address) {
	t.m1.Lock()
	defer t.m1.Unlock()
	t.addrs = addresses
	t.a++
}

func (t *testClientConn) getAddress() ([]resolver.Address, int) {
	t.m1.Lock()
	defer t.m1.Unlock()
	return t.addrs, t.a
}

func (t *testClientConn) NewServiceConfig(serviceConfig string) {
	t.m2.Lock()
	defer t.m2.Unlock()
	t.sc = serviceConfig
	t.s++
}

func (t *testClientConn) getSc() (string, int) {
	t.m2.Lock()
	defer t.m2.Unlock()
	return t.sc, t.s
}

var hostLookupTbl = struct {
	sync.Mutex
	tbl map[string][]string
}{
	tbl: map[string][]string{
		"foo.bar.com":          {"1.2.3.4", "5.6.7.8"},
		"ipv4.single.fake":     {"1.2.3.4"},
		"srv.ipv4.single.fake": {"2.4.6.8"},
		"ipv4.multi.fake":      {"1.2.3.4", "5.6.7.8", "9.10.11.12"},
		"ipv6.single.fake":     {"2607:f8b0:400a:801::1001"},
		"ipv6.multi.fake":      {"2607:f8b0:400a:801::1001", "2607:f8b0:400a:801::1002", "2607:f8b0:400a:801::1003"},
	},
}

func hostLookup(host string) ([]string, error) {
	hostLookupTbl.Lock()
	defer hostLookupTbl.Unlock()
	if addrs, cnt := hostLookupTbl.tbl[host]; cnt {
		return addrs, nil
	}
	return nil, fmt.Errorf("failed to lookup host:%s resolution in hostLookupTbl", host)
}

var srvLookupTbl = struct {
	sync.Mutex
	tbl map[string][]*net.SRV
}{
	tbl: map[string][]*net.SRV{
		"_grpclb._tcp.srv.ipv4.single.fake": {&net.SRV{Target: "ipv4.single.fake", Port: 1234}},
		"_grpclb._tcp.srv.ipv4.multi.fake":  {&net.SRV{Target: "ipv4.multi.fake", Port: 1234}},
		"_grpclb._tcp.srv.ipv6.single.fake": {&net.SRV{Target: "ipv6.single.fake", Port: 1234}},
		"_grpclb._tcp.srv.ipv6.multi.fake":  {&net.SRV{Target: "ipv6.multi.fake", Port: 1234}},
	},
}

func srvLookup(service, proto, name string) (string, []*net.SRV, error) {
	cname := "_" + service + "._" + proto + "." + name
	srvLookupTbl.Lock()
	defer srvLookupTbl.Unlock()
	if srvs, cnt := srvLookupTbl.tbl[cname]; cnt {
		return cname, srvs, nil
	}
	return "", nil, fmt.Errorf("failed to lookup srv record for %s in srvLookupTbl", cname)
}

// div divides a byte slice into a slice of strings, each of which is of maximum
// 255 bytes length, which is the length limit per TXT record in DNS.
func div(b []byte) []string {
	var r []string
	for i := 0; i < len(b); i += txtBytesLimit {
		if i+txtBytesLimit > len(b) {
			r = append(r, string(b[i:]))
		} else {
			r = append(r, string(b[i:i+txtBytesLimit]))
		}
	}
	return r
}

// scfs contains an array of service config file string in JSON format.
// Notes about the scfs contents and usage:
// scfs contains 4 service config file JSON strings for testing. Inside each
// service config file, there are multiple choices. scfs[0:3] each contains 5
// choices, and first 3 choices are nonmatching choices based on canarying rule,
// while the last two are matched choices. scfs[3] only contains 3 choices, and
// all of them are nonmatching based on canarying rule. For each of scfs[0:3],
// the eventually returned service config, which is from the first of the two
// matched choices, is stored in the corresponding scs element (e.g.
// scfs[0]->scs[0]). scfs and scs elements are used in pair to test the dns
// resolver functionality, with scfs as the input and scs used for validation of
// the output. For scfs[3], it corresponds to empty service config, since there
// isn't a matched choice.
var scfs = []string{
	`[
	{
		"clientLanguage": [
			"CPP",
			"JAVA"
		],
		"serviceConfig": {
			"loadBalancingPolicy": "grpclb",
			"methodConfig": [
				{
					"name": [
						{
							"service": "all"
						}
					],
					"timeout": "1s"
				}
			]
		}
	},
	{
		"percentage": 0,
		"serviceConfig": {
			"loadBalancingPolicy": "grpclb",
			"methodConfig": [
				{
					"name": [
						{
							"service": "all"
						}
					],
					"timeout": "1s"
				}
			]
		}
	},
	{
		"clientHostName": [
			"localhost"
		],
		"serviceConfig": {
			"loadBalancingPolicy": "grpclb",
			"methodConfig": [
				{
					"name": [
						{
							"service": "all"
						}
					],
					"timeout": "1s"
				}
			]
		}
	},
	{
		"clientLanguage": [
			"GO"
		],
		"percentage": 100,
		"serviceConfig": {
			"methodConfig": [
				{
					"name": [
						{
							"method": "bar"
						}
					],
					"maxRequestMessageBytes": 1024,
					"maxResponseMessageBytes": 1024
				}
			]
		}
	},
	{
		"serviceConfig": {
			"loadBalancingPolicy": "round_robin",
			"methodConfig": [
				{
					"name": [
						{
							"service": "foo",
							"method": "bar"
						}
					],
					"waitForReady": true
				}
			]
		}
	}
]`,
	`[
	{
		"clientLanguage": [
			"CPP",
			"JAVA"
		],
		"serviceConfig": {
			"loadBalancingPolicy": "grpclb",
			"methodConfig": [
				{
					"name": [
						{
							"service": "all"
						}
					],
					"timeout": "1s"
				}
			]
		}
	},
	{
		"percentage": 0,
		"serviceConfig": {
			"loadBalancingPolicy": "grpclb",
			"methodConfig": [
				{
					"name": [
						{
							"service": "all"
						}
					],
					"timeout": "1s"
				}
			]
		}
	},
	{
		"clientHostName": [
			"localhost"
		],
		"serviceConfig": {
			"loadBalancingPolicy": "grpclb",
			"methodConfig": [
				{
					"name": [
						{
							"service": "all"
						}
					],
					"timeout": "1s"
				}
			]
		}
	},
	{
		"clientLanguage": [
			"GO"
		],
		"percentage": 100,
		"serviceConfig": {
			"methodConfig": [
				{
					"name": [
						{
							"service": "foo",
							"method": "bar"
						}
					],
					"waitForReady": true,
					"timeout": "1s",
					"maxRequestMessageBytes": 1024,
					"maxResponseMessageBytes": 1024
				}
			]
		}
	},
	{
		"serviceConfig": {
			"loadBalancingPolicy": "round_robin",
			"methodConfig": [
				{
					"name": [
						{
							"service": "foo",
							"method": "bar"
						}
					],
					"waitForReady": true
				}
			]
		}
	}
]`,
	`[
	{
		"clientLanguage": [
			"CPP",
			"JAVA"
		],
		"serviceConfig": {
			"loadBalancingPolicy": "grpclb",
			"methodConfig": [
				{
					"name": [
						{
							"service": "all"
						}
					],
					"timeout": "1s"
				}
			]
		}
	},
	{
		"percentage": 0,
		"serviceConfig": {
			"loadBalancingPolicy": "grpclb",
			"methodConfig": [
				{
					"name": [
						{
							"service": "all"
						}
					],
					"timeout": "1s"
				}
			]
		}
	},
	{
		"clientHostName": [
			"localhost"
		],
		"serviceConfig": {
			"loadBalancingPolicy": "grpclb",
			"methodConfig": [
				{
					"name": [
						{
							"service": "all"
						}
					],
					"timeout": "1s"
				}
			]
		}
	},
	{
		"clientLanguage": [
			"GO"
		],
		"percentage": 100,
		"serviceConfig": {
			"loadBalancingPolicy": "round_robin",
			"methodConfig": [
				{
					"name": [
						{
							"service": "foo"
						}
					],
					"waitForReady": true,
					"timeout": "1s"
				},
				{
					"name": [
						{
							"service": "bar"
						}
					],
					"waitForReady": false
				}
			]
		}
	},
	{
		"serviceConfig": {
			"loadBalancingPolicy": "round_robin",
			"methodConfig": [
				{
					"name": [
						{
							"service": "foo",
							"method": "bar"
						}
					],
					"waitForReady": true
				}
			]
		}
	}
]`,
	`[
	{
		"clientLanguage": [
			"CPP",
			"JAVA"
		],
		"serviceConfig": {
			"loadBalancingPolicy": "grpclb",
			"methodConfig": [
				{
					"name": [
						{
							"service": "all"
						}
					],
					"timeout": "1s"
				}
			]
		}
	},
	{
		"percentage": 0,
		"serviceConfig": {
			"loadBalancingPolicy": "grpclb",
			"methodConfig": [
				{
					"name": [
						{
							"service": "all"
						}
					],
					"timeout": "1s"
				}
			]
		}
	},
	{
		"clientHostName": [
			"localhost"
		],
		"serviceConfig": {
			"loadBalancingPolicy": "grpclb",
			"methodConfig": [
				{
					"name": [
						{
							"service": "all"
						}
					],
					"timeout": "1s"
				}
			]
		}
	}
]`,
}

// scs contains an array of service config string in JSON format.
var scs = []string{
	`{
			"methodConfig": [
				{
					"name": [
						{
							"method": "bar"
						}
					],
					"maxRequestMessageBytes": 1024,
					"maxResponseMessageBytes": 1024
				}
			]
		}`,
	`{
			"methodConfig": [
				{
					"name": [
						{
							"service": "foo",
							"method": "bar"
						}
					],
					"waitForReady": true,
					"timeout": "1s",
					"maxRequestMessageBytes": 1024,
					"maxResponseMessageBytes": 1024
				}
			]
		}`,
	`{
			"loadBalancingPolicy": "round_robin",
			"methodConfig": [
				{
					"name": [
						{
							"service": "foo"
						}
					],
					"waitForReady": true,
					"timeout": "1s"
				},
				{
					"name": [
						{
							"service": "bar"
						}
					],
					"waitForReady": false
				}
			]
		}`,
}

// scLookupTbl is a set, which contains targets that have service config. Target
// not in this set should not have service config.
var scLookupTbl = map[string]bool{
	"foo.bar.com":          true,
	"srv.ipv4.single.fake": true,
	"srv.ipv4.multi.fake":  true,
	"no.attribute":         true,
}

// generateSCF generates a slice of strings (aggregately representing a single
// service config file) for the input name, which mocks the result from a real
// DNS TXT record lookup.
func generateSCF(name string) []string {
	var b []byte
	switch name {
	case "foo.bar.com":
		b = []byte(scfs[0])
	case "srv.ipv4.single.fake":
		b = []byte(scfs[1])
	case "srv.ipv4.multi.fake":
		b = []byte(scfs[2])
	default:
		b = []byte(scfs[3])
	}
	if name == "no.attribute" {
		return div(b)
	}
	return div(append([]byte(txtAttribute), b...))
}

// generateSC returns a service config string in JSON format for the input name.
func generateSC(name string) string {
	_, cnt := scLookupTbl[name]
	if !cnt || name == "no.attribute" {
		return ""
	}
	switch name {
	case "foo.bar.com":
		return scs[0]
	case "srv.ipv4.single.fake":
		return scs[1]
	case "srv.ipv4.multi.fake":
		return scs[2]
	default:
		return ""
	}
}

var txtLookupTbl = struct {
	sync.Mutex
	tbl map[string][]string
}{
	tbl: map[string][]string{
		"foo.bar.com":          generateSCF("foo.bar.com"),
		"srv.ipv4.single.fake": generateSCF("srv.ipv4.single.fake"),
		"srv.ipv4.multi.fake":  generateSCF("srv.ipv4.multi.fake"),
		"srv.ipv6.single.fake": generateSCF("srv.ipv6.single.fake"),
		"srv.ipv6.multi.fake":  generateSCF("srv.ipv6.multi.fake"),
		"no.attribute":         generateSCF("no.attribute"),
	},
}

func txtLookup(host string) ([]string, error) {
	txtLookupTbl.Lock()
	defer txtLookupTbl.Unlock()
	if scs, cnt := txtLookupTbl.tbl[host]; cnt {
		return scs, nil
	}
	return nil, fmt.Errorf("failed to lookup TXT:%s resolution in txtLookupTbl", host)
}

func TestResolve(t *testing.T) {
	testDNSResolver(t)
	testDNSResolveNow(t)
	testIPResolver(t)
}

func testDNSResolver(t *testing.T) {
	defer leakcheck.Check(t)
	tests := []struct {
		target   string
		addrWant []resolver.Address
		scWant   string
	}{
		{
			"foo.bar.com",
			[]resolver.Address{{Addr: "1.2.3.4" + colonDefaultPort}, {Addr: "5.6.7.8" + colonDefaultPort}},
			generateSC("foo.bar.com"),
		},
		{
			"foo.bar.com:1234",
			[]resolver.Address{{Addr: "1.2.3.4:1234"}, {Addr: "5.6.7.8:1234"}},
			generateSC("foo.bar.com"),
		},
		{
			"srv.ipv4.single.fake",
			[]resolver.Address{{Addr: "1.2.3.4:1234", Type: resolver.GRPCLB, ServerName: "ipv4.single.fake"}, {Addr: "2.4.6.8" + colonDefaultPort}},
			generateSC("srv.ipv4.single.fake"),
		},
		{
			"srv.ipv4.multi.fake",
			[]resolver.Address{
				{Addr: "1.2.3.4:1234", Type: resolver.GRPCLB, ServerName: "ipv4.multi.fake"},
				{Addr: "5.6.7.8:1234", Type: resolver.GRPCLB, ServerName: "ipv4.multi.fake"},
				{Addr: "9.10.11.12:1234", Type: resolver.GRPCLB, ServerName: "ipv4.multi.fake"},
			},
			generateSC("srv.ipv4.multi.fake"),
		},
		{
			"srv.ipv6.single.fake",
			[]resolver.Address{{Addr: "[2607:f8b0:400a:801::1001]:1234", Type: resolver.GRPCLB, ServerName: "ipv6.single.fake"}},
			generateSC("srv.ipv6.single.fake"),
		},
		{
			"srv.ipv6.multi.fake",
			[]resolver.Address{
				{Addr: "[2607:f8b0:400a:801::1001]:1234", Type: resolver.GRPCLB, ServerName: "ipv6.multi.fake"},
				{Addr: "[2607:f8b0:400a:801::1002]:1234", Type: resolver.GRPCLB, ServerName: "ipv6.multi.fake"},
				{Addr: "[2607:f8b0:400a:801::1003]:1234", Type: resolver.GRPCLB, ServerName: "ipv6.multi.fake"},
			},
			generateSC("srv.ipv6.multi.fake"),
		},
		{
			"no.attribute",
			nil,
			generateSC("no.attribute"),
		},
	}

	for _, a := range tests {
		b := NewBuilder()
		cc := &testClientConn{target: a.target}
		r, err := b.Build(resolver.Target{Endpoint: a.target}, cc, resolver.BuildOption{})
		if err != nil {
			t.Fatalf("%v\n", err)
		}
		var addrs []resolver.Address
		var cnt int
		for {
			addrs, cnt = cc.getAddress()
			if cnt > 0 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		var sc string
		for {
			sc, cnt = cc.getSc()
			if cnt > 0 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		if !reflect.DeepEqual(a.addrWant, addrs) {
			t.Errorf("Resolved addresses of target: %q = %+v, want %+v\n", a.target, addrs, a.addrWant)
		}
		if !reflect.DeepEqual(a.scWant, sc) {
			t.Errorf("Resolved service config of target: %q = %+v, want %+v\n", a.target, sc, a.scWant)
		}
		r.Close()
	}
}

func mutateTbl(target string) func() {
	hostLookupTbl.Lock()
	oldHostTblEntry := hostLookupTbl.tbl[target]
	hostLookupTbl.tbl[target] = hostLookupTbl.tbl[target][:len(oldHostTblEntry)-1]
	hostLookupTbl.Unlock()
	txtLookupTbl.Lock()
	oldTxtTblEntry := txtLookupTbl.tbl[target]
	txtLookupTbl.tbl[target] = []string{""}
	txtLookupTbl.Unlock()

	return func() {
		hostLookupTbl.Lock()
		hostLookupTbl.tbl[target] = oldHostTblEntry
		hostLookupTbl.Unlock()
		txtLookupTbl.Lock()
		txtLookupTbl.tbl[target] = oldTxtTblEntry
		txtLookupTbl.Unlock()
	}
}

func testDNSResolveNow(t *testing.T) {
	defer leakcheck.Check(t)
	tests := []struct {
		target   string
		addrWant []resolver.Address
		addrNext []resolver.Address
		scWant   string
		scNext   string
	}{
		{
			"foo.bar.com",
			[]resolver.Address{{Addr: "1.2.3.4" + colonDefaultPort}, {Addr: "5.6.7.8" + colonDefaultPort}},
			[]resolver.Address{{Addr: "1.2.3.4" + colonDefaultPort}},
			generateSC("foo.bar.com"),
			"",
		},
	}

	for _, a := range tests {
		b := NewBuilder()
		cc := &testClientConn{target: a.target}
		r, err := b.Build(resolver.Target{Endpoint: a.target}, cc, resolver.BuildOption{})
		if err != nil {
			t.Fatalf("%v\n", err)
		}
		var addrs []resolver.Address
		var cnt int
		for {
			addrs, cnt = cc.getAddress()
			if cnt > 0 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		var sc string
		for {
			sc, cnt = cc.getSc()
			if cnt > 0 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		if !reflect.DeepEqual(a.addrWant, addrs) {
			t.Errorf("Resolved addresses of target: %q = %+v, want %+v\n", a.target, addrs, a.addrWant)
		}
		if !reflect.DeepEqual(a.scWant, sc) {
			t.Errorf("Resolved service config of target: %q = %+v, want %+v\n", a.target, sc, a.scWant)
		}
		revertTbl := mutateTbl(a.target)
		r.ResolveNow(resolver.ResolveNowOption{})
		for {
			addrs, cnt = cc.getAddress()
			if cnt == 2 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		for {
			sc, cnt = cc.getSc()
			if cnt == 2 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		if !reflect.DeepEqual(a.addrNext, addrs) {
			t.Errorf("Resolved addresses of target: %q = %+v, want %+v\n", a.target, addrs, a.addrNext)
		}
		if !reflect.DeepEqual(a.scNext, sc) {
			t.Errorf("Resolved service config of target: %q = %+v, want %+v\n", a.target, sc, a.scNext)
		}
		revertTbl()
		r.Close()
	}
}

const colonDefaultPort = ":" + defaultPort

func testIPResolver(t *testing.T) {
	defer leakcheck.Check(t)
	tests := []struct {
		target string
		want   []resolver.Address
	}{
		{"127.0.0.1", []resolver.Address{{Addr: "127.0.0.1" + colonDefaultPort}}},
		{"127.0.0.1:12345", []resolver.Address{{Addr: "127.0.0.1:12345"}}},
		{"::1", []resolver.Address{{Addr: "[::1]" + colonDefaultPort}}},
		{"[::1]:12345", []resolver.Address{{Addr: "[::1]:12345"}}},
		{"[::1]:", []resolver.Address{{Addr: "[::1]:443"}}},
		{"2001:db8:85a3::8a2e:370:7334", []resolver.Address{{Addr: "[2001:db8:85a3::8a2e:370:7334]" + colonDefaultPort}}},
		{"[2001:db8:85a3::8a2e:370:7334]", []resolver.Address{{Addr: "[2001:db8:85a3::8a2e:370:7334]" + colonDefaultPort}}},
		{"[2001:db8:85a3::8a2e:370:7334]:12345", []resolver.Address{{Addr: "[2001:db8:85a3::8a2e:370:7334]:12345"}}},
		{"[2001:db8::1]:http", []resolver.Address{{Addr: "[2001:db8::1]:http"}}},
		// TODO(yuxuanli): zone support?
	}

	for _, v := range tests {
		b := NewBuilder()
		cc := &testClientConn{target: v.target}
		r, err := b.Build(resolver.Target{Endpoint: v.target}, cc, resolver.BuildOption{})
		if err != nil {
			t.Fatalf("%v\n", err)
		}
		var addrs []resolver.Address
		var cnt int
		for {
			addrs, cnt = cc.getAddress()
			if cnt > 0 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		if !reflect.DeepEqual(v.want, addrs) {
			t.Errorf("Resolved addresses of target: %q = %+v, want %+v\n", v.target, addrs, v.want)
		}
		r.ResolveNow(resolver.ResolveNowOption{})
		for {
			addrs, cnt = cc.getAddress()
			if cnt == 2 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		if !reflect.DeepEqual(v.want, addrs) {
			t.Errorf("Resolved addresses of target: %q = %+v, want %+v\n", v.target, addrs, v.want)
		}
		r.Close()
	}
}

func TestResolveFunc(t *testing.T) {
	defer leakcheck.Check(t)
	tests := []struct {
		addr string
		want error
	}{
		// TODO(yuxuanli): More false cases?
		{"www.google.com", nil},
		{"foo.bar:12345", nil},
		{"127.0.0.1", nil},
		{"127.0.0.1:12345", nil},
		{"[::1]:80", nil},
		{"[2001:db8:a0b:12f0::1]:21", nil},
		{":80", nil},
		{"127.0.0...1:12345", nil},
		{"[fe80::1%lo0]:80", nil},
		{"golang.org:http", nil},
		{"[2001:db8::1]:http", nil},
		{":", nil},
		{"", errMissingAddr},
		{"[2001:db8:a0b:12f0::1", errForInvalidTarget},
	}

	b := NewBuilder()
	for _, v := range tests {
		cc := &testClientConn{target: v.addr}
		r, err := b.Build(resolver.Target{Endpoint: v.addr}, cc, resolver.BuildOption{})
		if err == nil {
			r.Close()
		}
		if !reflect.DeepEqual(err, v.want) {
			t.Errorf("Build(%q, cc, resolver.BuildOption{}) = %v, want %v", v.addr, err, v.want)
		}
	}
}

func TestDisableServiceConfig(t *testing.T) {
	defer leakcheck.Check(t)
	tests := []struct {
		target               string
		scWant               string
		disableServiceConfig bool
	}{
		{
			"foo.bar.com",
			generateSC("foo.bar.com"),
			false,
		},
		{
			"foo.bar.com",
			"",
			true,
		},
	}

	for _, a := range tests {
		b := NewBuilder()
		cc := &testClientConn{target: a.target}
		r, err := b.Build(resolver.Target{Endpoint: a.target}, cc, resolver.BuildOption{DisableServiceConfig: a.disableServiceConfig})
		if err != nil {
			t.Fatalf("%v\n", err)
		}
		var cnt int
		var sc string
		for {
			sc, cnt = cc.getSc()
			if cnt > 0 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		if !reflect.DeepEqual(a.scWant, sc) {
			t.Errorf("Resolved service config of target: %q = %+v, want %+v\n", a.target, sc, a.scWant)
		}
		r.Close()
	}
}
