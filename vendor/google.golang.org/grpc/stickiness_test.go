/*
 *
 * Copyright 2018 gRPC authors.
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

package grpc

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/test/leakcheck"
)

func TestStickyKeyFromContext(t *testing.T) {
	for _, test := range []struct {
		org, add []string
		mdKey    string
		wantStr  string
		wantBool bool
	}{
		{[]string{}, []string{}, "", "", false},
		{[]string{"k1", "v1"}, []string{"k2", "v2"}, "k", "", false},

		{[]string{"k", "v"}, []string{}, "k", "v", true},
		{[]string{}, []string{"k", "v"}, "k", "v", true},
		{[]string{"k1", "v1"}, []string{"k2", "v2"}, "k1", "v1", true},
		{[]string{"k1", "v1"}, []string{"k2", "v2"}, "k2", "v2", true},
	} {
		ctx := context.Background()
		if len(test.org) > 0 {
			ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(test.org...))
		}
		if len(test.add) > 0 {
			ctx = metadata.AppendToOutgoingContext(ctx, test.add...)
		}
		got, ok := stickyKeyFromContext(ctx, test.mdKey)
		if got != test.wantStr || ok != test.wantBool {
			t.Errorf("test: %+v, got: %q, %v, want: %q, %v\n", test, got, ok, test.wantStr, test.wantBool)
		}
	}
}

func TestStickinessServiceConfig(t *testing.T) {
	envConfigStickinessOn = true
	defer func() { envConfigStickinessOn = false }()
	defer leakcheck.Check(t)
	r, rcleanup := manual.GenerateAndRegisterManualResolver()
	defer rcleanup()

	cc, err := Dial(r.Scheme()+":///test.server", WithInsecure())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cc.Close()

	const testInput = "testStickinessKey"
	wantStr := strings.ToLower(testInput)

	r.NewServiceConfig(fmt.Sprintf(`{"stickinessMetadataKey": "%v"}`, testInput)) // ToLower() will be applied to the input.

	for i := 0; i < 1000; i++ {
		if key := cc.blockingpicker.getStickinessMDKey(); key == wantStr {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("cc.blockingpicker.stickiness.stickinessMDKey failed to change to %v within one second", wantStr)
}

func TestStickinessEnd2end(t *testing.T) {
	envConfigStickinessOn = true
	defer func() { envConfigStickinessOn = false }()
	defer leakcheck.Check(t)
	r, rcleanup := manual.GenerateAndRegisterManualResolver()
	defer rcleanup()

	numServers := 2
	servers, _, scleanup := startServers(t, numServers, math.MaxInt32)
	defer scleanup()

	cc, err := Dial(r.Scheme()+":///test.server",
		WithInsecure(), WithCodec(testCodec{}), WithBalancerName(roundrobin.Name))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cc.Close()
	req := "port"
	var reply string
	r.NewAddress([]resolver.Address{{Addr: servers[0].addr}, {Addr: servers[1].addr}})

	var (
		i      int
		picked []int
	)

	// Check that each backend will be picked for at least 3 times.
	picked = make([]int, 2, 2)
	for i = 0; i < 1000; i++ {
		if err = Invoke(context.Background(), "/foo/bar", &req, &reply, cc); err != nil {
			if errorDesc(err) == servers[0].port {
				picked[0]++
			} else if errorDesc(err) == servers[1].port {
				picked[1]++
			}
		}
		if picked[0] >= 3 && picked[1] >= 3 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if i >= 1000 {
		t.Fatalf("When doing roundrobin, addr1 was picked %v times, addr2 was picked %v times", picked[0], picked[1])
	}

	r.NewServiceConfig(fmt.Sprintf(`{"stickinessMetadataKey": "sessionid"}`))

	// Should still be roundrobin.
	picked = make([]int, 2, 2)
	for i = 0; i < 1000; i++ {
		if err = Invoke(context.Background(), "/foo/bar", &req, &reply, cc); err != nil {
			if errorDesc(err) == servers[0].port {
				picked[0]++
			} else if errorDesc(err) == servers[1].port {
				picked[1]++
			}
		}
		if picked[0] >= 3 && picked[1] >= 3 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if i >= 1000 {
		t.Fatalf("When doing roundrobin, addr1 was picked %v times, addr2 was picked %v times", picked[0], picked[1])
	}

	// Do sticky call, only one backend will be picked.
	picked = make([]int, 2, 2)
	for i = 0; i < 100; i++ {
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("sessionid", "1"))
		if err = Invoke(ctx, "/foo/bar", &req, &reply, cc); err != nil {
			if errorDesc(err) == servers[0].port {
				picked[0]++
			} else if errorDesc(err) == servers[1].port {
				picked[1]++
			}
		}
		time.Sleep(time.Millisecond)
	}

	if (picked[0] != 0) == (picked[1] != 0) {
		t.Fatalf("When doing sticky RPC, addr1 was picked %v times, addr2 was picked %v times, want at least one of them to be 0", picked[0], picked[1])
	}

}

// Changing stickinessMDKey in service config will clear the sticky map.
func TestStickinessChangeMDKey(t *testing.T) {
	envConfigStickinessOn = true
	defer func() { envConfigStickinessOn = false }()
	defer leakcheck.Check(t)
	r, rcleanup := manual.GenerateAndRegisterManualResolver()
	defer rcleanup()

	numServers := 2
	servers, _, scleanup := startServers(t, numServers, math.MaxInt32)
	defer scleanup()

	cc, err := Dial(r.Scheme()+":///test.server", WithInsecure(), WithCodec(testCodec{}))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cc.Close()
	req := "port"
	var reply string
	r.NewAddress([]resolver.Address{{Addr: servers[0].addr}, {Addr: servers[1].addr}})

	r.NewServiceConfig(fmt.Sprintf(`{"stickinessMetadataKey": "sessionid"}`))

	// Do sticky call, only one backend will be picked, and there will be one
	// entry in stickiness map.
	for i := 0; i < 100; i++ {
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("sessionid", "1"))
		Invoke(ctx, "/foo/bar", &req, &reply, cc)
		time.Sleep(time.Millisecond)
	}

	cc.blockingpicker.stickiness.mu.Lock()
	mapLen := len(cc.blockingpicker.stickiness.store)
	cc.blockingpicker.stickiness.mu.Unlock()
	if mapLen != 1 {
		t.Fatalf("length of stickiness map is %v, want 1", mapLen)
	}

	r.NewServiceConfig(fmt.Sprintf(`{"stickinessMetadataKey": "sessionidnew"}`))

	var i int
	for i = 0; i < 1000; i++ {
		cc.blockingpicker.stickiness.mu.Lock()
		mapLen = len(cc.blockingpicker.stickiness.store)
		cc.blockingpicker.stickiness.mu.Unlock()
		if mapLen == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if i >= 1000 {
		t.Fatalf("After 1 second, length of stickiness map is %v, want 0", mapLen)
	}
}

// Switching balancer will clear the sticky map.
func TestStickinessSwitchingBalancer(t *testing.T) {
	envConfigStickinessOn = true
	defer func() { envConfigStickinessOn = false }()
	defer leakcheck.Check(t)
	r, rcleanup := manual.GenerateAndRegisterManualResolver()
	defer rcleanup()

	numServers := 2
	servers, _, scleanup := startServers(t, numServers, math.MaxInt32)
	defer scleanup()

	cc, err := Dial(r.Scheme()+":///test.server", WithInsecure(), WithCodec(testCodec{}))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cc.Close()
	req := "port"
	var reply string
	r.NewAddress([]resolver.Address{{Addr: servers[0].addr}, {Addr: servers[1].addr}})

	r.NewServiceConfig(fmt.Sprintf(`{"stickinessMetadataKey": "sessionid"}`))

	// Do sticky call, only one backend will be picked, and there will be one
	// entry in stickiness map.
	for i := 0; i < 100; i++ {
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("sessionid", "1"))
		Invoke(ctx, "/foo/bar", &req, &reply, cc)
		time.Sleep(time.Millisecond)
	}

	cc.blockingpicker.stickiness.mu.Lock()
	mapLen := len(cc.blockingpicker.stickiness.store)
	cc.blockingpicker.stickiness.mu.Unlock()
	if mapLen != 1 {
		t.Fatalf("length of stickiness map is %v, want 1", mapLen)
	}

	cc.mu.Lock()
	cc.switchBalancer("round_robin")
	cc.mu.Unlock()

	var i int
	for i = 0; i < 1000; i++ {
		cc.blockingpicker.stickiness.mu.Lock()
		mapLen = len(cc.blockingpicker.stickiness.store)
		cc.blockingpicker.stickiness.mu.Unlock()
		if mapLen == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if i >= 1000 {
		t.Fatalf("After 1 second, length of stickiness map is %v, want 0", mapLen)
	}
}
