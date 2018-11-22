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

package grpc

import (
	"fmt"
	"math"
	"testing"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc/balancer/roundrobin"
	_ "google.golang.org/grpc/grpclog/glogger"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/test/leakcheck"
)

func checkPickFirst(cc *ClientConn, servers []*server) error {
	var (
		req   = "port"
		reply string
		err   error
	)
	connected := false
	for i := 0; i < 5000; i++ {
		if err = cc.Invoke(context.Background(), "/foo/bar", &req, &reply); errorDesc(err) == servers[0].port {
			if connected {
				// connected is set to false if peer is not server[0]. So if
				// connected is true here, this is the second time we saw
				// server[0] in a row. Break because pickfirst is in effect.
				break
			}
			connected = true
		} else {
			connected = false
		}
		time.Sleep(time.Millisecond)
	}
	if !connected {
		return fmt.Errorf("pickfirst is not in effect after 5 second, EmptyCall() = _, %v, want _, %v", err, servers[0].port)
	}
	// The following RPCs should all succeed with the first server.
	for i := 0; i < 3; i++ {
		err = cc.Invoke(context.Background(), "/foo/bar", &req, &reply)
		if errorDesc(err) != servers[0].port {
			return fmt.Errorf("Index %d: want peer %v, got peer %v", i, servers[0].port, err)
		}
	}
	return nil
}

func checkRoundRobin(cc *ClientConn, servers []*server) error {
	var (
		req   = "port"
		reply string
		err   error
	)

	// Make sure connections to all servers are up.
	for i := 0; i < 2; i++ {
		// Do this check twice, otherwise the first RPC's transport may still be
		// picked by the closing pickfirst balancer, and the test becomes flaky.
		for _, s := range servers {
			var up bool
			for i := 0; i < 5000; i++ {
				if err = cc.Invoke(context.Background(), "/foo/bar", &req, &reply); errorDesc(err) == s.port {
					up = true
					break
				}
				time.Sleep(time.Millisecond)
			}
			if !up {
				return fmt.Errorf("server %v is not up within 5 second", s.port)
			}
		}
	}

	serverCount := len(servers)
	for i := 0; i < 3*serverCount; i++ {
		err = cc.Invoke(context.Background(), "/foo/bar", &req, &reply)
		if errorDesc(err) != servers[i%serverCount].port {
			return fmt.Errorf("Index %d: want peer %v, got peer %v", i, servers[i%serverCount].port, err)
		}
	}
	return nil
}

func TestSwitchBalancer(t *testing.T) {
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
	r.NewAddress([]resolver.Address{{Addr: servers[0].addr}, {Addr: servers[1].addr}})
	// The default balancer is pickfirst.
	if err := checkPickFirst(cc, servers); err != nil {
		t.Fatalf("check pickfirst returned non-nil error: %v", err)
	}
	// Switch to roundrobin.
	cc.handleServiceConfig(`{"loadBalancingPolicy": "round_robin"}`)
	if err := checkRoundRobin(cc, servers); err != nil {
		t.Fatalf("check roundrobin returned non-nil error: %v", err)
	}
	// Switch to pickfirst.
	cc.handleServiceConfig(`{"loadBalancingPolicy": "pick_first"}`)
	if err := checkPickFirst(cc, servers); err != nil {
		t.Fatalf("check pickfirst returned non-nil error: %v", err)
	}
}

// Test that balancer specified by dial option will not be overridden.
func TestBalancerDialOption(t *testing.T) {
	defer leakcheck.Check(t)
	r, rcleanup := manual.GenerateAndRegisterManualResolver()
	defer rcleanup()

	numServers := 2
	servers, _, scleanup := startServers(t, numServers, math.MaxInt32)
	defer scleanup()

	cc, err := Dial(r.Scheme()+":///test.server", WithInsecure(), WithCodec(testCodec{}), WithBalancerName(roundrobin.Name))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cc.Close()
	r.NewAddress([]resolver.Address{{Addr: servers[0].addr}, {Addr: servers[1].addr}})
	// The init balancer is roundrobin.
	if err := checkRoundRobin(cc, servers); err != nil {
		t.Fatalf("check roundrobin returned non-nil error: %v", err)
	}
	// Switch to pickfirst.
	cc.handleServiceConfig(`{"loadBalancingPolicy": "pick_first"}`)
	// Balancer is still roundrobin.
	if err := checkRoundRobin(cc, servers); err != nil {
		t.Fatalf("check roundrobin returned non-nil error: %v", err)
	}
}

// First addr update contains grpclb.
func TestSwitchBalancerGRPCLBFirst(t *testing.T) {
	defer leakcheck.Check(t)
	r, rcleanup := manual.GenerateAndRegisterManualResolver()
	defer rcleanup()

	cc, err := Dial(r.Scheme()+":///test.server", WithInsecure(), WithCodec(testCodec{}))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cc.Close()

	// ClientConn will switch balancer to grpclb when receives an address of
	// type GRPCLB.
	r.NewAddress([]resolver.Address{{Addr: "backend"}, {Addr: "grpclb", Type: resolver.GRPCLB}})
	var isGRPCLB bool
	for i := 0; i < 5000; i++ {
		cc.mu.Lock()
		isGRPCLB = cc.curBalancerName == "grpclb"
		cc.mu.Unlock()
		if isGRPCLB {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !isGRPCLB {
		t.Fatalf("after 5 second, cc.balancer is of type %v, not grpclb", cc.curBalancerName)
	}

	// New update containing new backend and new grpclb. Should not switch
	// balancer.
	r.NewAddress([]resolver.Address{{Addr: "backend2"}, {Addr: "grpclb2", Type: resolver.GRPCLB}})
	for i := 0; i < 200; i++ {
		cc.mu.Lock()
		isGRPCLB = cc.curBalancerName == "grpclb"
		cc.mu.Unlock()
		if !isGRPCLB {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !isGRPCLB {
		t.Fatalf("within 200 ms, cc.balancer switched to !grpclb, want grpclb")
	}

	var isPickFirst bool
	// Switch balancer to pickfirst.
	r.NewAddress([]resolver.Address{{Addr: "backend"}})
	for i := 0; i < 5000; i++ {
		cc.mu.Lock()
		isPickFirst = cc.curBalancerName == PickFirstBalancerName
		cc.mu.Unlock()
		if isPickFirst {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !isPickFirst {
		t.Fatalf("after 5 second, cc.balancer is of type %v, not pick_first", cc.curBalancerName)
	}
}

// First addr update does not contain grpclb.
func TestSwitchBalancerGRPCLBSecond(t *testing.T) {
	defer leakcheck.Check(t)
	r, rcleanup := manual.GenerateAndRegisterManualResolver()
	defer rcleanup()

	cc, err := Dial(r.Scheme()+":///test.server", WithInsecure(), WithCodec(testCodec{}))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cc.Close()

	r.NewAddress([]resolver.Address{{Addr: "backend"}})
	var isPickFirst bool
	for i := 0; i < 5000; i++ {
		cc.mu.Lock()
		isPickFirst = cc.curBalancerName == PickFirstBalancerName
		cc.mu.Unlock()
		if isPickFirst {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !isPickFirst {
		t.Fatalf("after 5 second, cc.balancer is of type %v, not pick_first", cc.curBalancerName)
	}

	// ClientConn will switch balancer to grpclb when receives an address of
	// type GRPCLB.
	r.NewAddress([]resolver.Address{{Addr: "backend"}, {Addr: "grpclb", Type: resolver.GRPCLB}})
	var isGRPCLB bool
	for i := 0; i < 5000; i++ {
		cc.mu.Lock()
		isGRPCLB = cc.curBalancerName == "grpclb"
		cc.mu.Unlock()
		if isGRPCLB {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !isGRPCLB {
		t.Fatalf("after 5 second, cc.balancer is of type %v, not grpclb", cc.curBalancerName)
	}

	// New update containing new backend and new grpclb. Should not switch
	// balancer.
	r.NewAddress([]resolver.Address{{Addr: "backend2"}, {Addr: "grpclb2", Type: resolver.GRPCLB}})
	for i := 0; i < 200; i++ {
		cc.mu.Lock()
		isGRPCLB = cc.curBalancerName == "grpclb"
		cc.mu.Unlock()
		if !isGRPCLB {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !isGRPCLB {
		t.Fatalf("within 200 ms, cc.balancer switched to !grpclb, want grpclb")
	}

	// Switch balancer back.
	r.NewAddress([]resolver.Address{{Addr: "backend"}})
	for i := 0; i < 5000; i++ {
		cc.mu.Lock()
		isPickFirst = cc.curBalancerName == PickFirstBalancerName
		cc.mu.Unlock()
		if isPickFirst {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !isPickFirst {
		t.Fatalf("after 5 second, cc.balancer is of type %v, not pick_first", cc.curBalancerName)
	}
}

// Test that if the current balancer is roundrobin, after switching to grpclb,
// when the resolved address doesn't contain grpclb addresses, balancer will be
// switched back to roundrobin.
func TestSwitchBalancerGRPCLBRoundRobin(t *testing.T) {
	defer leakcheck.Check(t)
	r, rcleanup := manual.GenerateAndRegisterManualResolver()
	defer rcleanup()

	cc, err := Dial(r.Scheme()+":///test.server", WithInsecure(), WithCodec(testCodec{}))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cc.Close()

	r.NewServiceConfig(`{"loadBalancingPolicy": "round_robin"}`)

	r.NewAddress([]resolver.Address{{Addr: "backend"}})
	var isRoundRobin bool
	for i := 0; i < 5000; i++ {
		cc.mu.Lock()
		isRoundRobin = cc.curBalancerName == "round_robin"
		cc.mu.Unlock()
		if isRoundRobin {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !isRoundRobin {
		t.Fatalf("after 5 second, cc.balancer is of type %v, not round_robin", cc.curBalancerName)
	}

	// ClientConn will switch balancer to grpclb when receives an address of
	// type GRPCLB.
	r.NewAddress([]resolver.Address{{Addr: "grpclb", Type: resolver.GRPCLB}})
	var isGRPCLB bool
	for i := 0; i < 5000; i++ {
		cc.mu.Lock()
		isGRPCLB = cc.curBalancerName == "grpclb"
		cc.mu.Unlock()
		if isGRPCLB {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !isGRPCLB {
		t.Fatalf("after 5 second, cc.balancer is of type %v, not grpclb", cc.curBalancerName)
	}

	// Switch balancer back.
	r.NewAddress([]resolver.Address{{Addr: "backend"}})
	for i := 0; i < 5000; i++ {
		cc.mu.Lock()
		isRoundRobin = cc.curBalancerName == "round_robin"
		cc.mu.Unlock()
		if isRoundRobin {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !isRoundRobin {
		t.Fatalf("after 5 second, cc.balancer is of type %v, not round_robin", cc.curBalancerName)
	}
}

// Test that if resolved address list contains grpclb, the balancer option in
// service config won't take effect. But when there's no grpclb address in a new
// resolved address list, balancer will be switched to the new one.
func TestSwitchBalancerGRPCLBServiceConfig(t *testing.T) {
	defer leakcheck.Check(t)
	r, rcleanup := manual.GenerateAndRegisterManualResolver()
	defer rcleanup()

	cc, err := Dial(r.Scheme()+":///test.server", WithInsecure(), WithCodec(testCodec{}))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer cc.Close()

	r.NewAddress([]resolver.Address{{Addr: "backend"}})
	var isPickFirst bool
	for i := 0; i < 5000; i++ {
		cc.mu.Lock()
		isPickFirst = cc.curBalancerName == PickFirstBalancerName
		cc.mu.Unlock()
		if isPickFirst {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !isPickFirst {
		t.Fatalf("after 5 second, cc.balancer is of type %v, not pick_first", cc.curBalancerName)
	}

	// ClientConn will switch balancer to grpclb when receives an address of
	// type GRPCLB.
	r.NewAddress([]resolver.Address{{Addr: "grpclb", Type: resolver.GRPCLB}})
	var isGRPCLB bool
	for i := 0; i < 5000; i++ {
		cc.mu.Lock()
		isGRPCLB = cc.curBalancerName == "grpclb"
		cc.mu.Unlock()
		if isGRPCLB {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !isGRPCLB {
		t.Fatalf("after 5 second, cc.balancer is of type %v, not grpclb", cc.curBalancerName)
	}

	r.NewServiceConfig(`{"loadBalancingPolicy": "round_robin"}`)
	var isRoundRobin bool
	for i := 0; i < 200; i++ {
		cc.mu.Lock()
		isRoundRobin = cc.curBalancerName == "round_robin"
		cc.mu.Unlock()
		if isRoundRobin {
			break
		}
		time.Sleep(time.Millisecond)
	}
	// Balancer should NOT switch to round_robin because resolved list contains
	// grpclb.
	if isRoundRobin {
		t.Fatalf("within 200 ms, cc.balancer switched to round_robin, want grpclb")
	}

	// Switch balancer back.
	r.NewAddress([]resolver.Address{{Addr: "backend"}})
	for i := 0; i < 5000; i++ {
		cc.mu.Lock()
		isRoundRobin = cc.curBalancerName == "round_robin"
		cc.mu.Unlock()
		if isRoundRobin {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !isRoundRobin {
		t.Fatalf("after 5 second, cc.balancer is of type %v, not round_robin", cc.curBalancerName)
	}
}
