/*
 *
 * Copyright 2016, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

// client starts an interop client to do stress test and a metrics server to report qps.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/interop"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	metricspb "google.golang.org/grpc/stress/grpc_testing"
)

var (
	serverAddresses      = flag.String("server_addresses", "localhost:8080", "a list of server addresses")
	testCases            = flag.String("test_cases", "", "a list of test cases along with the relative weights")
	testDurationSecs     = flag.Int("test_duration_secs", -1, "test duration in seconds")
	numChannelsPerServer = flag.Int("num_channels_per_server", 1, "Number of channels (i.e connections) to each server")
	numStubsPerChannel   = flag.Int("num_stubs_per_channel", 1, "Number of client stubs per each connection to server")
	metricsPort          = flag.Int("metrics_port", 8081, "The port at which the stress client exposes QPS metrics")
	useTLS               = flag.Bool("use_tls", false, "Connection uses TLS if true, else plain TCP")
	testCA               = flag.Bool("use_test_ca", false, "Whether to replace platform root CAs with test CA as the CA root")
	tlsServerName        = flag.String("server_host_override", "foo.test.google.fr", "The server name use to verify the hostname returned by TLS handshake if it is not empty. Otherwise, --server_host is used.")

	// The test CA root cert file
	testCAFile = "testdata/ca.pem"
)

// testCaseWithWeight contains the test case type and its weight.
type testCaseWithWeight struct {
	name   string
	weight int
}

// parseTestCases converts test case string to a list of struct testCaseWithWeight.
func parseTestCases(testCaseString string) []testCaseWithWeight {
	testCaseStrings := strings.Split(testCaseString, ",")
	testCases := make([]testCaseWithWeight, len(testCaseStrings))
	for i, str := range testCaseStrings {
		testCase := strings.Split(str, ":")
		if len(testCase) != 2 {
			panic(fmt.Sprintf("invalid test case with weight: %s", str))
		}
		// Check if test case is supported.
		switch testCase[0] {
		case
			"empty_unary",
			"large_unary",
			"client_streaming",
			"server_streaming",
			"ping_pong",
			"empty_stream",
			"timeout_on_sleeping_server",
			"cancel_after_begin",
			"cancel_after_first_response",
			"status_code_and_message",
			"custom_metadata":
		default:
			panic(fmt.Sprintf("unknown test type: %s", testCase[0]))
		}
		testCases[i].name = testCase[0]
		w, err := strconv.Atoi(testCase[1])
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}
		testCases[i].weight = w
	}
	return testCases
}

// weightedRandomTestSelector defines a weighted random selector for test case types.
type weightedRandomTestSelector struct {
	tests       []testCaseWithWeight
	totalWeight int
}

// newWeightedRandomTestSelector constructs a weightedRandomTestSelector with the given list of testCaseWithWeight.
func newWeightedRandomTestSelector(tests []testCaseWithWeight) *weightedRandomTestSelector {
	var totalWeight int
	for _, t := range tests {
		totalWeight += t.weight
	}
	rand.Seed(time.Now().UnixNano())
	return &weightedRandomTestSelector{tests, totalWeight}
}

func (selector weightedRandomTestSelector) getNextTest() string {
	random := rand.Intn(selector.totalWeight)
	var weightSofar int
	for _, test := range selector.tests {
		weightSofar += test.weight
		if random < weightSofar {
			return test.name
		}
	}
	panic("no test case selected by weightedRandomTestSelector")
}

// gauge stores the qps of one interop client (one stub).
type gauge struct {
	mutex sync.RWMutex
	val   int64
}

func (g *gauge) set(v int64) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	g.val = v
}

func (g *gauge) get() int64 {
	g.mutex.RLock()
	defer g.mutex.RUnlock()
	return g.val
}

// server implements metrics server functions.
type server struct {
	mutex sync.RWMutex
	// gauges is a map from /stress_test/server_<n>/channel_<n>/stub_<n>/qps to its qps gauge.
	gauges map[string]*gauge
}

// newMetricsServer returns a new metrics server.
func newMetricsServer() *server {
	return &server{gauges: make(map[string]*gauge)}
}

// GetAllGauges returns all gauges.
func (s *server) GetAllGauges(in *metricspb.EmptyMessage, stream metricspb.MetricsService_GetAllGaugesServer) error {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	for name, gauge := range s.gauges {
		if err := stream.Send(&metricspb.GaugeResponse{Name: name, Value: &metricspb.GaugeResponse_LongValue{LongValue: gauge.get()}}); err != nil {
			return err
		}
	}
	return nil
}

// GetGauge returns the gauge for the given name.
func (s *server) GetGauge(ctx context.Context, in *metricspb.GaugeRequest) (*metricspb.GaugeResponse, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if g, ok := s.gauges[in.Name]; ok {
		return &metricspb.GaugeResponse{Name: in.Name, Value: &metricspb.GaugeResponse_LongValue{LongValue: g.get()}}, nil
	}
	return nil, grpc.Errorf(codes.InvalidArgument, "gauge with name %s not found", in.Name)
}

// createGauge creates a gauge using the given name in metrics server.
func (s *server) createGauge(name string) *gauge {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.gauges[name]; ok {
		// gauge already exists.
		panic(fmt.Sprintf("gauge %s already exists", name))
	}
	var g gauge
	s.gauges[name] = &g
	return &g
}

func startServer(server *server, port int) {
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		grpclog.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	metricspb.RegisterMetricsServiceServer(s, server)
	s.Serve(lis)

}

// performRPCs uses weightedRandomTestSelector to select test case and runs the tests.
func performRPCs(gauge *gauge, conn *grpc.ClientConn, selector *weightedRandomTestSelector, stop <-chan bool) {
	client := testpb.NewTestServiceClient(conn)
	var numCalls int64
	startTime := time.Now()
	for {
		test := selector.getNextTest()
		switch test {
		case "empty_unary":
			interop.DoEmptyUnaryCall(client, grpc.FailFast(false))
		case "large_unary":
			interop.DoLargeUnaryCall(client, grpc.FailFast(false))
		case "client_streaming":
			interop.DoClientStreaming(client, grpc.FailFast(false))
		case "server_streaming":
			interop.DoServerStreaming(client, grpc.FailFast(false))
		case "ping_pong":
			interop.DoPingPong(client, grpc.FailFast(false))
		case "empty_stream":
			interop.DoEmptyStream(client, grpc.FailFast(false))
		case "timeout_on_sleeping_server":
			interop.DoTimeoutOnSleepingServer(client, grpc.FailFast(false))
		case "cancel_after_begin":
			interop.DoCancelAfterBegin(client, grpc.FailFast(false))
		case "cancel_after_first_response":
			interop.DoCancelAfterFirstResponse(client, grpc.FailFast(false))
		case "status_code_and_message":
			interop.DoStatusCodeAndMessage(client, grpc.FailFast(false))
		case "custom_metadata":
			interop.DoCustomMetadata(client, grpc.FailFast(false))
		}
		numCalls++
		gauge.set(int64(float64(numCalls) / time.Since(startTime).Seconds()))

		select {
		case <-stop:
			return
		default:
		}
	}
}

func logParameterInfo(addresses []string, tests []testCaseWithWeight) {
	grpclog.Printf("server_addresses: %s", *serverAddresses)
	grpclog.Printf("test_cases: %s", *testCases)
	grpclog.Printf("test_duration_secs: %d", *testDurationSecs)
	grpclog.Printf("num_channels_per_server: %d", *numChannelsPerServer)
	grpclog.Printf("num_stubs_per_channel: %d", *numStubsPerChannel)
	grpclog.Printf("metrics_port: %d", *metricsPort)
	grpclog.Printf("use_tls: %t", *useTLS)
	grpclog.Printf("use_test_ca: %t", *testCA)
	grpclog.Printf("server_host_override: %s", *tlsServerName)

	grpclog.Println("addresses:")
	for i, addr := range addresses {
		grpclog.Printf("%d. %s\n", i+1, addr)
	}
	grpclog.Println("tests:")
	for i, test := range tests {
		grpclog.Printf("%d. %v\n", i+1, test)
	}
}

func newConn(address string, useTLS, testCA bool, tlsServerName string) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	if useTLS {
		var sn string
		if tlsServerName != "" {
			sn = tlsServerName
		}
		var creds credentials.TransportCredentials
		if testCA {
			var err error
			creds, err = credentials.NewClientTLSFromFile(testCAFile, sn)
			if err != nil {
				grpclog.Fatalf("Failed to create TLS credentials %v", err)
			}
		} else {
			creds = credentials.NewClientTLSFromCert(nil, sn)
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}
	return grpc.Dial(address, opts...)
}

func main() {
	flag.Parse()
	addresses := strings.Split(*serverAddresses, ",")
	tests := parseTestCases(*testCases)
	logParameterInfo(addresses, tests)
	testSelector := newWeightedRandomTestSelector(tests)
	metricsServer := newMetricsServer()

	var wg sync.WaitGroup
	wg.Add(len(addresses) * *numChannelsPerServer * *numStubsPerChannel)
	stop := make(chan bool)

	for serverIndex, address := range addresses {
		for connIndex := 0; connIndex < *numChannelsPerServer; connIndex++ {
			conn, err := newConn(address, *useTLS, *testCA, *tlsServerName)
			if err != nil {
				grpclog.Fatalf("Fail to dial: %v", err)
			}
			defer conn.Close()
			for clientIndex := 0; clientIndex < *numStubsPerChannel; clientIndex++ {
				name := fmt.Sprintf("/stress_test/server_%d/channel_%d/stub_%d/qps", serverIndex+1, connIndex+1, clientIndex+1)
				go func() {
					defer wg.Done()
					g := metricsServer.createGauge(name)
					performRPCs(g, conn, testSelector, stop)
				}()
			}

		}
	}
	go startServer(metricsServer, *metricsPort)
	if *testDurationSecs > 0 {
		time.Sleep(time.Duration(*testDurationSecs) * time.Second)
		close(stop)
	}
	wg.Wait()
	grpclog.Printf(" ===== ALL DONE ===== ")

}
