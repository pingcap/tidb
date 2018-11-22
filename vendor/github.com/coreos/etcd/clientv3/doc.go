// Copyright 2016 The etcd Authors
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

// Package clientv3 implements the official Go etcd client for v3.
//
// Create client using `clientv3.New`:
//
//	// expect dial time-out on ipv4 blackhole
//	_, err := clientv3.New(clientv3.Config{
//		Endpoints:   []string{"http://254.0.0.1:12345"},
//		DialTimeout: 2 * time.Second
//	})
//
//	// etcd clientv3 >= v3.2.10, grpc/grpc-go >= v1.7.3
//	if err == context.DeadlineExceeded {
//		// handle errors
//	}
//
//	// etcd clientv3 <= v3.2.9, grpc/grpc-go <= v1.2.1
//	if err == grpc.ErrClientConnTimeout {
//		// handle errors
//	}
//
//	cli, err := clientv3.New(clientv3.Config{
//		Endpoints:   []string{"localhost:2379", "localhost:22379", "localhost:32379"},
//		DialTimeout: 5 * time.Second,
//	})
//	if err != nil {
//		// handle error!
//	}
//	defer cli.Close()
//
// Make sure to close the client after using it. If the client is not closed, the
// connection will have leaky goroutines.
//
// To specify a client request timeout, wrap the context with context.WithTimeout:
//
//	ctx, cancel := context.WithTimeout(context.Background(), timeout)
//	resp, err := kvc.Put(ctx, "sample_key", "sample_value")
//	cancel()
//	if err != nil {
//	    // handle error!
//	}
//	// use the response
//
// The Client has internal state (watchers and leases), so Clients should be reused instead of created as needed.
// Clients are safe for concurrent use by multiple goroutines.
//
// etcd client returns 3 types of errors:
//
//  1. context error: canceled or deadline exceeded.
//  2. gRPC status error: e.g. when clock drifts in server-side before client's context deadline exceeded.
//  3. gRPC error: see https://github.com/coreos/etcd/blob/master/etcdserver/api/v3rpc/rpctypes/error.go
//
// Here is the example code to handle client errors:
//
//	resp, err := kvc.Put(ctx, "", "")
//	if err != nil {
//		if err == context.Canceled {
//			// ctx is canceled by another routine
//		} else if err == context.DeadlineExceeded {
//			// ctx is attached with a deadline and it exceeded
//		} else if ev, ok := status.FromError(err); ok {
//			code := ev.Code()
//			if code == codes.DeadlineExceeded {
//				// server-side context might have timed-out first (due to clock skew)
//				// while original client-side context is not timed-out yet
//			}
//		} else if verr, ok := err.(*v3rpc.ErrEmptyKey); ok {
//			// process (verr.Errors)
//		} else {
//			// bad cluster endpoints, which are not etcd servers
//		}
//	}
//
//	go func() { cli.Close() }()
//	_, err := kvc.Get(ctx, "a")
//	if err != nil {
//		if err == context.Canceled {
//			// grpc balancer calls 'Get' with an inflight client.Close
//		} else if err == grpc.ErrClientConnClosing {
//			// grpc balancer calls 'Get' after client.Close.
//		}
//	}
//
package clientv3
