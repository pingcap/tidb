// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package client

// Different parameter keys and values used by the system
const (
	// S1 instructions
	sampledParam     = "sampled"
	server1NameParam = "s1name"
	// S1->S2 instructions
	server2NameParam      = "s2name"
	server2TransportParam = "s2transport"
	// S2->S3 instructions
	server3NameParam      = "s3name"
	server3TransportParam = "s3transport"

	transportHTTP     = "http"
	transportTChannel = "tchannel"
	transportDummy    = "dummy"

	behaviorTrace = "trace"

	// RoleS1 is the name of the role for server S1
	RoleS1 = "S1"

	// RoleS2 is the name of the role for server S2
	RoleS2 = "S2"

	// RoleS3 is the name of the role for server S3
	RoleS3 = "S3"
)
