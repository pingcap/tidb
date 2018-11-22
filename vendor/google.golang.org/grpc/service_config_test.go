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
	"reflect"
	"testing"
	"time"
)

func TestParseLoadBalancer(t *testing.T) {
	testcases := []struct {
		scjs    string
		wantSC  ServiceConfig
		wantErr bool
	}{
		{
			`{
    "loadBalancingPolicy": "round_robin",
    "methodConfig": [
        {
            "name": [
                {
                    "service": "foo",
                    "method": "Bar"
                }
            ],
            "waitForReady": true
        }
    ]
}`,
			ServiceConfig{
				LB: newString("round_robin"),
				Methods: map[string]MethodConfig{
					"/foo/Bar": {
						WaitForReady: newBool(true),
					},
				},
			},
			false,
		},
		{
			`{
    "loadBalancingPolicy": 1,
    "methodConfig": [
        {
            "name": [
                {
                    "service": "foo",
                    "method": "Bar"
                }
            ],
            "waitForReady": false
        }
    ]
}`,
			ServiceConfig{},
			true,
		},
	}

	for _, c := range testcases {
		sc, err := parseServiceConfig(c.scjs)
		if c.wantErr != (err != nil) || !reflect.DeepEqual(sc, c.wantSC) {
			t.Fatalf("parseServiceConfig(%s) = %+v, %v, want %+v, %v", c.scjs, sc, err, c.wantSC, c.wantErr)
		}
	}
}

func TestParseWaitForReady(t *testing.T) {
	testcases := []struct {
		scjs    string
		wantSC  ServiceConfig
		wantErr bool
	}{
		{
			`{
    "methodConfig": [
        {
            "name": [
                {
                    "service": "foo",
                    "method": "Bar"
                }
            ],
            "waitForReady": true
        }
    ]
}`,
			ServiceConfig{
				Methods: map[string]MethodConfig{
					"/foo/Bar": {
						WaitForReady: newBool(true),
					},
				},
			},
			false,
		},
		{
			`{
    "methodConfig": [
        {
            "name": [
                {
                    "service": "foo",
                    "method": "Bar"
                }
            ],
            "waitForReady": false
        }
    ]
}`,
			ServiceConfig{
				Methods: map[string]MethodConfig{
					"/foo/Bar": {
						WaitForReady: newBool(false),
					},
				},
			},
			false,
		},
		{
			`{
    "methodConfig": [
        {
            "name": [
                {
                    "service": "foo",
                    "method": "Bar"
                }
            ],
            "waitForReady": fall
        },
        {
            "name": [
                {
                    "service": "foo",
                    "method": "Bar"
                }
            ],
            "waitForReady": true
        }
    ]
}`,
			ServiceConfig{},
			true,
		},
	}

	for _, c := range testcases {
		sc, err := parseServiceConfig(c.scjs)
		if c.wantErr != (err != nil) || !reflect.DeepEqual(sc, c.wantSC) {
			t.Fatalf("parseServiceConfig(%s) = %+v, %v, want %+v, %v", c.scjs, sc, err, c.wantSC, c.wantErr)
		}
	}
}

func TestPraseTimeOut(t *testing.T) {
	testcases := []struct {
		scjs    string
		wantSC  ServiceConfig
		wantErr bool
	}{
		{
			`{
    "methodConfig": [
        {
            "name": [
                {
                    "service": "foo",
                    "method": "Bar"
                }
            ],
            "timeout": "1s"
        }
    ]
}`,
			ServiceConfig{
				Methods: map[string]MethodConfig{
					"/foo/Bar": {
						Timeout: newDuration(time.Second),
					},
				},
			},
			false,
		},
		{
			`{
    "methodConfig": [
        {
            "name": [
                {
                    "service": "foo",
                    "method": "Bar"
                }
            ],
            "timeout": "3c"
        }
    ]
}`,
			ServiceConfig{},
			true,
		},
		{
			`{
    "methodConfig": [
        {
            "name": [
                {
                    "service": "foo",
                    "method": "Bar"
                }
            ],
            "timeout": "3c"
        },
        {
            "name": [
                {
                    "service": "foo",
                    "method": "Bar"
                }
            ],
            "timeout": "1s"
        }
    ]
}`,
			ServiceConfig{},
			true,
		},
	}

	for _, c := range testcases {
		sc, err := parseServiceConfig(c.scjs)
		if c.wantErr != (err != nil) || !reflect.DeepEqual(sc, c.wantSC) {
			t.Fatalf("parseServiceConfig(%s) = %+v, %v, want %+v, %v", c.scjs, sc, err, c.wantSC, c.wantErr)
		}
	}
}

func TestPraseMsgSize(t *testing.T) {
	testcases := []struct {
		scjs    string
		wantSC  ServiceConfig
		wantErr bool
	}{
		{
			`{
    "methodConfig": [
        {
            "name": [
                {
                    "service": "foo",
                    "method": "Bar"
                }
            ],
            "maxRequestMessageBytes": 1024,
            "maxResponseMessageBytes": 2048
        }
    ]
}`,
			ServiceConfig{
				Methods: map[string]MethodConfig{
					"/foo/Bar": {
						MaxReqSize:  newInt(1024),
						MaxRespSize: newInt(2048),
					},
				},
			},
			false,
		},
		{
			`{
    "methodConfig": [
        {
            "name": [
                {
                    "service": "foo",
                    "method": "Bar"
                }
            ],
            "maxRequestMessageBytes": "1024",
            "maxResponseMessageBytes": "2048"
        },
        {
            "name": [
                {
                    "service": "foo",
                    "method": "Bar"
                }
            ],
            "maxRequestMessageBytes": 1024,
            "maxResponseMessageBytes": 2048
        }
    ]
}`,
			ServiceConfig{},
			true,
		},
	}

	for _, c := range testcases {
		sc, err := parseServiceConfig(c.scjs)
		if c.wantErr != (err != nil) || !reflect.DeepEqual(sc, c.wantSC) {
			t.Fatalf("parseServiceConfig(%s) = %+v, %v, want %+v, %v", c.scjs, sc, err, c.wantSC, c.wantErr)
		}
	}
}

func TestParseDuration(t *testing.T) {
	testCases := []struct {
		s    *string
		want *time.Duration
		err  bool
	}{
		{s: nil, want: nil},
		{s: newString("1s"), want: newDuration(time.Second)},
		{s: newString("-1s"), want: newDuration(-time.Second)},
		{s: newString("1.1s"), want: newDuration(1100 * time.Millisecond)},
		{s: newString("1.s"), want: newDuration(time.Second)},
		{s: newString("1.0s"), want: newDuration(time.Second)},
		{s: newString(".002s"), want: newDuration(2 * time.Millisecond)},
		{s: newString(".002000s"), want: newDuration(2 * time.Millisecond)},
		{s: newString("0.003s"), want: newDuration(3 * time.Millisecond)},
		{s: newString("0.000004s"), want: newDuration(4 * time.Microsecond)},
		{s: newString("5000.000000009s"), want: newDuration(5000*time.Second + 9*time.Nanosecond)},
		{s: newString("4999.999999999s"), want: newDuration(5000*time.Second - time.Nanosecond)},
		{s: newString("1"), err: true},
		{s: newString("s"), err: true},
		{s: newString(".s"), err: true},
		{s: newString("1 s"), err: true},
		{s: newString(" 1s"), err: true},
		{s: newString("1ms"), err: true},
		{s: newString("1.1.1s"), err: true},
		{s: newString("Xs"), err: true},
		{s: newString("as"), err: true},
		{s: newString(".0000000001s"), err: true},
		{s: newString(fmt.Sprint(math.MaxInt32) + "s"), want: newDuration(math.MaxInt32 * time.Second)},
		{s: newString(fmt.Sprint(int64(math.MaxInt32)+1) + "s"), err: true},
	}
	for _, tc := range testCases {
		got, err := parseDuration(tc.s)
		if tc.err != (err != nil) ||
			(got == nil) != (tc.want == nil) ||
			(got != nil && *got != *tc.want) {
			wantErr := "<nil>"
			if tc.err {
				wantErr = "<non-nil error>"
			}
			s := "<nil>"
			if tc.s != nil {
				s = `&"` + *tc.s + `"`
			}
			t.Errorf("parseDuration(%v) = %v, %v; want %v, %v", s, got, err, tc.want, wantErr)
		}
	}
}

func newBool(b bool) *bool {
	return &b
}

func newDuration(b time.Duration) *time.Duration {
	return &b
}

func newString(b string) *string {
	return &b
}
