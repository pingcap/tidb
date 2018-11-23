// +build go1.8

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
	"context"
	"fmt"
	"net"
)

var errForInvalidTarget = fmt.Errorf("invalid target address [2001:db8:a0b:12f0::1, error info: address [2001:db8:a0b:12f0::1:443: missing ']' in address")

func replaceNetFunc() func() {
	oldLookupHost := lookupHost
	oldLookupSRV := lookupSRV
	oldLookupTXT := lookupTXT
	lookupHost = func(ctx context.Context, host string) ([]string, error) {
		return hostLookup(host)
	}
	lookupSRV = func(ctx context.Context, service, proto, name string) (string, []*net.SRV, error) {
		return srvLookup(service, proto, name)
	}
	lookupTXT = func(ctx context.Context, host string) ([]string, error) {
		return txtLookup(host)
	}
	return func() {
		lookupHost = oldLookupHost
		lookupSRV = oldLookupSRV
		lookupTXT = oldLookupTXT
	}
}
