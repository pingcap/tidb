// Copyright 2023-2023 PingCAP, Inc.
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

package conn

import "context"

// MFAAuthError marks an authentication failure for an account that requires
// more than its first factor. It preserves the original error for normal
// logging and error handling while allowing COM_CHANGE_USER to apply MySQL's
// disconnect rule only to MFA targets.
type MFAAuthError struct {
	CauseErr error
}

func (e *MFAAuthError) Error() string {
	if e == nil || e.CauseErr == nil {
		return "multi-factor authentication failed"
	}
	return e.CauseErr.Error()
}

func (e *MFAAuthError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.CauseErr
}

// Cause is provided for github.com/pingcap/errors.Cause compatibility.
func (e *MFAAuthError) Cause() error { return e.Unwrap() }

// WrapMFAAuthError marks err without changing its externally visible message.
func WrapMFAAuthError(err error) error {
	if err == nil {
		return nil
	}
	if existing, ok := err.(*MFAAuthError); ok {
		return existing
	}
	return &MFAAuthError{CauseErr: err}
}

// AuthConn is an interface to help auth plugin to communicate with the client.
type AuthConn interface {
	// WriteAuthMoreData should write a (u8)1 before data.
	// also see `client_mpvio_read_packet` in the MySQL client. It'll ignore the first byte if it's 1.
	WriteAuthMoreData(data []byte) error
	// ReadPacket reads one more packet.
	// also see `client_mpvio_write_packet` in the MySQL client. It's just a normal packet without any prefixing.
	// NOTE: the first packet returned by the client is appended in the `Protocol::AuthSwitchResponse`, also see `client_mpvio_write_packet`.
	ReadPacket() ([]byte, error)
	// Flush flushes all packets in the connection and sends them to the client
	Flush(ctx context.Context) error
}

// NextFactorAuthConn is implemented by protocol connections that support the
// MySQL AuthNextFactor packet. It is optional so existing authentication
// extensions keep the original AuthConn contract.
type NextFactorAuthConn interface {
	RequestNextAuthFactor(plugin string) ([]byte, error)
}
