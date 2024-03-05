// Copyright 2023 PingCAP, Inc.
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

package err

import (
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

var (
	// ErrInvalidType is returned when the type of a value is not expected.
	ErrInvalidType = dbterror.ClassServer.NewStd(errno.ErrInvalidType)
	// ErrInvalidSequence is returned when the sequence of method invocations is not correct.
	ErrInvalidSequence = dbterror.ClassServer.NewStd(errno.ErrInvalidSequence)
	// ErrNotAllowedCommand is returned when the used command is not in the expected list.
	ErrNotAllowedCommand = dbterror.ClassServer.NewStd(errno.ErrNotAllowedCommand)
	// ErrAccessDenied is returned when the user does not have sufficient privileges.
	ErrAccessDenied = dbterror.ClassServer.NewStd(errno.ErrAccessDenied)
	// ErrAccessDeniedNoPassword is returned when trying to use an anonymous user without password is not allowed.
	ErrAccessDeniedNoPassword = dbterror.ClassServer.NewStd(errno.ErrAccessDeniedNoPassword)
	// ErrConCount is returned when too many connections are established by the user.
	ErrConCount = dbterror.ClassServer.NewStd(errno.ErrConCount)
	// ErrSecureTransportRequired is returned when the user tries to connect without SSL.
	ErrSecureTransportRequired = dbterror.ClassServer.NewStd(errno.ErrSecureTransportRequired)
	// ErrMultiStatementDisabled is returned when the user tries to send multiple statements in one statement.
	ErrMultiStatementDisabled = dbterror.ClassServer.NewStd(errno.ErrMultiStatementDisabled)
	// ErrNewAbortingConnection is returned when the user tries to connect with an aborting connection.
	ErrNewAbortingConnection = dbterror.ClassServer.NewStd(errno.ErrNewAbortingConnection)
	// ErrNotSupportedAuthMode is returned when the user uses an unsupported authentication method.
	ErrNotSupportedAuthMode = dbterror.ClassServer.NewStd(errno.ErrNotSupportedAuthMode)
	// ErrNetPacketTooLarge is returned when the user sends a packet too large.
	ErrNetPacketTooLarge = dbterror.ClassServer.NewStd(errno.ErrNetPacketTooLarge)
	// ErrMustChangePassword is returned when the user must change the password.
	ErrMustChangePassword = dbterror.ClassServer.NewStd(errno.ErrMustChangePassword)
)
