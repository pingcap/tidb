// Copyright 2026 PingCAP, Inc.
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

package export

import (
	"github.com/pingcap/errors"
	"github.com/spf13/pflag"
)

const (
	flagPasswordEncryptedFile = "password-encrypted-file"
	flagPasswordKeyFile       = "password-key-file"
)

func defineCredentialFlags(flags *pflag.FlagSet) {
	flags.String(flagPasswordEncryptedFile, "", "Path to the Encrypted Password File")
	flags.String(flagPasswordKeyFile, "", "Path to the Password Key File")
}

func (conf *Config) parseCredentialFlags(flags *pflag.FlagSet) error {
	var err error
	conf.PasswordEncryptedFile, err = flags.GetString(flagPasswordEncryptedFile)
	if err != nil {
		return errors.Trace(err)
	}
	conf.PasswordKeyFile, err = flags.GetString(flagPasswordKeyFile)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
