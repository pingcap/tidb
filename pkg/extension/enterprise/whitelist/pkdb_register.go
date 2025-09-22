// Copyright 2022-2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package whitelist

import (
	"flag"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/parser/terror"
)

// ExtensionName is the extension name for audit log
const ExtensionName = "WhiteList"

// globalWhitelistManager manages the global state of the whitelist
var globalWhitelistManager AccessManager

var enableWhiteList bool

// Register registers enterprise extension
func Register() {
	// todo: add a system variable to control whether to enable whitelist
	flag.BoolVar(&enableWhiteList, "enable-whitelist", true, "enable whitelist or not")
	err := extension.RegisterFactory(ExtensionName, extensionOptionsFactory)
	terror.MustNil(err)
}

// Register4Test is the mock function that is used to test.
func Register4Test() {
	extension.Reset()
	enableWhiteList = true
	err := extension.RegisterFactory(ExtensionName, extensionOptionsFactory)
	terror.MustNil(err)
}

func extensionOptionsFactory() ([]extension.Option, error) {
	if !enableWhiteList {
		return nil, nil
	}

	if err := globalWhitelistManager.Init(); err != nil {
		return nil, err
	}

	return []extension.Option{
		extension.WithSessionHandlerFactory(globalWhitelistManager.GetSessionHandler),
		extension.WithBootstrap(globalWhitelistManager.Bootstrap),
		extension.WithClose(globalWhitelistManager.Close),
	}, nil
}
