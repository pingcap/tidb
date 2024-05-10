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

package api

import "errors"

// ErrTimerNotExist indicates that the specified timer not exist.
var ErrTimerNotExist = errors.New("timer not exist")

// ErrTimerExists indicates that the specified timer already exits.
var ErrTimerExists = errors.New("timer already exists")

// ErrVersionNotMatch indicates that the timer's version not match.
var ErrVersionNotMatch = errors.New("timer version not match")

// ErrEventIDNotMatch indicates that the timer's event id not match.
var ErrEventIDNotMatch = errors.New("timer event id not match")
