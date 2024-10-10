// Copyright 2024 PingCAP, Inc.
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

//go:build !ddlargsv1

package testargsv1

// ForceV1 is a flag to force using ddl job V1 in test.
// Since 8.4.0, we have a new version of DDL args, but we have to keep logics of
// old version for compatibility. We change this to run unit-test another round
// in V1 to make sure both code are working correctly.
const ForceV1 = false
