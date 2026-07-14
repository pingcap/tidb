// Copyright 2025 PingCAP, Inc.
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

// Package example is an example application of DXF.
//
// add-index and import-into have integrated with DXF, but they might too complex
// for others to learn how to integrate with DXF. So we provide a simple example
// to help other developers to understand how to use DXF and how to integrate.
//
// See framework/doc.go for how DXF works.
//
// This example application have 2 steps, StepOne and StepTwo, and on each subtask
// of those steps, it will print a message.
//
// Use TestExampleApplication to test this example application.
package example
