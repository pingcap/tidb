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

//! Go `pkg/executor/windows`: `WindowExec` with its `aggWindowProcessor`,
//! `rowFrameWindowProcessor` and `rangeFrameWindowProcessor`,
//! `PipelinedWindowExec`, `OrderedWindowExec` and their builder, over the
//! `AggFunc`s `aggfuncs.BuildWindowFunctions` builds.

mod builder;
mod pipelined_window;
mod window;

pub use builder::{build, build_ordered, WindowPlan};
pub use pipelined_window::{OrderedWindowExec, PipelinedWindowExec};
pub use window::WindowExec;
