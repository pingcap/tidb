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

package base

// read me if you want change the content of base interface definition.
// several things you should think twice before you add new method in.
//
// 1: interface should be simplified for abstract logic for most implementors
//    if your method is only inherited by few follower, do not use interface.
//
// 2: interface method declared here, meaning additional implementation logic
//    should be added in where the inheritors are declared. (pointer receiver
//    func can only be declared in where the inheritor defined: same pkg)
//
// 3: interface definition should cover the abstract logic, do not depend on
//    concrete implementor type, or relay on other core pkg handling logic.
//    otherwise, importing cycle occurs, think about abstraction again.
//
// 4: if additional interface method is decided to added, pls append it to
//	  function list with order, the later implementors reference can also be
//    easy to locate.
