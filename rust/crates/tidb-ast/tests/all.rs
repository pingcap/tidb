// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Single integration-test binary for all `tidb-ast` source tests.

// Register module-safe suites here; isolated suites remain explicit Cargo targets.
mod parser_ast_ddl_package_source;
mod parser_ast_dml_import_package_source;
mod parser_ast_dml_package_source;
mod parser_ast_expressions_package_source;
mod parser_ast_flag_package_source;
mod parser_ast_format_package_source;
mod parser_ast_functions_package_source;
mod parser_ast_misc_package_source;
mod parser_ast_model_package_source;
mod parser_ast_node_restore_source;
mod parser_ast_procedure_package_source;
mod parser_ast_sem_package_source;
mod parser_ast_stats_package_source;
mod parser_ast_util_package_source;
mod parser_format_package_source;
mod parser_opcode_package_source;
