// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//! Embedding-provider credentials and endpoint.

use super::super::{SysVarDef, VarType};

pub(super) static ENTRIES: [SysVarDef; 7] = [
    SysVarDef {
        name: "tidb_exp_embed_cohere_api_key",
        scope: 1,
        value: "",
        var_type: VarType::Str,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 0,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_exp_embed_gemini_api_key",
        scope: 1,
        value: "",
        var_type: VarType::Str,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 0,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_exp_embed_huggingface_api_key",
        scope: 1,
        value: "",
        var_type: VarType::Str,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 0,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_exp_embed_jina_ai_api_key",
        scope: 1,
        value: "",
        var_type: VarType::Str,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 0,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_exp_embed_nvidia_nim_api_key",
        scope: 1,
        value: "",
        var_type: VarType::Str,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 0,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_exp_embed_openai_api_base",
        scope: 1,
        value: "https://api.openai.com/v1",
        var_type: VarType::Str,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 0,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
    SysVarDef {
        name: "tidb_exp_embed_openai_api_key",
        scope: 1,
        value: "",
        var_type: VarType::Str,
        read_only: false,
        allow_auto_value: false,
        min_value: 0,
        max_value: 0,
        possible_values: &[],
        auto_convert_negative_bool: false,
    },
];
