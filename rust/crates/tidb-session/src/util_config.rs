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

//! Go `pkg/util/config`.

use std::collections::HashMap;
use std::fmt;
use std::io::{self, Read};

use tidb_log::{Field, Value};

use crate::sysvar::{get_sys_var, SCOPE_SESSION};
use crate::SessionVars;

const INNODB_LOCK_WAIT_TIMEOUT: &str = "innodb_lock_wait_timeout";

/// An error returned before system-variable loading can begin.
#[derive(Debug)]
pub enum LoadError {
    /// Reading the TOML stream failed.
    Read(io::Error),
    /// The stream was not a TOML string map.
    Decode(toml::de::Error),
}

impl fmt::Display for LoadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(error) => error.fmt(formatter),
            Self::Decode(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for LoadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Read(error) => Some(error),
            Self::Decode(error) => Some(error),
        }
    }
}

impl From<io::Error> for LoadError {
    fn from(error: io::Error) -> Self {
        Self::Read(error)
    }
}

impl From<toml::de::Error> for LoadError {
    fn from(error: toml::de::Error) -> Self {
        Self::Decode(error)
    }
}

fn warn(message: String, error: Option<String>) {
    let fields = error
        .map(|error| vec![Field::new("error", Value::Str(error))])
        .unwrap_or_default();
    tidb_util::logutil::bg_logger().warn(&message, &fields);
}

/// Go `LoadConfigForPlanReplayerLoad`.
///
/// The caller retains ownership of closing its reader, as the Go function
/// leaves closing its `io.ReadCloser` to `executor.loadVariables`.
pub fn load_config_for_plan_replayer_load(
    vars: &mut SessionVars,
    mut reader: impl Read,
) -> Result<Vec<String>, LoadError> {
    let mut input = String::new();
    reader.read_to_string(&mut input)?;
    let var_map: HashMap<String, String> = toml::from_str(&input)?;
    let mut unloaded = Vec::new();

    for (name, value) in var_map {
        if name == INNODB_LOCK_WAIT_TIMEOUT {
            warn(format!("ignore set variable {name}:{value}"), None);
            continue;
        }

        let Some(sys_var) = get_sys_var(&name) else {
            unloaded.push(name.clone());
            warn(format!("skip set variable {name}:{value}"), None);
            continue;
        };

        let validated = match sys_var.validate_in_scope(&value, SCOPE_SESSION) {
            Ok(validated) => validated.value,
            Err(error) => {
                unloaded.push(name.clone());
                warn(
                    format!("skip variable {name}:{value}"),
                    Some(format!("{error:?}")),
                );
                continue;
            }
        };

        if let Err(error) = vars.set_system(&name, validated) {
            unloaded.push(name.clone());
            warn(
                format!("skip set variable {name}:{value}"),
                Some(format!("{error:?}")),
            );
        }
    }

    Ok(unloaded)
}
