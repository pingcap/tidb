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

//! Pins the complete Go source and runner universe of TiDB's direct client-go dependency.
//!
//! Resolution is deliberately offline: the exact direct `go.mod` pin must already exist in
//! `GOMODCACHE`. The checked ledgers bind every production source and test declaration to its
//! file SHA-256, while the module row also binds the Go sums and the digest of all Go files.

use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

const MODULE: &str = "github.com/tikv/client-go/v2";
const UNIVERSE: &str = "client-go";
const VERSION: &str = "v2.0.8-0.20260708122311-01bd8f99f4da";
const EXPECTED_PRODUCTION_SOURCES: usize = 151;
const EXPECTED_TEST_FILES: usize = 75;
const EXPECTED_DECLARATIONS: usize = 809;
const EXPECTED_RUNNERS: usize = 337;
const COVERAGE: &str = "rust/difftests/corpus/coverage";
const MODULE_LEDGER: &str = "external_go_modules.tsv";
const SOURCE_LEDGER: &str = "client_go_source_inventory.tsv";
const DECLARATION_LEDGER: &str = "client_go_test_declaration_inventory.tsv";
const TEST_LEDGER: &str = "client_go_test_inventory.tsv";

#[derive(Clone, Debug)]
struct Declaration {
    path: String,
    line: usize,
    column: usize,
    receiver: String,
    name: String,
    category: String,
    valid: bool,
    file_sha256: String,
}

#[derive(Debug)]
struct Inventory {
    module: String,
    sources: String,
    declarations: String,
    tests: String,
    production_sources: usize,
    test_files: usize,
    declaration_count: usize,
    runner_count: usize,
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("difftest must live at rust/difftests")
        .to_path_buf()
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn module_cache() -> Result<PathBuf, String> {
    if let Some(value) = env::var_os("GOMODCACHE") {
        return Ok(PathBuf::from(value));
    }
    if let Some(value) = env::var_os("GOPATH") {
        let first = env::split_paths(&value)
            .next()
            .ok_or_else(|| "GOPATH contains no paths".to_owned())?;
        return Ok(first.join("pkg/mod"));
    }
    let home = env::var_os("HOME").ok_or_else(|| {
        "GOMODCACHE, GOPATH, and HOME are unset; cannot resolve cached client-go".to_owned()
    })?;
    Ok(PathBuf::from(home).join("go/pkg/mod"))
}

fn exact_direct_pin(go_mod: &str) -> Result<(), String> {
    for line in go_mod.lines() {
        let code = line.split("//").next().unwrap_or_default().trim();
        if (code.starts_with("replace ") && code.contains(MODULE))
            || (code.starts_with(MODULE) && code.contains("=>"))
        {
            return Err(format!(
                "go.mod must not replace the pinned {MODULE} module"
            ));
        }
    }
    let exact = format!("{MODULE} {VERSION}");
    let mut matches = go_mod.lines().filter_map(|line| {
        let indirect = line.contains("// indirect");
        let code = line.split("//").next()?.trim();
        let mut fields = code.split_whitespace();
        let first = fields.next()?;
        let module = if first == "require" {
            fields.next()?
        } else {
            first
        };
        let version = fields.next()?;
        (module == MODULE).then(|| (format!("{module} {version}"), indirect))
    });
    let found = matches
        .next()
        .ok_or_else(|| format!("go.mod has no direct {MODULE} requirement"))?;
    if matches.next().is_some() {
        return Err(format!("go.mod contains duplicate {MODULE} requirements"));
    }
    if found.1 {
        return Err(format!(
            "{MODULE} must be a direct requirement, not // indirect"
        ));
    }
    if found.0 != exact {
        return Err(format!(
            "client-go pin drift: expected {exact}, found {}",
            found.0
        ));
    }
    Ok(())
}

fn exact_sums(go_sum: &str) -> Result<(String, String), String> {
    let source_prefix = format!("{MODULE} {VERSION} ");
    let mod_prefix = format!("{MODULE} {VERSION}/go.mod ");
    let source: Vec<_> = go_sum
        .lines()
        .filter_map(|line| line.strip_prefix(&source_prefix))
        .collect();
    let module: Vec<_> = go_sum
        .lines()
        .filter_map(|line| line.strip_prefix(&mod_prefix))
        .collect();
    if source.len() != 1 || module.len() != 1 {
        return Err(format!(
            "go.sum must contain exactly one source and go.mod sum for {MODULE} {VERSION}"
        ));
    }
    Ok((source[0].to_owned(), module[0].to_owned()))
}

fn walk_go_files(root: &Path, current: &Path, files: &mut Vec<PathBuf>) -> io::Result<()> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            if matches!(
                entry.file_name().to_str(),
                Some(".git" | "target" | "vendor")
            ) {
                continue;
            }
            walk_go_files(root, &path, files)?;
        } else if path.extension().and_then(|value| value.to_str()) == Some("go") {
            let _ = path.strip_prefix(root).expect("walk stays below root");
            files.push(path);
        }
    }
    Ok(())
}

fn relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .expect("module path")
        .to_string_lossy()
        .replace('\\', "/")
}

fn declarations(
    repo: &Path,
    module_root: &Path,
    hashes: &[(String, String)],
) -> Result<Vec<Declaration>, String> {
    let output = Command::new("go")
        .current_dir(repo)
        .env("GOPROXY", "off")
        .env("GOSUMDB", "off")
        .args([
            "run",
            "-mod=mod",
            "-p=12",
            "./rust/difftests/tools/go_test_declaration_inventory",
            "--root",
        ])
        .arg(module_root)
        .output()
        .map_err(|error| format!("run Go AST inventory helper: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "Go AST inventory helper failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let source = String::from_utf8(output.stdout)
        .map_err(|error| format!("Go AST inventory emitted non-UTF-8 output: {error}"))?;
    let hash_by_path: std::collections::BTreeMap<_, _> = hashes.iter().cloned().collect();
    let mut result = Vec::new();
    for (index, line) in source.lines().enumerate() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields: Vec<_> = line.split('\t').collect();
        if fields.len() != 7 {
            return Err(format!(
                "AST helper line {} has {} fields, expected 7",
                index + 1,
                fields.len()
            ));
        }
        let path = fields[0].to_owned();
        let file_sha256 = hash_by_path
            .get(&path)
            .ok_or_else(|| format!("AST helper returned unknown test file {path}"))?
            .clone();
        result.push(Declaration {
            path,
            line: fields[1]
                .parse()
                .map_err(|_| format!("invalid AST line at helper row {}", index + 1))?,
            column: fields[2]
                .parse()
                .map_err(|_| format!("invalid AST column at helper row {}", index + 1))?,
            receiver: fields[3].to_owned(),
            name: fields[4].to_owned(),
            category: fields[5].to_owned(),
            valid: fields[6]
                .parse()
                .map_err(|_| format!("invalid AST validity at helper row {}", index + 1))?,
            file_sha256,
        });
    }
    Ok(result)
}

fn offline_download(repo: &Path) -> Result<(PathBuf, String, String), String> {
    let output = Command::new("go")
        .current_dir(repo)
        .env("GOPROXY", "off")
        .env("GOSUMDB", "off")
        .args(["mod", "download", "-json", &format!("{MODULE}@{VERSION}")])
        .output()
        .map_err(|error| format!("run offline go mod download: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "offline go mod download failed; populate GOMODCACHE first: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let value: serde_json::Value = serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("parse go mod download JSON: {error}"))?;
    let field = |name: &str| {
        value[name]
            .as_str()
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| format!("go mod download JSON has no {name}"))
    };
    if field("Path")? != MODULE || field("Version")? != VERSION {
        return Err("go mod download resolved a different module or version".to_owned());
    }
    Ok((
        PathBuf::from(field("Dir")?),
        field("Sum")?,
        field("GoModSum")?,
    ))
}

fn render(repo: &Path) -> Result<Inventory, String> {
    let go_mod = fs::read_to_string(repo.join("go.mod")).map_err(|error| error.to_string())?;
    exact_direct_pin(&go_mod)?;
    let go_sum = fs::read_to_string(repo.join("go.sum")).map_err(|error| error.to_string())?;
    let (source_sum, go_mod_sum) = exact_sums(&go_sum)?;
    let (module_root, downloaded_sum, downloaded_go_mod_sum) = offline_download(repo)?;
    if downloaded_sum != source_sum || downloaded_go_mod_sum != go_mod_sum {
        return Err(format!(
            "offline module sums differ from go.sum: download {downloaded_sum}/{downloaded_go_mod_sum}, go.sum {source_sum}/{go_mod_sum}"
        ));
    }
    let expected_root = module_cache()?.join(format!("github.com/tikv/client-go/v2@{VERSION}"));
    if module_root != expected_root {
        return Err(format!(
            "offline module directory drift: expected {}, found {}",
            expected_root.display(),
            module_root.display()
        ));
    }
    if !module_root.is_dir() {
        return Err(format!(
            "cached client-go module is missing at {}; populate GOMODCACHE before running this offline check",
            module_root.display()
        ));
    }
    let cached_go_mod = fs::read_to_string(module_root.join("go.mod"))
        .map_err(|error| format!("read cached client-go go.mod: {error}"))?;
    if !cached_go_mod
        .lines()
        .any(|line| line.trim() == format!("module {MODULE}"))
    {
        return Err(format!("{} is not module {MODULE}", module_root.display()));
    }

    let mut files = Vec::new();
    walk_go_files(&module_root, &module_root, &mut files).map_err(|error| error.to_string())?;
    files.sort_by_key(|path| relative(&module_root, path));
    let mut hashes = Vec::new();
    let mut production = Vec::new();
    let mut test_files = 0;
    let mut tree_hasher = Sha256::new();
    for path in &files {
        let relative = relative(&module_root, path);
        let bytes = fs::read(path).map_err(|error| format!("read {}: {error}", path.display()))?;
        let hash = sha256(&bytes);
        tree_hasher.update(relative.as_bytes());
        tree_hasher.update([0]);
        tree_hasher.update(hash.as_bytes());
        tree_hasher.update([0]);
        hashes.push((relative.clone(), hash.clone()));
        if relative.ends_with("_test.go") {
            test_files += 1;
        } else {
            let lines = bytes.iter().filter(|byte| **byte == b'\n').count()
                + usize::from(!bytes.is_empty() && !bytes.ends_with(b"\n"));
            production.push((relative, lines, hash));
        }
    }
    let declarations = declarations(repo, &module_root, &hashes)?;
    let runners: Vec<_> = declarations
        .iter()
        .filter(|declaration| declaration.valid)
        .collect();
    let unique_runner_anchors: BTreeSet<_> = runners
        .iter()
        .map(|declaration| (&declaration.path, declaration.line, &declaration.name))
        .collect();
    if unique_runner_anchors.len() != runners.len() {
        return Err("client-go runner anchors are not unique".to_owned());
    }
    let counts = (
        production.len(),
        test_files,
        declarations.len(),
        runners.len(),
    );
    let expected = (
        EXPECTED_PRODUCTION_SOURCES,
        EXPECTED_TEST_FILES,
        EXPECTED_DECLARATIONS,
        EXPECTED_RUNNERS,
    );
    if counts != expected {
        return Err(format!(
            "client-go universe drift: expected production/test-files/declarations/runners {expected:?}, found {counts:?}"
        ));
    }

    let tree_sha256 = format!("{:x}", tree_hasher.finalize());
    let mut module = String::from("# universe\tmodule\tversion\tgo_sum\tgo_mod_sum\tgo_file_tree_sha256\tproduction_sources\ttest_files\ttest_declarations\ttest_obligations\n");
    module.push_str(&format!("{UNIVERSE}\t{MODULE}\t{VERSION}\t{source_sum}\t{go_mod_sum}\t{tree_sha256}\t{}\t{test_files}\t{}\t{}\n", production.len(), declarations.len(), runners.len()));

    let mut sources = String::from(
        "# universe\tsource_path\tline_count\tfile_sha256\tstatus\towner\tartifact\tnote\n",
    );
    for (path, lines, hash) in &production {
        sources.push_str(&format!(
            "{UNIVERSE}\t{path}\t{lines}\t{hash}\tUNTRIAGED\t-\t-\t-\n"
        ));
    }
    let mut declaration_text = String::from("# universe\tsource_path\tsource_line\tsource_column\treceiver\tfunction_name\tcategory\tvalid_runner_signature\tfile_sha256\n");
    for item in &declarations {
        declaration_text.push_str(&format!(
            "{UNIVERSE}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            item.path,
            item.line,
            item.column,
            item.receiver,
            item.name,
            item.category,
            item.valid,
            item.file_sha256
        ));
    }
    let mut tests = String::from("# universe\tkind\tsource_path\tsource_line\tfunction_name\tring\tfile_sha256\tstatus\towner\tartifact\tnote\n");
    for item in runners {
        tests.push_str(&format!(
            "{UNIVERSE}\t{}\t{}\t{}\t{}\ttransaction\t{}\tUNTRIAGED\t-\t-\t-\n",
            item.category, item.path, item.line, item.name, item.file_sha256
        ));
    }
    Ok(Inventory {
        module,
        sources,
        declarations: declaration_text,
        tests,
        production_sources: production.len(),
        test_files,
        declaration_count: declarations.len(),
        runner_count: unique_runner_anchors.len(),
    })
}

fn ledger_paths(repo: &Path) -> [(PathBuf, &'static str); 4] {
    let root = repo.join(COVERAGE);
    [
        (root.join(MODULE_LEDGER), MODULE_LEDGER),
        (root.join(SOURCE_LEDGER), SOURCE_LEDGER),
        (root.join(DECLARATION_LEDGER), DECLARATION_LEDGER),
        (root.join(TEST_LEDGER), TEST_LEDGER),
    ]
}

fn main() -> Result<(), String> {
    let arguments: Vec<_> = env::args().skip(1).collect();
    if arguments.len() != 1 || !matches!(arguments[0].as_str(), "--write" | "--check" | "--summary")
    {
        return Err("usage: client_go_ledger --write | --check | --summary".to_owned());
    }
    let repo = repo_root();
    let inventory = render(&repo)?;
    let rendered = [
        &inventory.module,
        &inventory.sources,
        &inventory.declarations,
        &inventory.tests,
    ];
    match arguments[0].as_str() {
        "--write" => {
            for ((path, _), content) in ledger_paths(&repo).iter().zip(rendered) {
                fs::write(path, content).map_err(|error| format!("write {}: {error}", path.display()))?;
            }
        }
        "--check" => {
            for ((path, label), content) in ledger_paths(&repo).iter().zip(rendered) {
                let current = fs::read_to_string(path).unwrap_or_default();
                if current != *content {
                    return Err(format!("{label} is stale; run client_go_ledger --write"));
                }
            }
        }
        "--summary" => println!(
            "module\t{MODULE}\t{VERSION}\nproduction_sources\t{}\ntest_files\t{}\ntest_declarations\t{}\ntest_obligations\t{}",
            inventory.production_sources, inventory.test_files, inventory.declaration_count, inventory.runner_count
        ),
        _ => unreachable!(),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{exact_direct_pin, exact_sums, MODULE, VERSION};

    #[test]
    fn direct_pin_rejects_version_drift_and_indirect_only_rows() {
        assert!(exact_direct_pin(&format!("require {MODULE} {VERSION}\n")).is_ok());
        assert!(exact_direct_pin(&format!("require {MODULE} v0.0.0\n")).is_err());
        assert!(exact_direct_pin(&format!("require {MODULE} {VERSION} // indirect\n")).is_err());
        assert!(exact_direct_pin(&format!(
            "require {MODULE} {VERSION}\nreplace {MODULE} => ../client-go\n"
        ))
        .is_err());
    }

    #[test]
    fn sums_must_be_complete_and_unique() {
        let sums = format!("{MODULE} {VERSION} h1:source\n{MODULE} {VERSION}/go.mod h1:module\n");
        assert_eq!(
            exact_sums(&sums),
            Ok(("h1:source".to_owned(), "h1:module".to_owned()))
        );
        assert!(exact_sums(&format!("{MODULE} {VERSION} h1:source\n")).is_err());
    }
}
