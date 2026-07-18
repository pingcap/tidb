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

//! Pins the complete Go source and test-obligation universes of TiDB's direct external dependencies.
//!
//! Resolution is deliberately offline: the exact direct `go.mod` pin must already exist in
//! `GOMODCACHE`. The checked ledgers bind every production source and test declaration to its
//! file SHA-256, while the module row also binds the Go sums and the digest of all Go files.

use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

const COVERAGE: &str = "rust/difftests/corpus/coverage";
const MODULE_LEDGER: &str = "external_go_modules.tsv";
const SOURCE_LEDGER: &str = "external_go_source_inventory.tsv";
const DECLARATION_LEDGER: &str = "external_go_test_declaration_inventory.tsv";
const TEST_LEDGER: &str = "external_go_test_inventory.tsv";

#[derive(Clone, Copy, Debug)]
struct ModuleSpec {
    universe: &'static str,
    module: &'static str,
    version: &'static str,
    source_sum: &'static str,
    go_mod_sum: &'static str,
    expected_production_sources: usize,
    expected_test_files: usize,
    expected_declarations: usize,
    expected_obligations: usize,
}

const MODULES: [ModuleSpec; 2] = [
    ModuleSpec {
        universe: "client-go",
        module: "github.com/tikv/client-go/v2",
        version: "v2.0.8-0.20260708122311-01bd8f99f4da",
        source_sum: "h1:Ju9uUKu3M5gPl7Z90e9dcGSe8LtXxGqvSetbPcr6tyc=",
        go_mod_sum: "h1:MRhIujZMMkYcI49Euif4+A3+SHSGwTpyUUadNz48R4g=",
        expected_production_sources: 151,
        expected_test_files: 75,
        expected_declarations: 809,
        expected_obligations: 474,
    },
    ModuleSpec {
        universe: "pd-client",
        module: "github.com/tikv/pd/client",
        version: "v0.0.0-20260708075407-4e05b9d2c2d3",
        source_sum: "h1:OoBvgoeWmdNEXtS+eOlhysz/OvhA4GS0OdPVhTXteGA=",
        go_mod_sum: "h1:3/Bu91CJONgkDA+Y0v/cnbROSJnu5tQ09vv7JGybUBA=",
        expected_production_sources: 58,
        expected_test_files: 28,
        expected_declarations: 319,
        expected_obligations: 170,
    },
];

#[derive(Clone, Debug, Eq, PartialEq)]
struct Evidence {
    status: String,
    owner: String,
    artifact: String,
    note: String,
}

#[derive(Clone, Debug)]
struct Declaration {
    path: String,
    line: usize,
    column: usize,
    receiver: String,
    name: String,
    category: String,
    actionable: bool,
    suite_parents: Vec<String>,
    file_sha256: String,
}

#[derive(Clone, Debug)]
struct TestObligation {
    path: String,
    line: usize,
    name: String,
    category: String,
    file_sha256: String,
}

fn test_obligations(declarations: &[Declaration]) -> Vec<TestObligation> {
    declarations
        .iter()
        .filter(|declaration| declaration.actionable)
        .flat_map(|declaration| {
            let names = if declaration.category == "TestSuiteMethod" {
                declaration
                    .suite_parents
                    .iter()
                    .map(|parent| format!("{parent}/{}", declaration.name))
                    .collect()
            } else {
                vec![declaration.name.clone()]
            };
            names.into_iter().map(|name| TestObligation {
                path: declaration.path.clone(),
                line: declaration.line,
                name,
                category: declaration.category.clone(),
                file_sha256: declaration.file_sha256.clone(),
            })
        })
        .collect()
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
    obligation_count: usize,
    source_keys: Vec<String>,
    test_keys: Vec<String>,
}

fn evidence_directory(spec: ModuleSpec, kind: &str) -> String {
    format!(
        "rust/difftests/corpus/coverage/evidence/{}/{kind}",
        spec.universe
    )
}

fn evidence_files(repo: &Path, relative: &str) -> Result<Vec<PathBuf>, String> {
    let directory = repo.join(relative);
    let mut files = Vec::new();
    for entry in fs::read_dir(&directory)
        .map_err(|error| format!("read evidence directory {}: {error}", directory.display()))?
    {
        let entry = entry.map_err(|error| format!("read evidence entry: {error}"))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| format!("read evidence type {}: {error}", path.display()))?;
        if path.file_name().and_then(|value| value.to_str()) == Some(".gitkeep") {
            if !file_type.is_file() {
                return Err(format!(
                    "unexpected external Go evidence entry {}; .gitkeep must be a regular file",
                    path.display()
                ));
            }
            continue;
        }
        if !file_type.is_file() || path.extension().and_then(|value| value.to_str()) != Some("tsv")
        {
            return Err(format!(
                "unexpected external Go evidence entry {}; only regular .tsv files and .gitkeep are allowed",
                path.display()
            ));
        }
        files.push(path);
    }
    files.sort();
    Ok(files)
}

fn evidence_row(
    repo: &Path,
    path: &Path,
    line: usize,
    fields: &[&str],
    status_index: usize,
) -> Result<Evidence, String> {
    let status = fields[status_index];
    if !matches!(status, "PARTIAL" | "COVERED" | "BLOCKED") {
        return Err(format!(
            "{}:{line}: status must be PARTIAL, COVERED, or BLOCKED",
            path.display()
        ));
    }
    let owner = fields[status_index + 1];
    let artifact = fields[status_index + 2];
    let note = fields[status_index + 3];
    if owner.is_empty() || artifact.is_empty() || note.is_empty() {
        return Err(format!(
            "{}:{line}: status, owner, artifact, and note must all be present",
            path.display()
        ));
    }
    if path.file_stem().and_then(|value| value.to_str()) != Some(owner) {
        return Err(format!(
            "{}:{line}: evidence owner {owner} must match the fragment filename",
            path.display()
        ));
    }
    if artifact.contains(',') {
        return Err(format!(
            "{}:{line}: evidence artifact must be one path, not a comma-separated list",
            path.display()
        ));
    }
    if !repo.join(artifact).is_file() {
        return Err(format!(
            "{}:{line}: evidence artifact {} does not exist",
            path.display(),
            repo.join(artifact).display()
        ));
    }
    Ok(Evidence {
        status: status.to_owned(),
        owner: owner.to_owned(),
        artifact: artifact.to_owned(),
        note: note.to_owned(),
    })
}

fn source_evidence(
    repo: &Path,
    spec: ModuleSpec,
    production: &[(String, usize, String)],
) -> Result<BTreeMap<String, Evidence>, String> {
    let known: BTreeSet<_> = production.iter().map(|item| item.0.as_str()).collect();
    let mut overlays = BTreeMap::new();
    let mut origins = BTreeMap::new();
    for path in evidence_files(repo, &evidence_directory(spec, "source"))? {
        let text = fs::read_to_string(&path)
            .map_err(|error| format!("read {}: {error}", path.display()))?;
        for (index, line) in text.lines().enumerate() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let fields: Vec<_> = line.split('\t').collect();
            if fields.len() != 5 {
                return Err(format!(
                    "{}:{}: expected 5 tab-separated fields",
                    path.display(),
                    index + 1
                ));
            }
            if !known.contains(fields[0]) {
                return Err(format!(
                    "{}:{}: stale {} source path {}",
                    path.display(),
                    index + 1,
                    spec.universe,
                    fields[0]
                ));
            }
            if let Some(first) = origins.insert(fields[0].to_owned(), path.display().to_string()) {
                return Err(format!(
                    "{}:{}: duplicate {} source {} (first declared in {first})",
                    path.display(),
                    index + 1,
                    spec.universe,
                    fields[0]
                ));
            }
            overlays.insert(
                fields[0].to_owned(),
                evidence_row(repo, &path, index + 1, &fields, 1)?,
            );
        }
    }
    Ok(overlays)
}

fn test_evidence(
    repo: &Path,
    spec: ModuleSpec,
    obligations: &[TestObligation],
) -> Result<BTreeMap<(String, usize, String), Evidence>, String> {
    let known: BTreeMap<_, _> = obligations
        .iter()
        .map(|item| {
            (
                (item.path.as_str(), item.line, item.name.as_str()),
                item.category.as_str(),
            )
        })
        .collect();
    let mut overlays = BTreeMap::new();
    let mut origins = BTreeMap::new();
    for path in evidence_files(repo, &evidence_directory(spec, "tests"))? {
        let text = fs::read_to_string(&path)
            .map_err(|error| format!("read {}: {error}", path.display()))?;
        for (index, line) in text.lines().enumerate() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let fields: Vec<_> = line.split('\t').collect();
            if fields.len() != 8 {
                return Err(format!(
                    "{}:{}: expected 8 tab-separated fields",
                    path.display(),
                    index + 1
                ));
            }
            let source_line: usize = fields[2]
                .parse()
                .map_err(|_| format!("{}:{}: invalid source line", path.display(), index + 1))?;
            let borrowed = (fields[1], source_line, fields[3]);
            let Some(category) = known.get(&borrowed) else {
                return Err(format!(
                    "{}:{}: stale {} test anchor {}:{}:{}",
                    path.display(),
                    index + 1,
                    spec.universe,
                    fields[1],
                    source_line,
                    fields[3]
                ));
            };
            if fields[0] != *category {
                return Err(format!(
                    "{}:{}: test kind {} does not match generated kind {}",
                    path.display(),
                    index + 1,
                    fields[0],
                    category
                ));
            }
            let key = (fields[1].to_owned(), source_line, fields[3].to_owned());
            if let Some(first) = origins.insert(key.clone(), path.display().to_string()) {
                return Err(format!(
                    "{}:{}: duplicate {} test anchor {}:{}:{} (first declared in {first})",
                    path.display(),
                    index + 1,
                    spec.universe,
                    fields[1],
                    source_line,
                    fields[3]
                ));
            }
            overlays.insert(key, evidence_row(repo, &path, index + 1, &fields, 4)?);
        }
    }
    Ok(overlays)
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
        "GOMODCACHE, GOPATH, and HOME are unset; cannot resolve cached external Go modules"
            .to_owned()
    })?;
    Ok(PathBuf::from(home).join("go/pkg/mod"))
}

fn exact_direct_pin(go_mod: &str, spec: ModuleSpec) -> Result<(), String> {
    for line in go_mod.lines() {
        let code = line.split("//").next().unwrap_or_default().trim();
        if (code.starts_with("replace ") && code.contains(spec.module))
            || (code.starts_with(spec.module) && code.contains("=>"))
        {
            return Err(format!(
                "go.mod must not replace the pinned {} module",
                spec.module
            ));
        }
    }
    let exact = format!("{} {}", spec.module, spec.version);
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
        (module == spec.module).then(|| (format!("{module} {version}"), indirect))
    });
    let found = matches
        .next()
        .ok_or_else(|| format!("go.mod has no direct {} requirement", spec.module))?;
    if matches.next().is_some() {
        return Err(format!(
            "go.mod contains duplicate {} requirements",
            spec.module
        ));
    }
    if found.1 {
        return Err(format!(
            "{} must be a direct requirement, not // indirect",
            spec.module
        ));
    }
    if found.0 != exact {
        return Err(format!(
            "{} pin drift: expected {exact}, found {}",
            spec.universe, found.0
        ));
    }
    Ok(())
}

fn exact_sums(go_sum: &str, spec: ModuleSpec) -> Result<(String, String), String> {
    let source_prefix = format!("{} {} ", spec.module, spec.version);
    let mod_prefix = format!("{} {}/go.mod ", spec.module, spec.version);
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
            "go.sum must contain exactly one source and go.mod sum for {} {}",
            spec.module, spec.version
        ));
    }
    if source[0] != spec.source_sum || module[0] != spec.go_mod_sum {
        return Err(format!(
            "{} sum drift: expected {}/{}, found {}/{}",
            spec.universe, spec.source_sum, spec.go_mod_sum, source[0], module[0]
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
        if fields.len() != 8 {
            return Err(format!(
                "AST helper line {} has {} fields, expected 8",
                index + 1,
                fields.len()
            ));
        }
        let path = fields[0].to_owned();
        let file_sha256 = hash_by_path
            .get(&path)
            .ok_or_else(|| format!("AST helper returned unknown test file {path}"))?
            .clone();
        let suite_parents = if fields[7] == "-" {
            Vec::new()
        } else {
            let parents: Vec<_> = fields[7].split(',').map(str::to_owned).collect();
            if parents.iter().any(String::is_empty)
                || parents.windows(2).any(|pair| pair[0] >= pair[1])
            {
                return Err(format!(
                    "AST helper row {} has unsorted or duplicate suite parents",
                    index + 1
                ));
            }
            parents
        };
        if (fields[5] == "TestSuiteMethod") != !suite_parents.is_empty() {
            return Err(format!(
                "AST helper row {} has inconsistent suite category and parents",
                index + 1
            ));
        }
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
            actionable: fields[6]
                .parse()
                .map_err(|_| format!("invalid AST obligation flag at helper row {}", index + 1))?,
            suite_parents,
            file_sha256,
        });
    }
    Ok(result)
}

fn offline_download(repo: &Path, spec: ModuleSpec) -> Result<(PathBuf, String, String), String> {
    let output = Command::new("go")
        .current_dir(repo)
        .env("GOPROXY", "off")
        .env("GOSUMDB", "off")
        .args([
            "mod",
            "download",
            "-json",
            &format!("{}@{}", spec.module, spec.version),
        ])
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
    if field("Path")? != spec.module || field("Version")? != spec.version {
        return Err("go mod download resolved a different module or version".to_owned());
    }
    Ok((
        PathBuf::from(field("Dir")?),
        field("Sum")?,
        field("GoModSum")?,
    ))
}

fn render_module(
    repo: &Path,
    go_mod: &str,
    go_sum: &str,
    spec: ModuleSpec,
) -> Result<Inventory, String> {
    exact_direct_pin(go_mod, spec)?;
    let (source_sum, go_mod_sum) = exact_sums(go_sum, spec)?;
    let (module_root, downloaded_sum, downloaded_go_mod_sum) = offline_download(repo, spec)?;
    if downloaded_sum != source_sum || downloaded_go_mod_sum != go_mod_sum {
        return Err(format!(
            "offline module sums differ from go.sum: download {downloaded_sum}/{downloaded_go_mod_sum}, go.sum {source_sum}/{go_mod_sum}"
        ));
    }
    let expected_root = module_cache()?.join(format!("{}@{}", spec.module, spec.version));
    if module_root != expected_root {
        return Err(format!(
            "offline module directory drift: expected {}, found {}",
            expected_root.display(),
            module_root.display()
        ));
    }
    if !module_root.is_dir() {
        return Err(format!(
            "cached {} module is missing at {}; populate GOMODCACHE before running this offline check",
            spec.universe, module_root.display()
        ));
    }
    let cached_go_mod = fs::read_to_string(module_root.join("go.mod"))
        .map_err(|error| format!("read cached {} go.mod: {error}", spec.universe))?;
    if !cached_go_mod
        .lines()
        .any(|line| line.trim() == format!("module {}", spec.module))
    {
        return Err(format!(
            "{} is not module {}",
            module_root.display(),
            spec.module
        ));
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
    let obligations = test_obligations(&declarations);
    let unique_obligation_anchors: BTreeSet<_> = obligations
        .iter()
        .map(|obligation| (&obligation.path, obligation.line, &obligation.name))
        .collect();
    if unique_obligation_anchors.len() != obligations.len() {
        return Err(format!(
            "{} test obligation anchors are not unique",
            spec.universe
        ));
    }
    let counts = (
        production.len(),
        test_files,
        declarations.len(),
        obligations.len(),
    );
    let expected = (
        spec.expected_production_sources,
        spec.expected_test_files,
        spec.expected_declarations,
        spec.expected_obligations,
    );
    if counts != expected {
        return Err(format!(
            "{} universe drift: expected production/test-files/declarations/obligations {expected:?}, found {counts:?}",
            spec.universe
        ));
    }
    let source_evidence = source_evidence(repo, spec, &production)?;
    let test_evidence = test_evidence(repo, spec, &obligations)?;

    let tree_sha256 = format!("{:x}", tree_hasher.finalize());
    let module = format!(
        "{}\t{}\t{}\t{source_sum}\t{go_mod_sum}\t{tree_sha256}\t{}\t{test_files}\t{}\t{}\n",
        spec.universe,
        spec.module,
        spec.version,
        production.len(),
        declarations.len(),
        obligations.len()
    );

    let mut sources = String::new();
    for (path, lines, hash) in &production {
        let (status, owner, artifact, note) =
            source_evidence
                .get(path)
                .map_or(("UNTRIAGED", "-", "-", "-"), |item| {
                    (
                        item.status.as_str(),
                        item.owner.as_str(),
                        item.artifact.as_str(),
                        item.note.as_str(),
                    )
                });
        sources.push_str(&format!(
            "{}\t{path}\t{lines}\t{hash}\t{status}\t{owner}\t{artifact}\t{note}\n",
            spec.universe
        ));
    }
    let mut declaration_text = String::new();
    for item in &declarations {
        declaration_text.push_str(&format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            spec.universe,
            item.path,
            item.line,
            item.column,
            item.receiver,
            item.name,
            item.category,
            item.actionable,
            if item.suite_parents.is_empty() {
                "-".to_owned()
            } else {
                item.suite_parents.join(",")
            },
            item.file_sha256
        ));
    }
    let mut tests = String::new();
    for item in &obligations {
        let key = (item.path.clone(), item.line, item.name.clone());
        let (status, owner, artifact, note) =
            test_evidence
                .get(&key)
                .map_or(("UNTRIAGED", "-", "-", "-"), |evidence| {
                    (
                        evidence.status.as_str(),
                        evidence.owner.as_str(),
                        evidence.artifact.as_str(),
                        evidence.note.as_str(),
                    )
                });
        tests.push_str(&format!(
            "{}\t{}\t{}\t{}\t{}\ttransaction\t{}\t{status}\t{owner}\t{artifact}\t{note}\n",
            spec.universe, item.category, item.path, item.line, item.name, item.file_sha256
        ));
    }
    let source_keys = production
        .iter()
        .map(|(path, _, _)| format!("{}::{path}", spec.universe))
        .collect();
    let test_keys = obligations
        .iter()
        .map(|item| {
            format!(
                "{}::{}:{}:{}",
                spec.universe, item.path, item.line, item.name
            )
        })
        .collect();
    Ok(Inventory {
        module,
        sources,
        declarations: declaration_text,
        tests,
        production_sources: production.len(),
        test_files,
        declaration_count: declarations.len(),
        obligation_count: unique_obligation_anchors.len(),
        source_keys,
        test_keys,
    })
}

fn insert_unique(
    seen: &mut BTreeSet<String>,
    keys: impl IntoIterator<Item = String>,
    kind: &str,
) -> Result<(), String> {
    for key in keys {
        if !seen.insert(key.clone()) {
            return Err(format!("duplicate qualified external Go {kind} key {key}"));
        }
    }
    Ok(())
}

fn render(repo: &Path) -> Result<Inventory, String> {
    let go_mod = fs::read_to_string(repo.join("go.mod")).map_err(|error| error.to_string())?;
    let go_sum = fs::read_to_string(repo.join("go.sum")).map_err(|error| error.to_string())?;
    let mut combined = Inventory {
        module: String::from("# universe\tmodule\tversion\tgo_sum\tgo_mod_sum\tgo_file_tree_sha256\tproduction_sources\ttest_files\ttest_declarations\ttest_obligations\n"),
        sources: String::from("# universe\tsource_path\tline_count\tfile_sha256\tstatus\towner\tartifact\tnote\n"),
        declarations: String::from("# universe\tsource_path\tsource_line\tsource_column\treceiver\tfunction_name\tcategory\tactionable_test_obligation\tsuite_parents\tfile_sha256\n"),
        tests: String::from("# universe\tkind\tsource_path\tsource_line\tfunction_name\tring\tfile_sha256\tstatus\towner\tartifact\tnote\n"),
        production_sources: 0,
        test_files: 0,
        declaration_count: 0,
        obligation_count: 0,
        source_keys: Vec::new(),
        test_keys: Vec::new(),
    };
    let mut source_keys = BTreeSet::new();
    let mut test_keys = BTreeSet::new();
    for spec in MODULES {
        let inventory = render_module(repo, &go_mod, &go_sum, spec)?;
        insert_unique(&mut source_keys, inventory.source_keys, "source")?;
        insert_unique(&mut test_keys, inventory.test_keys, "test")?;
        combined.module.push_str(&inventory.module);
        combined.sources.push_str(&inventory.sources);
        combined.declarations.push_str(&inventory.declarations);
        combined.tests.push_str(&inventory.tests);
        combined.production_sources += inventory.production_sources;
        combined.test_files += inventory.test_files;
        combined.declaration_count += inventory.declaration_count;
        combined.obligation_count += inventory.obligation_count;
    }
    combined.source_keys = source_keys.into_iter().collect();
    combined.test_keys = test_keys.into_iter().collect();
    Ok(combined)
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
        return Err("usage: external_go_ledger --write | --check | --summary".to_owned());
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
                fs::write(path, content)
                    .map_err(|error| format!("write {}: {error}", path.display()))?;
            }
        }
        "--check" => {
            for ((path, label), content) in ledger_paths(&repo).iter().zip(rendered) {
                let current = fs::read_to_string(path).unwrap_or_default();
                if current != *content {
                    return Err(format!("{label} is stale; run external_go_ledger --write"));
                }
            }
        }
        "--summary" => {
            for spec in MODULES {
                println!(
                    "module\t{}\t{}\t{}",
                    spec.universe, spec.module, spec.version
                );
            }
            println!(
                "production_sources\t{}\ntest_files\t{}\ntest_declarations\t{}\ntest_obligations\t{}",
                inventory.production_sources,
                inventory.test_files,
                inventory.declaration_count,
                inventory.obligation_count
            );
        }
        _ => unreachable!(),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        exact_direct_pin, exact_sums, insert_unique, source_evidence, test_obligations,
        Declaration, MODULES,
    };
    use std::collections::BTreeSet;
    use std::fs;

    #[test]
    fn suite_methods_expand_once_per_reachable_parent() {
        let declarations = [Declaration {
            path: "suite_test.go".to_owned(),
            line: 42,
            column: 1,
            receiver: "method".to_owned(),
            name: "TestCase".to_owned(),
            category: "TestSuiteMethod".to_owned(),
            actionable: true,
            suite_parents: vec!["TestConfiguredA".to_owned(), "TestConfiguredB".to_owned()],
            file_sha256: "hash".to_owned(),
        }];
        let obligations = test_obligations(&declarations);
        assert_eq!(obligations.len(), 2);
        assert_eq!(obligations[0].name, "TestConfiguredA/TestCase");
        assert_eq!(obligations[1].name, "TestConfiguredB/TestCase");
    }

    #[test]
    fn direct_pin_rejects_version_drift_and_indirect_only_rows() {
        let spec = MODULES[0];
        assert!(
            exact_direct_pin(&format!("require {} {}\n", spec.module, spec.version), spec).is_ok()
        );
        assert!(exact_direct_pin(&format!("require {} v0.0.0\n", spec.module), spec).is_err());
        assert!(exact_direct_pin(
            &format!("require {} {} // indirect\n", spec.module, spec.version),
            spec
        )
        .is_err());
        assert!(exact_direct_pin(
            &format!(
                "require {} {}\nreplace {} => ../client-go\n",
                spec.module, spec.version, spec.module
            ),
            spec
        )
        .is_err());
    }

    #[test]
    fn sums_must_be_complete_and_unique() {
        let spec = MODULES[0];
        let sums = format!(
            "{} {} {}\n{} {}/go.mod {}\n",
            spec.module, spec.version, spec.source_sum, spec.module, spec.version, spec.go_mod_sum
        );
        assert_eq!(
            exact_sums(&sums, spec),
            Ok((spec.source_sum.to_owned(), spec.go_mod_sum.to_owned()))
        );
        assert!(exact_sums(
            &format!("{} {} {}\n", spec.module, spec.version, spec.source_sum),
            spec
        )
        .is_err());
        assert!(exact_sums(&sums.replace(spec.source_sum, "h1:wrong"), spec)
            .unwrap_err()
            .contains("sum drift"));
    }

    #[test]
    fn consolidated_keys_reject_duplicates_instead_of_overwriting() {
        let mut seen = BTreeSet::new();
        insert_unique(&mut seen, ["client-go::a.go".to_owned()], "source").unwrap();
        assert!(
            insert_unique(&mut seen, ["client-go::a.go".to_owned()], "source")
                .unwrap_err()
                .contains("duplicate qualified external Go source key")
        );
    }

    #[test]
    fn checked_source_overlay_promotes_only_an_exact_generated_anchor() {
        let repo =
            std::env::temp_dir().join(format!("tidb-client-go-ledger-{}", std::process::id()));
        let directory = repo.join("rust/difftests/corpus/coverage/evidence/client-go/source");
        fs::remove_dir_all(&repo).ok();
        fs::create_dir_all(&directory).unwrap();
        fs::create_dir_all(repo.join("rust/evidence")).unwrap();
        fs::write(repo.join("rust/evidence/client.rs"), "evidence").unwrap();
        fs::write(
            directory.join("owner.tsv"),
            "internal/client/client.go\tPARTIAL\towner\trust/evidence/client.rs\tbounded\n",
        )
        .unwrap();
        let production = vec![(
            "internal/client/client.go".to_owned(),
            10,
            "hash".to_owned(),
        )];
        let overlay = source_evidence(&repo, MODULES[0], &production).unwrap();
        assert_eq!(overlay["internal/client/client.go"].status, "PARTIAL");

        fs::write(
            directory.join("owner.tsv"),
            "internal/client/missing.go\tPARTIAL\towner\trust/evidence/client.rs\tstale\n",
        )
        .unwrap();
        assert!(source_evidence(&repo, MODULES[0], &production)
            .unwrap_err()
            .contains("stale client-go source path"));
        fs::remove_dir_all(repo).unwrap();
    }
}
