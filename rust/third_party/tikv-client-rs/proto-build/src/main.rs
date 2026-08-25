// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashSet,
    ffi::{OsStr, OsString},
    fs, io,
    path::Path,
    process::Command,
};

const GENERATED_DIR: &str = "src/generated";
const DESCRIPTOR_FILE: &str = "file_descriptor_set.bin";
const PROTOC_VERSION: &str = "libprotoc 35.1";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    verify_protoc_version()?;
    let staging = tempfile::Builder::new()
        .prefix(".generated-")
        .tempdir_in("src")?;
    let mut protos = glob::glob("proto/*.proto")?.collect::<Result<Vec<_>, _>>()?;
    protos.sort();

    let mut prost = prost_build::Config::new();
    prost.enable_type_names();

    tonic_prost_build::configure()
        .emit_rerun_if_changed(false)
        .build_server(true)
        .server_mod_attribute(".", "#[allow(non_camel_case_types)]")
        .include_file("mod.rs")
        .out_dir(staging.path())
        .file_descriptor_set_path(staging.path().join(DESCRIPTOR_FILE))
        .compile_with_config(
            prost,
            &protos,
            &["proto/include".into(), "proto".into()],
        )?;

    install_generated_output(staging.path(), Path::new(GENERATED_DIR))?;
    Ok(())
}

fn verify_protoc_version() -> io::Result<()> {
    let output = Command::new("protoc").arg("--version").output()?;
    let actual = String::from_utf8_lossy(&output.stdout);
    if output.status.success() && protoc_version_is_supported(&actual) {
        return Ok(());
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!(
            "protocol generation requires {PROTOC_VERSION}; found {}",
            actual.trim()
        ),
    ))
}

fn protoc_version_is_supported(version: &str) -> bool {
    version.trim() == PROTOC_VERSION
}

fn install_generated_output(staging: &Path, destination: &Path) -> io::Result<()> {
    fs::create_dir_all(destination)?;

    let staged_files = fs::read_dir(staging)?
        .map(|entry| {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unexpected generated output: {}", entry.path().display()),
                ));
            }
            Ok((entry.file_name(), entry.path()))
        })
        .collect::<io::Result<Vec<_>>>()?;
    let expected = staged_files
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<HashSet<OsString>>();

    for (name, source) in staged_files {
        fs::copy(source, destination.join(name))?;
    }

    for entry in fs::read_dir(destination)? {
        let entry = entry?;
        let name = entry.file_name();
        if entry.file_type()?.is_file() && is_generated_output(&name) && !expected.contains(&name) {
            fs::remove_file(entry.path())?;
        }
    }

    Ok(())
}

fn is_generated_output(name: &OsStr) -> bool {
    name == OsStr::new(DESCRIPTOR_FILE) || Path::new(name).extension() == Some(OsStr::new("rs"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protoc_version_is_pinned_for_cross_platform_descriptors() {
        assert!(protoc_version_is_supported("libprotoc 35.1\n"));
        assert!(!protoc_version_is_supported("libprotoc 3.20.3\n"));
        assert!(!protoc_version_is_supported("libprotoc 35.2\n"));
    }

    #[test]
    fn installation_replaces_expected_and_removes_stale_generated_files() {
        let root = tempfile::tempdir().unwrap();
        let staging = root.path().join("staging");
        let destination = root.path().join("generated");
        fs::create_dir_all(&staging).unwrap();
        fs::create_dir_all(&destination).unwrap();

        fs::write(staging.join("mod.rs"), "new mod").unwrap();
        fs::write(staging.join("message.rs"), "new message").unwrap();
        fs::write(staging.join(DESCRIPTOR_FILE), b"new descriptor").unwrap();
        fs::write(destination.join("message.rs"), "old message").unwrap();
        fs::write(destination.join("span.rs"), "stale message").unwrap();
        fs::write(destination.join(DESCRIPTOR_FILE), b"old descriptor").unwrap();
        fs::write(destination.join("README.md"), "keep me").unwrap();

        install_generated_output(&staging, &destination).unwrap();

        assert_eq!(
            fs::read_to_string(destination.join("mod.rs")).unwrap(),
            "new mod"
        );
        assert_eq!(
            fs::read_to_string(destination.join("message.rs")).unwrap(),
            "new message"
        );
        assert_eq!(
            fs::read(destination.join(DESCRIPTOR_FILE)).unwrap(),
            b"new descriptor"
        );
        assert!(!destination.join("span.rs").exists());
        assert_eq!(
            fs::read_to_string(destination.join("README.md")).unwrap(),
            "keep me"
        );
    }

    #[test]
    fn installation_creates_a_missing_destination() {
        let root = tempfile::tempdir().unwrap();
        let staging = root.path().join("staging");
        let destination = root.path().join("generated");
        fs::create_dir_all(&staging).unwrap();
        fs::write(staging.join("mod.rs"), "generated").unwrap();

        install_generated_output(&staging, &destination).unwrap();

        assert_eq!(
            fs::read_to_string(destination.join("mod.rs")).unwrap(),
            "generated"
        );
    }
}
