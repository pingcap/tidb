// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/objstore/hdfs.go`.
use super::storage::{Reader, Storage};
use std::{
    io,
    io::Write,
    path::Path,
    process::{Command, Stdio},
    time::Duration,
};

const UNSUPPORTED: &str = "currently HDFS backend only support rawkv backup: [BR:Common:ErrUnsupportedOperation]the operation is not supported";
const MISSING_HOME: &str = "please specify environment variable HADOOP_HOME: [BR:Common:ErrEnvNotSpecified]environment variable not found";

/// Go `HDFSStorage`.
pub struct HDFSStorage {
    remote: String,
}
impl HDFSStorage {
    /// Go `NewHDFSStorage`.
    pub fn new(remote: String) -> Self {
        Self { remote }
    }
    /// Go `getHdfsBin`, `getLinuxUser`, `dfsCommand`.
    fn dfs_command(args: &[&str]) -> io::Result<Command> {
        let home = std::env::var_os("HADOOP_HOME").ok_or_else(|| io::Error::other(MISSING_HOME))?;
        let binary = Path::new(&home).join("bin/hdfs");
        let mut command = if let Some(user) = std::env::var_os("HADOOP_LINUX_USER") {
            let mut command = Command::new("sudo");
            command.arg("-u").arg(user).arg(binary);
            command
        } else {
            Command::new(binary)
        };
        command.arg("dfs").args(args);
        Ok(command)
    }
    /// Go `exec.Cmd.CombinedOutput`; redact any storage URI echoed by the CLI.
    fn run(&self, args: &[&str], input: Option<&[u8]>) -> io::Result<std::process::ExitStatus> {
        let mut command = Self::dfs_command(args)?;
        // A common pipe preserves the command's combined stdout/stderr order.
        let (mut output, pipe) = std::os::unix::net::UnixStream::pair()?;
        let stderr = pipe.try_clone()?;
        command.stdout(Stdio::from(std::os::fd::OwnedFd::from(pipe)));
        command.stderr(Stdio::from(std::os::fd::OwnedFd::from(stderr)));
        command.stdin(if input.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        });
        let mut child = command.spawn()?;
        drop(command);
        let data = input.map(<[u8]>::to_vec);
        let mut stdin = child.stdin.take();
        let writer = std::thread::spawn(move || {
            if let (Some(stdin), Some(data)) = (&mut stdin, data) {
                let _ = stdin.write_all(&data);
            }
        });
        let mut bytes = Vec::new();
        io::Read::read_to_end(&mut output, &mut bytes)?;
        let _ = writer.join();
        let status = child.wait()?;
        if !status.success() && args.first() == Some(&"-put") {
            let diagnostic = String::from_utf8_lossy(&bytes)
                .replace(&self.remote, &tidb_ast::redact_url(&self.remote));
            let status = status
                .code()
                .map_or_else(|| status.to_string(), |code| format!("exit status {code}"));
            return Err(io::Error::other(format!("{diagnostic}: {status}")));
        }
        Ok(status)
    }
}
impl Storage for HDFSStorage {
    /// Go `HDFSStorage.URI`.
    fn uri(&self) -> String {
        self.remote.clone()
    }
    /// Go `HDFSStorage.WriteFile`.
    fn write_file(&self, name: &str, data: &[u8]) -> io::Result<()> {
        self.run(
            &["-put", "-", &format!("{}/{name}", self.remote)],
            Some(data),
        )
        .map(|_| ())
    }
    /// Go `HDFSStorage.ReadFile`.
    fn read_file(&self, _: &str) -> io::Result<Vec<u8>> {
        Err(io::Error::other(UNSUPPORTED))
    }
    /// Go `HDFSStorage.FileExists`: a nonzero child exit means absent.
    fn file_exists(&self, name: &str) -> io::Result<bool> {
        self.run(&["-ls", &format!("{}/{name}", self.remote)], None)
            .map(|status| status.success())
    }
    /// Go `HDFSStorage.DeleteFile`.
    fn delete_file(&self, _: &str) -> Result<(), String> {
        Err(UNSUPPORTED.into())
    }
    /// Go `HDFSStorage.DeleteFiles`.
    fn delete_files(&self, _: &[&str]) -> Result<(), String> {
        Err(UNSUPPORTED.into())
    }
    /// Go `HDFSStorage.Open`.
    fn open(&self, _: &str) -> io::Result<Box<dyn Reader>> {
        Err(io::Error::other(UNSUPPORTED))
    }
    /// Go `HDFSStorage.Create`.
    fn create_writer(&self, _: &Path) -> io::Result<Box<dyn tidb_replayer::ObjectWriter + Send>> {
        Err(io::Error::other(UNSUPPORTED))
    }
    /// Go `HDFSStorage.Rename`.
    fn rename(&self, _: &str, _: &str) -> io::Result<()> {
        Err(io::Error::other(UNSUPPORTED))
    }
    /// Go `HDFSStorage.WalkDir`.
    fn walk_dir(&self, _: &str) -> io::Result<Vec<String>> {
        Err(io::Error::other(UNSUPPORTED))
    }
    /// Go `HDFSStorage.PresignFile`.
    fn presign_file(&self, _: &str, _: Duration) -> Result<String, String> {
        Err("HDFS backend does not support PresignFile".into())
    }
    /// Go `HDFSStorage.Close`.
    fn close(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Go `zzprobe/o15_hdfs.TestHDFSStorage`.
    #[test]
    fn test_hdfs_storage() {
        use std::os::unix::fs::PermissionsExt as _;
        let s = crate::new_ext_storage("hdfs://host:9000/base", "tenant").unwrap();
        assert_eq!(s.uri(), "hdfs://host:9000/base/tenant");
        assert_eq!(s.read_file("x").unwrap_err().to_string(), UNSUPPORTED);
        assert_eq!(
            s.presign_file("x", Duration::ZERO).unwrap_err(),
            "HDFS backend does not support PresignFile"
        );
        assert_eq!(s.delete_file("x").unwrap_err(), UNSUPPORTED);
        assert_eq!(s.delete_files(&["x"]).unwrap_err(), UNSUPPORTED);
        assert_eq!(s.open("x").err().unwrap().to_string(), UNSUPPORTED);
        assert_eq!(
            s.create_writer(Path::new("x")).err().unwrap().to_string(),
            UNSUPPORTED
        );
        assert_eq!(s.rename("x", "y").unwrap_err().to_string(), UNSUPPORTED);
        assert_eq!(s.walk_dir("").unwrap_err().to_string(), UNSUPPORTED);
        match std::env::var("O15_HDFS_TEST_CHILD").as_deref() {
            Ok("run") => {
                let home = std::path::PathBuf::from(std::env::var_os("HADOOP_HOME").unwrap());
                s.write_file("item", b"payload").unwrap();
                assert_eq!(std::fs::read(home.join("data")).unwrap(), b"payload");
                assert_eq!(
                    std::fs::read_to_string(home.join("args")).unwrap(),
                    "dfs\n-put\n-\nhdfs://host:9000/base/tenant/item\n"
                );
                assert!(s.file_exists("item").unwrap());
                assert!(!s.file_exists("missing").unwrap());
                return;
            }
            Ok("missing") => {
                assert_eq!(s.file_exists("item").unwrap_err().to_string(), MISSING_HOME);
                return;
            }
            _ => {}
        }
        let home = std::env::temp_dir().join(format!("o15-hdfs-{}", uuid::Uuid::new_v4()));
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            /// Go `TestHDFSStorage` temporary directory cleanup.
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(home.clone());
        std::fs::create_dir_all(home.join("bin")).unwrap();
        let script=b"#!/bin/sh\ncase \"$2\" in\n-put) cat > \"$HADOOP_HOME/data\"; printf '%s\\n' \"$@\" > \"$HADOOP_HOME/args\";;\n-ls) case \"$3\" in */missing) exit 1;; esac;;\nesac\n";
        std::fs::write(home.join("bin/hdfs"), script).unwrap();
        std::fs::set_permissions(
            home.join("bin/hdfs"),
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        for mode in ["run", "missing"] {
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .args(["--exact", "hdfs::tests::test_hdfs_storage", "--nocapture"])
                .env("O15_HDFS_TEST_CHILD", mode)
                .env_remove("HADOOP_LINUX_USER");
            if mode == "run" {
                command.env("HADOOP_HOME", &home);
            } else {
                command.env_remove("HADOOP_HOME");
            }
            let output = command.output().unwrap();
            assert!(
                output.status.success(),
                "child failed: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        s.close();
    }
}
