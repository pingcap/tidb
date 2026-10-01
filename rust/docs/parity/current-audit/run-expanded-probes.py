#!/usr/bin/env python3
"""Run the retained diagnostics without installing examples in production crates.

The output records observations, including incorrect behavior. Exit zero means
the diagnostics completed, not that Go/Rust parity passed. The server probe
opens an ephemeral localhost listener. Run from any directory.
"""
import argparse
import pathlib
import subprocess

HERE = pathlib.Path(__file__).resolve().parent
RUST = HERE.parents[2]


def run(crate, source, example, output):
    temporary = RUST / "crates" / crate / "examples" / f"{example}.rs"
    if temporary.exists():
        raise FileExistsError(temporary)
    created_directory = not temporary.parent.exists()
    temporary.parent.mkdir(exist_ok=True)
    temporary.write_bytes((HERE / source).read_bytes())
    try:
        log_path = pathlib.Path("/private/tmp") / f"{example}-build.log"
        with log_path.open("w") as log:
            result = subprocess.run(
                ["cargo", "run", "--locked", "-p", crate, "--example", example],
                cwd=RUST, stdout=subprocess.PIPE, stderr=log, text=True,
            )
        (HERE / output).write_text(result.stdout)
        print(f"{example}: exit {result.returncode}; output {output}; build log {log_path}")
        result.check_returncode()
    finally:
        temporary.unlink()
        if created_directory:
            temporary.parent.rmdir()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe", choices=("all", "ownership", "server", "structure"), default="all")
    selected = parser.parse_args().probe
    if selected in ("all", "ownership"):
        run("tidb-session", "expanded-ownership-probe.rs", "expanded_ownership_audit", "expanded-ownership-probe.txt")
    if selected in ("all", "server"):
        run("tidb-server", "expanded-server-probe.rs", "expanded_server_audit", "expanded-server-probe.txt")
    if selected in ("all", "structure"):
        run("tidb-session", "subsystem-structure-probe.rs", "subsystem_structure_audit", "subsystem-structure-probe.txt")
