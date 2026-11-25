#!/usr/bin/env python3
"""Generate Rust GBK and GB18030 lookup tables using TiDB's Go charset package."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "rust/crates/tidb-charset/src/charset_data"
TOOLCHAIN = "go1.27.4"

GO_SOURCE = r"""
package main

import (
    "os"
    "path/filepath"
    "unicode/utf8"

    "github.com/pingcap/tidb/pkg/parser/charset"
)

func write(name string, contents []byte) {
    if err := os.WriteFile(filepath.Join(os.Args[1], name), contents, 0644); err != nil {
        panic(err)
    }
}

func encode(name string, width int) []byte {
    encoding := charset.FindEncoding(name)
    table := make([]byte, (1<<16)*width)
    for r := rune(0x80); r < 1<<16; r++ {
        if !utf8.ValidRune(r) {
            continue
        }
        result, err := encoding.Transform(nil, []byte(string(r)), charset.OpEncode)
        if err != nil {
            if name == "gb18030" {
                panic(err)
            }
            continue
        }
        if len(result) != 2 && len(result) != width {
            panic("unexpected encoded width")
        }
        end := (int(r)+1)*width
        copy(table[end-len(result):end], result)
    }
    return table
}

func decode() []byte {
    const highBytes = 0x80
    encoding := charset.FindEncoding("gbk")
    table := make([]byte, 5*highBytes*(1+256))
    collect := func(offset int, input []byte) {
        for pass, op := range []charset.Op{0, 1 << 1} {
            encoding.Foreach(input, op, func(from, to []byte, valid bool) bool {
                if len(from) != len(input) || len(to) > 4 {
                    panic("unexpected decoded group")
                }
                entry := table[5*offset : 5*(offset+1)]
                if valid {
                    entry[0] |= 1 << (3+pass)
                }
                entry[0] |= byte(len(to))
                copy(entry[1:], to)
                return true
            })
        }
    }
    for lead := 0; lead < highBytes; lead++ {
        collect(lead, []byte{byte(lead+0x80)})
        for trail := 0; trail < 256; trail++ {
            collect(highBytes+lead*256+trail, []byte{byte(lead+0x80), byte(trail)})
        }
    }
    return table
}

func main() {
    write("gbk_encode.bin", encode("gbk", 2))
    write("gb18030_encode.bin", encode("gb18030", 4))
    write("gbk_decode.bin", decode())
}
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="check the existing tables")
    args = parser.parse_args()
    env = dict(os.environ, GOTOOLCHAIN=TOOLCHAIN)
    with tempfile.TemporaryDirectory(prefix="tidb-gb-tables-") as directory:
        scratch = Path(directory)
        source = scratch / "main.go"
        source.write_text(GO_SOURCE, encoding="utf-8")
        subprocess.run(
            ["go", "run", "-mod=readonly", str(source), directory],
            check=True,
            cwd=ROOT,
            env=env,
        )
        for name in ("gbk_encode.bin", "gb18030_encode.bin", "gbk_decode.bin"):
            generated = (scratch / name).read_bytes()
            path = DATA / name
            if args.check:
                if not path.exists() or path.read_bytes() != generated:
                    raise SystemExit(f"{path} differs from Go")
            else:
                path.write_bytes(generated)


if __name__ == "__main__":
    main()
