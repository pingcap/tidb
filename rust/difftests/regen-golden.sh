#!/usr/bin/env bash
# Regenerate the golden token dumps from the production Go scanner.
#
# Run from anywhere in the repo after changing a corpus file or the Go scanner.
# The Rust differential tests (cd rust && cargo test -p difftest) compare
# tidb-lexer's output against these goldens.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
corpus=rust/difftests/corpus

regen() { # <statements-file> <golden-file>
    echo "regenerating $2 from $1 ..."
    grep -v '^##' "${corpus}/$1" | go run ./rust/difftests/godump > "${corpus}/$2"
}

regen statements.txt      golden.txt
regen real_statements.txt real_golden.txt

echo "done. now run: (cd rust && cargo test -p difftest)"
