#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Extract literal SQL assertions from the complete Go windows SQL test helpers.

Run from any directory. Dynamic matrix setup stays in upstream.rs; each action
retains its Go line number. Unrecognized test actions fail regeneration.
"""
import argparse
import hashlib
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "pkg/executor/windows/window_sql_test.go"
OUTPUT = ROOT / "rust/crates/tidb-session/src/tests_window/upstream.json"
FUNCTIONS = ["doTestWindowFunctions", "TestWindowFunctionsDataReference",
             "baseTestSlidingWindowFunctions", "TestIssue45964And46050",
             "TestVarSampAsAWindowFunction"]
STRING = r'"(?:[^"\\]|\\.)*"|`[^`]*`'
ACTION = re.compile(
    rf'tk\.MustExec\((?P<exec>{STRING})\)'
    rf'|tk\.MustQuery\((?P<query>{STRING})\)\.(?P<sort>Sort\(\)\.)?\s*'
    rf'Check\(\s*testkit\.Rows\((?P<rows>(?:\s*(?:{STRING})\s*,?\s*)*)\)\s*,?\s*\)'
    r'|tk\.Session\(\)\.GetSessionVars\(\)\.MaxChunkSize = (?P<chunk>\d+)'
    rf'|testReturnColumnNullableAttribute\(tk, (?P<nullable>{STRING}), (?P<flag>true|false)\)'
)


def literal(value):
    return value[1:-1] if value.startswith("`") else json.loads(value)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="reject stale generated fixtures")
    args = parser.parse_args()
    source = SOURCE.read_text()
    suites = {}
    for name in FUNCTIONS:
        start = source.index("func " + name + "(")
        end = source.find("\nfunc ", start + 1)
        body = source[start:end if end != -1 else len(source)]
        # Defer changes session state only on return, after all assertions.
        deferred = re.search(r'\tdefer func\(\) \{(.*?)\n\t\}\(\)', body, re.S)
        deferred_actions = []
        if deferred:
            deferred_actions = [literal(m.group("exec")) for m in ACTION.finditer(deferred[1])]
            body = body[:deferred.start()] + re.sub(r'[^\n]', ' ', deferred[0]) + body[deferred.end():]
        actions = []
        for match in ACTION.finditer(body):
            action = {"line": source.count("\n", 0, start) + body.count("\n", 0, match.start()) + 1}
            if match["exec"]:
                action["sql"] = literal(match["exec"])
            elif match["query"]:
                action.update(sql=literal(match["query"]), sort=bool(match["sort"]),
                              rows=[literal(s) for s in re.findall(STRING, match["rows"])])
            elif match["chunk"]:
                action["chunk"] = int(match["chunk"])
            else:
                action.update(nullable=literal(match["nullable"]), flag=match["flag"] == "true")
            actions.append(action)
        rest = ACTION.sub("", body)
        if re.search(r'tk\.Must(?:Exec|Query)|MaxChunkSize|testReturnColumnNullableAttribute\(tk,', rest):
            raise ValueError(f"Unrecognized test action in {name}: {rest}")
        actions.extend({"sql": sql, "deferred": True} for sql in deferred_actions)
        suites[name] = actions
    generated = json.dumps({"source": str(SOURCE.relative_to(ROOT)),
                                 "sha256": hashlib.sha256(SOURCE.read_bytes()).hexdigest(),
                                 "suites": suites}, indent=2) + "\n"
    if args.check:
        if OUTPUT.read_text() != generated:
            raise SystemExit("upstream.json is stale; regenerate with this script")
    else:
        OUTPUT.write_text(generated)
    print(f"Extracted {sum(map(len, suites.values()))} actions in {len(suites)} helpers")


if __name__ == "__main__":
    main()
