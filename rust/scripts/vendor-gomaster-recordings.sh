#!/usr/bin/env bash
# Vendors Go master's integrationtest scripts and recordings for the enrolled
# replay topics whose recording master changed, into
# difftests/result-tests/testdata/gomaster/{t,r}/, and writes the commit to
# its PIN file. The replay accepts a statement that diverges from this
# branch's recording when it matches master's (`MatchKind::GoMaster`), so a
# ported Go-master change is checked against master's own recording.
#
# Usage: rust/scripts/vendor-gomaster-recordings.sh <go-master-checkout>
set -euo pipefail

master=${1:?usage: $0 <go-master-checkout>}
rust=$(cd "$(dirname "$0")/.." && pwd)
repo=$(cd "$rust/.." && pwd)
out="$rust/difftests/result-tests/testdata/gomaster"
pin=$(git -C "$master" rev-parse --short=11 HEAD)

rm -rf "$out"
mkdir -p "$out"
python3 - "$rust/difftests/result-tests/src/enrolled_topics.rs" "$repo/tests/integrationtest" \
    "$master/tests/integrationtest" "$out" <<'EOF'
import os, re, shutil, sys
topics_rs, branch, master, out = sys.argv[1:]
topics = re.findall(r'^\s*"([^"]+)",', open(topics_rs).read(), re.M)
copied = 0
for topic in topics:
    directory = os.path.join(master, 'r', os.path.dirname(topic))
    # A topic master deleted keeps the branch's recording only.
    if not os.path.isdir(directory) or not os.path.exists(os.path.join(master, 't', topic + '.test')):
        continue
    for name in os.listdir(directory):
        base = os.path.basename(topic)
        # `<topic>.result`, or the `_enabled` / `_disabled` collation pair.
        if not re.fullmatch(re.escape(base) + r'(_enabled|_disabled)?\.result', name):
            continue
        rel = os.path.join(os.path.dirname(topic), name)
        mine = os.path.join(branch, 'r', rel)
        theirs = os.path.join(master, 'r', rel)
        if os.path.exists(mine) and open(mine, 'rb').read() == open(theirs, 'rb').read():
            continue
        for kind, path in (('r', rel), ('t', topic + '.test')):
            dst = os.path.join(out, kind, path)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copyfile(os.path.join(master, kind, path), dst)
        copied += 1
print(f"{copied} recordings differ from master")
EOF
echo "$pin" > "$out/PIN"
echo "pinned $pin"
