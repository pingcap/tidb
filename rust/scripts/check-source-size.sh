#!/usr/bin/env bash
# The source-file size ratchet: no file grows huge SILENTLY again.
#
# Size is not the sin -- growth nobody DECIDED is. A file not listed in
# scripts/source_size_bounds.txt may not exceed the soft limit; a listed file
# may not exceed its recorded bound (growing one = raise the bound in the same
# commit, with the reason, next to the entry); a listed file at or below the
# soft limit MUST have its entry removed, so the table only ratchets down.
#
# Fast path: run this script directly (no build). Enforced path: the
# difftest-result-tests `source_size_ratchet` test shells out to this script,
# so `cargo test --workspace` still gates it. ONE owner: the bounds file.
set -u
cd "$(dirname "$0")/.."

SOFT_LIMIT=2200
BOUNDS=scripts/source_size_bounds.txt
fail=0

while IFS= read -r file; do
  rel=${file#./}
  lines=$(wc -l < "$file" | tr -d ' ')
  limit=$(awk -v p="$rel" '$1 == p {print $2}' "$BOUNDS")
  if [ -n "$limit" ]; then
    if [ "$lines" -gt "$limit" ]; then
      echo "GREW: $rel: $lines lines, bound $limit. Growing it is a DECISION:"
      echo "      raise the bound in this same commit with the reason, or split the file."
      fail=1
    elif [ "$lines" -le "$SOFT_LIMIT" ]; then
      echo "RETIRE: $rel: $lines lines (bound $limit) -- now within the soft limit."
      echo "        Remove its entry from $BOUNDS so the table ratchets down."
      fail=1
    fi
  elif [ "$lines" -gt "$SOFT_LIMIT" ]; then
    echo "NEW-HUGE: $rel: $lines lines, over the $SOFT_LIMIT-line limit for unlisted files."
    echo "          Split it into sibling modules rather than adding an entry."
    fail=1
  fi
done < <(find ./crates -name '*.rs' -type f)

# A bounds entry whose file no longer exists is stale.
while read -r path _; do
  case "$path" in ''|\#*) continue ;; esac
  if [ ! -f "$path" ]; then
    echo "STALE: $path is in $BOUNDS but does not exist -- remove the entry."
    fail=1
  fi
done < "$BOUNDS"

if [ "$fail" -eq 0 ]; then echo "source-size ratchet: OK"; fi
exit $fail
