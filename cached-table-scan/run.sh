#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

START_FROM="${1:-1}"

# Find all WORKxx.md files and sort them numerically
WORK_FILES=($(ls "$SCRIPT_DIR"/WORK*.md 2>/dev/null | sort -t'K' -k2 -n))

if [ ${#WORK_FILES[@]} -eq 0 ]; then
    echo "No WORK*.md files found in $SCRIPT_DIR"
    exit 1
fi

echo "Found ${#WORK_FILES[@]} work files, starting from #${START_FROM}:"
for i in "${!WORK_FILES[@]}"; do
    idx=$((i + 1))
    marker=" "
    if [ "$idx" -ge "$START_FROM" ]; then
        marker="*"
    fi
    echo "  ${marker} ${idx}. $(basename "${WORK_FILES[$i]}")"
done
echo ""

for i in "${!WORK_FILES[@]}"; do
    idx=$((i + 1))
    if [ "$idx" -lt "$START_FROM" ]; then
        echo "Skipping: $(basename "${WORK_FILES[$i]}")"
        continue
    fi
    f="${WORK_FILES[$i]}"
    name="$(basename "$f")"
    echo "=========================================="
    echo "Processing: $name"
    echo "=========================================="

    prompt="$(cat "$f")"

    claude --model=claude-opus-4-6 --print --permission-mode=plan --dangerously-skip-permissions -p "$prompt" 2>&1
    claude --model=claude-opus-4-6 --print --permission-mode=plan --dangerously-skip-permissions -p "If there are uncommitted contents, generate message and commit it, do nothing else." 2>&1

    echo ""
    echo "Completed: $name"
    echo ""

done

echo "All work files processed."
