#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Find all WORKxx.md files and sort them numerically
WORK_FILES=($(ls "$SCRIPT_DIR"/WORK*.md 2>/dev/null | sort -t'K' -k2 -n))

if [ ${#WORK_FILES[@]} -eq 0 ]; then
    echo "No WORK*.md files found in $SCRIPT_DIR"
    exit 1
fi

echo "Found ${#WORK_FILES[@]} work files to process:"
for f in "${WORK_FILES[@]}"; do
    echo "  - $(basename "$f")"
done
echo ""

for f in "${WORK_FILES[@]}"; do
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

    sleep 1200
done

echo "All work files processed."
