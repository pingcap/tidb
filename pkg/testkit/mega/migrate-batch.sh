#!/bin/bash
# migrate-batch.sh - Migrate all external test files for a package to mega test structure
#
# Usage:
#   ./migrate-batch.sh <pkg>
# Example:
#   ./migrate-batch.sh ddl

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

PKG_NAME="$1"

if [ -z "$PKG_NAME" ]; then
    echo "Usage: $0 <pkg>"
    echo "Example: $0 ddl"
    exit 1
fi

# Find the package directory
PKG_DIR="$REPO_ROOT/pkg/$PKG_NAME"
if [ ! -d "$PKG_DIR" ]; then
    echo "Error: Package directory not found: $PKG_DIR"
    exit 1
fi

# Find all external test files
echo "Finding external test files for package $PKG_NAME..."
TEST_FILES=$(grep -l "^package ${PKG_NAME}_test" "$PKG_DIR"/*_test.go 2>/dev/null || true)

if [ -z "$TEST_FILES" ]; then
    echo "No external test files found for package $PKG_NAME"
    exit 0
fi

# Count files
FILE_COUNT=$(echo "$TEST_FILES" | wc -l | tr -d ' ')
echo "Found $FILE_COUNT external test files"

# Create test directory
TEST_DIR="$PKG_DIR/test"
mkdir -p "$TEST_DIR"

# Track all migrated files for later summary
MIGRATED_FILES=()
FAILED_FILES=()

# Process each test file
for TEST_FILE in $TEST_FILES; do
    BASE_NAME=$(basename "$TEST_FILE")
    echo ""
    echo "========================================"
    echo "Migrating: $BASE_NAME"
    echo "========================================"

    # Get the base name without _test.go suffix
    BASE_NAME_NO_SUFFIX="${BASE_NAME%_test.go}"
    GO_FILE="${BASE_NAME_NO_SUFFIX}.go"

    # Destination path
    DEST_PATH="$TEST_DIR/$GO_FILE"

    # Check if it's an external test file (package xxx_test)
    if ! grep -q "^package ${PKG_NAME}_test" "$TEST_FILE"; then
        echo "Warning: $BASE_NAME is not an external test file, skipping"
        FAILED_FILES+=("$BASE_NAME")
        continue
    fi

    # Transform TestXXX to RunXXX
    awk '
/^func Test[A-Z]/ {
    # Replace "func TestXXX(" with "func RunXXX("
    gsub(/^func Test/, "func Run")
}
{ print }
' "$TEST_FILE" > "$DEST_PATH"

    if [ $? -eq 0 ]; then
        MIGRATED_FILES+=("$BASE_NAME")
        echo "✓ Migrated: $BASE_NAME -> $GO_FILE"
    else
        FAILED_FILES+=("$BASE_NAME")
        echo "✗ Failed to migrate: $BASE_NAME"
        rm -f "$DEST_PATH"
    fi
done

echo ""
echo "========================================"
echo "Migration Summary"
echo "========================================"
echo "Successfully migrated: ${#MIGRATED_FILES[@]} files"
echo "Failed: ${#FAILED_FILES[@]} files"

if [ ${#FAILED_FILES[@]} -gt 0 ]; then
    echo ""
    echo "Failed files:"
    for file in "${FAILED_FILES[@]}"; do
        echo "  - $file"
    done
fi

# Now run mega-gen to generate wrapper files
echo ""
echo "========================================"
echo "Running mega-gen to generate wrappers..."
echo "========================================"

cd "$REPO_ROOT"
go run ./pkg/testkit/mega/gen/main.go \
    -pkg "$PKG_NAME" \
    -output "pkg/$PKG_NAME/test" \
    -mega-dir "pkg/testkit/mega"

echo ""
echo "========================================"
echo "Migration complete!"
echo "========================================"
echo ""
echo "Generated files:"
echo "  - $TEST_DIR/*.go (transformed test logic)"
echo "  - $TEST_DIR/*_test.go (generated wrappers)"
echo "  - $TEST_DIR/register_gen.go (generated registry)"
echo "  - pkg/testkit/mega/${PKG_NAME}_generated_test.go (mega test functions)"
echo ""
echo "Next steps:"
echo "  1. Review the generated files"
echo "  2. Test locally: go test ./pkg/$PKG_NAME/test/"
echo "  3. Test mega: go test ./pkg/testkit/mega/ -run Test_${PKG_NAME}_"
echo "  4. If OK, remove original files from $PKG_DIR"
