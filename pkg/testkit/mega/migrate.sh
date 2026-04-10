#!/bin/bash
# migrate.sh - Migrate external test files to mega test structure
#
# Usage:
#   ./migrate.sh <pkg> <test_file>
# Example:
#   ./migrate.sh ddl partition_test.go

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

PKG_NAME="$1"
TEST_FILE="$2"

if [ -z "$PKG_NAME" ] || [ -z "$TEST_FILE" ]; then
    echo "Usage: $0 <pkg> <test_file>"
    echo "Example: $0 ddl partition_test.go"
    exit 1
fi

# Find the test file in the package directory
PKG_DIR="$REPO_ROOT/pkg/$PKG_NAME"
TEST_PATH="$PKG_DIR/$TEST_FILE"

if [ ! -f "$TEST_PATH" ]; then
    echo "Error: Test file not found: $TEST_PATH"
    exit 1
fi

# Create test directory if it doesn't exist
TEST_DIR="$PKG_DIR/test"
mkdir -p "$TEST_DIR"

# Get the base name without _test.go suffix
BASE_NAME="${TEST_FILE%_test.go}"
GO_FILE="$BASE_NAME.go"

# Destination path
DEST_PATH="$TEST_DIR/$GO_FILE"

# Check if it's an external test file (package xxx_test)
if ! grep -q "^package ${PKG_NAME}_test" "$TEST_PATH"; then
    echo "Error: $TEST_PATH is not an external test file (package ${PKG_NAME}_test)"
    exit 1
fi

echo "Migrating $TEST_PATH -> $DEST_PATH"

# Copy the file and transform it
# 1. Keep the package declaration the same (already xxx_test)
# 2. Transform TestXXX(t *testing.T) -> RunXXX(t *testing.T)
#    We need to remove the "Test" prefix, not just prepend "Run"
awk '
/^func Test[A-Z]/ {
    # Replace "func TestXXX(" with "func RunXXX("
    gsub(/^func Test/, "func Run")
}
{ print }
' "$TEST_PATH" > "$DEST_PATH"

# Run mega-gen
echo "Running mega-gen..."
cd "$REPO_ROOT"
go run ./pkg/testkit/mega/gen/main.go \
    -pkg "$PKG_NAME" \
    -output "pkg/$PKG_NAME/test" \
    -mega-dir "pkg/testkit/mega"

echo ""
echo "Migration complete!"
echo "Generated files:"
echo "  - $DEST_PATH (transformed test logic)"
echo "  - $TEST_DIR/${BASE_NAME}_test.go (generated wrapper)"
echo "  - $TEST_DIR/register_gen.go (generated registry)"
echo ""
echo "Next steps:"
echo "  1. Review the generated files"
echo "  2. Test locally: go test ./pkg/$PKG_NAME/test/"
echo "  3. If OK, remove original: rm $TEST_PATH"
