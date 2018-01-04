find vendor -type f -not -name "*.go" -not -name "LICENSE" -not -name "*.s" -not -name "PATENTS" | xargs -I {} ls {} > files
# delete all test files
find vendor -type f -name "*_test.go" | xargs -I {} rm {}
