find vendor \( -type f -or -type l \)  -not -name "*.go" -not -name "LICENSE" -not -name "*.s" -not -name "PATENTS" -not -name "*.h" -not -name "*.c" | xargs -I {} rm {}
# delete all test files
find vendor -type f -name "*_generated.go" | xargs -I {} rm {}
find vendor -type f -name "*_test.go" | xargs -I {} rm {}
