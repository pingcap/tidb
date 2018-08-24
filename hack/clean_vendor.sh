# delete internal vendor folders
find vendor -type d -name "_vendor" | xargs -I {} rm -r {}
# delete all files that are not go, c, h, or legal
find vendor -type f -not -name "*.go" -not -name "NOTICE*" -not -name "COPYING*"  -not -name "LICENSE*" -not -name "*.s" -not -name "PATENTS*" -not -name "*.h" -not -name "*.c" | xargs -I {} rm {}
# delete all generated files
find vendor -type f -name "*_generated.go" | xargs -I {} rm {}
# delete all test files
find vendor -type f -name "*_test.go" | xargs -I {} rm {}
find vendor -type d -name "fixtures" | xargs -I {} rm -r {}
# Delete documentation files. Keep doc.go.
find vendor -type d -name "Documentation" | xargs -I {} rm -r {}
find vendor -type d -name "tutorial" | xargs -I {} rm -r {}
find vendor -name "*.md" | xargs -I {} rm {}
# Delete unused languages
find vendor -type d -name "ruby" | xargs -I {} rm -r {}
