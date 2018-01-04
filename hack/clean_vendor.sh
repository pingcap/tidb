find vendor -type f -not -name "*.go" -not -name "LICENSE" -not -name "*.s" | xargs -I {} rm {}
find vendor -type f -name "*_test.go" | xargs -I {} rm {}
