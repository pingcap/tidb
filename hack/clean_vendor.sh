find vendor -type f -not -name "*.go" -not -name "LICENSE" | xargs -I {} rm {}
find vendor -type f -name "*_test.go" | xargs -I {} rm {}
