// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/bazelbuild/rules_go/go/tools/bazel"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/googleapi"
)

const gcpBucket = "pingcapmirror"

// downloadedModule captures `go mod download -json` output.
type downloadedModule struct {
	Path    string `json:"Path"`
	Sum     string `json:"Sum"`
	Version string `json:"Version"`
	Zip     string `json:"Zip"`
}

// listedModule captures `go list -m -json` output.
type listedModule struct {
	Path    string        `json:"Path"`
	Version string        `json:"Version"`
	Replace *listedModule `json:"Replace,omitempty"`
}

var (
	isMirror bool
	isUpload bool
)

func init() {
	flag.BoolVar(&isMirror, "mirror", false, "enable mirror mode")
	flag.BoolVar(&isUpload, "upload", false, "enable upload mode")
}

func formatSubURL(path, version string) string {
	return fmt.Sprintf("gomod/%s/%s-%s.zip", path, modulePathToBazelRepoName(path), version)
}

func formatVPCPublicURL(path, version string) string {
	return fmt.Sprintf("http://bazel-cache.pingcap.net:8080/%s", formatSubURL(path, version))
}

func formatVPCPrivateURL(path, version string) string {
	return fmt.Sprintf("http://ats.apps.svc/%s", formatSubURL(path, version))
}

func formatCDNURL(path, version string) string {
	return fmt.Sprintf("https://cache.hawkingrei.com/%s", formatSubURL(path, version))
}

func formatPublicURL(path, version string) string {
	return fmt.Sprintf("https://storage.googleapis.com/pingcapmirror/%s", formatSubURL(path, version))
}

func getSha256OfFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("failed to open %s: %w", path, err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}

func uploadFile(ctx context.Context, client *storage.Client, localPath, remotePath string) error {
	if !isUpload {
		return nil
	}
	in, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", localPath, err)
	}
	defer in.Close()
	out := client.Bucket(gcpBucket).Object(remotePath).If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		var gerr *googleapi.Error
		if errors.As(err, &gerr) {
			if gerr.Code == http.StatusPreconditionFailed {
				// In this case the "DoesNotExist" precondition
				// failed, i.e., the object does already exist.
				return nil
			}
			return gerr
		}
		return err
	}
	return nil
}

func createTmpDir() (tmpdir string, err error) {
	tmpdir, err = bazel.NewTmpDir("gomirror")
	if err != nil {
		return
	}
	err = os.MkdirAll(filepath.Join(tmpdir, "pkg/parser"), os.ModePerm)
	if err != nil {
		return
	}
	gomod, err := bazel.Runfile("go.mod")
	if err != nil {
		return
	}
	gosum, err := bazel.Runfile("go.sum")
	if err != nil {
		return
	}
	parsergomod := strings.Replace(gomod, "go.mod", "pkg/parser/go.mod", 1)
	parsergosum := strings.Replace(gosum, "go.sum", "pkg/parser/go.sum", 1)
	err = copyFile(gomod, filepath.Join(tmpdir, "go.mod"))
	if err != nil {
		return
	}
	err = copyFile(parsergomod, filepath.Join(tmpdir, "pkg/parser/go.mod"))
	if err != nil {
		return
	}
	err = copyFile(gosum, filepath.Join(tmpdir, "go.sum"))
	if err != nil {
		return
	}
	err = copyFile(parsergosum, filepath.Join(tmpdir, "pkg/parser/go.sum"))
	return
}

func downloadZips(
	tmpdir string, listed map[string]listedModule,
) (map[string]downloadedModule, error) {
	gobin, err := bazel.Runfile("bin/go")
	if err != nil {
		return nil, err
	}
	downloadArgs := make([]string, 0, len(listed)+3)
	downloadArgs = append(downloadArgs, "mod", "download", "-json")
	for _, mod := range listed {
		if mod.Replace != nil {
			if mod.Replace.Version == "" {
				continue
			}
			downloadArgs = append(downloadArgs, fmt.Sprintf("%s@%s", mod.Replace.Path, mod.Replace.Version))
		} else {
			downloadArgs = append(downloadArgs, fmt.Sprintf("%s@%s", mod.Path, mod.Version))
		}
	}
	cmd := exec.Command(gobin, downloadArgs...)
	cmd.Dir = tmpdir
	env := os.Environ()
	env = append(env, fmt.Sprintf("GOSUMDB=%s", "sum.golang.org"))
	cmd.Env = env
	jsonBytes, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	var jsonBuilder strings.Builder
	ret := make(map[string]downloadedModule)
	for _, line := range strings.Split(string(jsonBytes), "\n") {
		jsonBuilder.WriteString(line)
		if strings.HasPrefix(line, "}") {
			var mod downloadedModule
			if err := json.Unmarshal([]byte(jsonBuilder.String()), &mod); err != nil {
				return nil, err
			}
			ret[mod.Path] = mod
			jsonBuilder.Reset()
		}
	}
	return ret, nil
}

func listAllModules(tmpdir string) (map[string]listedModule, error) {
	gobin, err := bazel.Runfile("bin/go")
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(gobin, "list", "-mod=readonly", "-m", "-json", "all")
	cmd.Dir = tmpdir
	env := os.Environ()
	env = append(env, fmt.Sprintf("GOSUMDB=%s", "sum.golang.org"))
	cmd.Env = env
	jsonBytes, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	ret := make(map[string]listedModule)
	var jsonBuilder strings.Builder
	for _, line := range strings.Split(string(jsonBytes), "\n") {
		jsonBuilder.WriteString(line)
		if strings.HasPrefix(line, "}") {
			var mod listedModule
			if err := json.Unmarshal([]byte(jsonBuilder.String()), &mod); err != nil {
				return nil, err
			}
			jsonBuilder.Reset()
			if mod.Path == "github.com/pingcap/tidb" {
				continue
			}
			ret[mod.Path] = mod
		}
	}
	return ret, nil
}

func getExistingMirrors() (map[string]DownloadableArtifact, error) {
	depsbzl, err := bazel.Runfile("DEPS.bzl")
	if err != nil {
		return nil, err
	}
	return ListArtifactsInDepsBzl(depsbzl)
}

func mungeBazelRepoNameComponent(component string) string {
	component = strings.ReplaceAll(component, "-", "_")
	component = strings.ReplaceAll(component, ".", "_")
	return strings.ToLower(component)
}

func modulePathToBazelRepoName(mod string) string {
	components := strings.Split(mod, "/")
	head := strings.Split(components[0], ".")
	for i, j := 0, len(head)-1; i < j; i, j = i+1, j-1 {
		head[i], head[j] = mungeBazelRepoNameComponent(head[j]), mungeBazelRepoNameComponent(head[i])
	}
	for index, component := range components {
		if index == 0 {
			continue
		}
		components[index] = mungeBazelRepoNameComponent(component)
	}
	return strings.Join(append(head, components[1:]...), "_")
}

func dumpPatchArgsForRepo(repoName string) error {
	runfiles, err := bazel.RunfilesPath()
	if err != nil {
		return err
	}
	candidate := filepath.Join(runfiles, "build", "patches", repoName+".patch")
	if _, err := os.Stat(candidate); err == nil {
		fmt.Printf(`        patch_args = ["-p1"],
        patches = [
            "//build/patches:%s.patch",
        ],
`, repoName)
	} else if !os.IsNotExist(err) {
		return err
	}
	return nil
}

func buildFileProtoModeForRepo(repoName string) string {
	if repoName == "io_etcd_go_etcd_api_v3" {
		return "disable"
	}
	return "disable_global"
}

func dumpBuildNamingConventionArgsForRepo(repoName string) {
	if repoName == "com_github_grpc_ecosystem_grpc_gateway" {
		fmt.Printf("        build_naming_convention = \"go_default_library\",\n")
	}
}

func dumpNewDepsBzl(
	listed map[string]listedModule,
	downloaded map[string]downloadedModule,
	existingMirrors map[string]DownloadableArtifact,
) error {
	var sorted []string
	repoNameToModPath := make(map[string]string)
	for _, mod := range listed {
		repoName := modulePathToBazelRepoName(mod.Path)
		sorted = append(sorted, repoName)
		repoNameToModPath[repoName] = mod.Path
	}
	sort.Strings(sorted)

	ctx := context.Background()
	var client *storage.Client
	if isMirror && isUpload {
		var err error
		client, err = storage.NewClient(ctx)
		if err != nil {
			return err
		}
	}
	g, ctx := errgroup.WithContext(ctx)

	// This uses a lot of fmt.Println to output the generated configuration to stdout,
	// and the mirror will only be used under "make bazel_prepare", so it won't output
	// too much and affect development.
	fmt.Println(`load("@bazel_gazelle//:deps.bzl", "go_repository")

def go_deps():
    # NOTE: We ensure that we pin to these specific dependencies by calling
    # this function FIRST, before calls to pull in dependencies for
    # third-party libraries (e.g. rules_go, gazelle, etc.)`)
	for _, repoName := range sorted {
		if repoName == "com_github_pingcap_tidb_pkg_parser" {
			continue
		}
		path := repoNameToModPath[repoName]
		mod := listed[path]
		replaced := &mod
		if mod.Replace != nil {
			replaced = mod.Replace
		}
		fmt.Printf(`    go_repository(
        name = "%s",
`, repoName)
		fmt.Printf(`        build_file_proto_mode = "%s",
`, buildFileProtoModeForRepo(repoName))
		dumpBuildNamingConventionArgsForRepo(repoName)
		expectedVPCPrivateURL := formatVPCPrivateURL(replaced.Path, replaced.Version)
		expectedCDNURL := formatCDNURL(replaced.Path, replaced.Version)
		expectedPublicURL := formatPublicURL(replaced.Path, replaced.Version)
		expectedVPCPublicURL := formatVPCPublicURL(replaced.Path, replaced.Version)
		fmt.Printf("        importpath = \"%s\",\n", mod.Path)
		if err := dumpPatchArgsForRepo(repoName); err != nil {
			return err
		}
		oldMirror, ok := existingMirrors[repoName]
		if ok &&
			slices.Contains(oldMirror.URL, expectedVPCPrivateURL) &&
			slices.Contains(oldMirror.URL, expectedCDNURL) &&
			slices.Contains(oldMirror.URL, expectedPublicURL) &&
			slices.Contains(oldMirror.URL, expectedVPCPublicURL) {
			// The URL matches, so just reuse the old mirror.
			fmt.Printf(`        sha256 = "%s",
        strip_prefix = "%s@%s",
        urls = [
			"%s",
			"%s",
			"%s",
			"%s",
        ],
`, oldMirror.Sha256, replaced.Path, replaced.Version, expectedPublicURL, expectedVPCPrivateURL, expectedCDNURL, expectedPublicURL)
		} else if isMirror {
			// We'll have to mirror our copy of the zip ourselves.
			d := downloaded[replaced.Path]
			sha, err := getSha256OfFile(d.Zip)
			if err != nil {
				return fmt.Errorf("could not get zip for %v: %w", *replaced, err)
			}
			if replaced.Path == "github.com/form3tech-oss/jwt-go" && replaced.Version == "v3.2.5+incompatible" {
				replaced.Version = "v3.2.6-0.20210809144907-32ab6a8243d7+incompatible"
			}
			if sha == "30cf0ef9aa63aea696e40df8912d41fbce69dd02986a5b99af7c5b75f277690c" {
				sha = "ebe8386761761d53fac2de5f8f575ddf66c114ec9835947c761131662f1d38f3"
			}
			fmt.Printf(`        sha256 = "%s",
        strip_prefix = "%s@%s",
        urls = [
            "%s",
            "%s",
            "%s",
            "%s",
        ],
`, sha, replaced.Path, replaced.Version, expectedVPCPublicURL, expectedVPCPrivateURL, expectedCDNURL, expectedPublicURL)
			g.Go(func() error {
				return uploadFile(ctx, client, d.Zip, formatSubURL(replaced.Path, replaced.Version))
			})
		} else {
			// We don't have a mirror and can't upload one, so just
			// have Gazelle pull the repo for us.
			d := downloaded[replaced.Path]
			sum, version := d.Sum, d.Version
			if mod.Replace != nil {
				fmt.Printf("        replace = \"%s\",\n", replaced.Path)
			}
			artifact, ok := existingMirrors[replaced.Path]
			if ok {
				sum, version = artifact.Sha256, artifact.Version
			}
			// Note: `build/teamcity-check-genfiles.sh` checks for
			// the presence of the "TODO: mirror this repo" comment.
			// Don't update this comment without also updating the
			// script.
			fmt.Printf(`        sum = "%s",
        version = "%s",
`, sum, version)
		}
		fmt.Println("    )")
	}

	// Wait for uploads to complete.
	if err := g.Wait(); err != nil {
		return err
	}
	if client == nil {
		return nil
	}
	return client.Close()
}

func mirror() error {
	tmpdir, err := createTmpDir()
	if err != nil {
		return err
	}
	defer func() {
		err := os.RemoveAll(tmpdir)
		if err != nil {
			panic(err)
		}
	}()
	listed, err := listAllModules(tmpdir)
	if err != nil {
		return err
	}
	downloaded, err := downloadZips(tmpdir, listed)
	if err != nil {
		return err
	}
	existingMirrors, err := getExistingMirrors()
	if err != nil {
		return err
	}
	return dumpNewDepsBzl(listed, downloaded, existingMirrors)
}

func main() {
	flag.Parse()
	if err := mirror(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			panic("subprocess exited with stderr:\n" + string(exitErr.Stderr))
		}
		panic(err)
	}
}
