# Copyright 2020 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

[CmdletBinding()]
param (
    [ValidateSet('Community', 'Enterprise')]
    [string]$Edition = (property Edition Community),

    [string]$GO = (property GO 'go'),
    [string]$TargetOS = (property GOOS ''),
    [string]$Target = $IsWindows ? 'bin\tidb-server.exe' : 'bin\tidb-server',
    [string]$BuildFlags = '',

    [switch]$Race = $false,
    [switch]$Check = $false,

    [uint]$P = (property P 8),
    [string]$ExplainTests = ''
)

if (-not (Test-Path (Join-Path 'tools' 'bin'))) {
    New-Item -ItemType Directory (Join-Path 'tools' 'bin') | Out-Null
}

$gopath = Resolve-Path (& $GO env -json | ConvertFrom-Json).GOPATH
$packages = & $GO list ./... | Where-Object { $_ -notlike '*cmd*' }
$directories = $packages -replace 'github.com/pingcap/tidb/', ''
$sources = Get-ChildItem -Path $directories -Filter '*.go'

$testFlags = @('-X', "'github.com/pingcap/tidb/config.checkBeforeDropLDFlag=1'")

function Get-ToolPath ($name, $dir = (Resolve-Path 'tools\bin')) {
    $exe = $IsWindows ? "$name.exe" : $name
    Join-Path $dir -ChildPath $exe
}

$tools = @{
    Path        = Resolve-Path tools/check
    Mod         = Resolve-Path tools/check/go.mod

    FailPoint   = @{
        Src  = 'github.com/pingcap/failpoint/failpoint-ctl'
        Path = (Get-ToolPath 'failpoint-ctl')
    }
    Errcheck    = @{
        Src  = 'github.com/kisielk/errcheck'
        Path = (Get-ToolPath 'errcheck')
    }
    Revive      = @{
        Src  = 'github.com/mgechev/revive'
        Path = (Get-ToolPath 'revive')
    }
    Unconvert   = @{
        Src  = 'github.com/mdempsky/unconvert'
        Path = (Get-ToolPath 'unconvert')
    }
    StaticCheck = @{
        Src  = 'honnef.co/go/tools/cmd/staticcheck'
        Path = (Get-ToolPath 'staticcheck' (Join-Path $gopath bin))
    }
    Linter      = @{
        Path = (Get-ToolPath 'golangci-lint')
    }
}

task BuildFailPoint -Inputs go.mod -Outputs $tools.FailPoint.Path {
    exec { & $GO build -o $tools.FailPoint.Path $tools.FailPoint.Src }
}

function Enable-FailPoint {
    Get-ChildItem . -Recurse -Directory | Where-Object { $_ -cnotmatch '(\.git|tools|\.idea)' } |
    ForEach-Object { exec { & $tools.FailPoint.Path enable $_ } }
}

task EnableFailPoint BuildFailPoint, {
    Enable-FailPoint
}

function Disable-FailPoint {
    Get-ChildItem . -Recurse -Directory | Where-Object { $_ -cnotmatch '(\.git|tools|\.idea)' } |
    ForEach-Object { exec { & $tools.FailPoint.Path disable $_ } }
}

task DisableFailPoint BuildFailPoint, {
    Disable-FailPoint
}

task BuildErrcheck -Inputs $tools.Mod -Outputs $tools.Errcheck.Path {
    Set-Location $tools.Path
    exec { & $GO build -o $tools.Errcheck.Path $tools.Errcheck.Src }
}

task RunErrcheck BuildErrcheck, {
    $exclude = Join-Path $tools.Path errcheck_excludes.txt
    exec { & $tools.Errcheck.Path -exclude $exclude -ignoretests -blank $packages }
}

task BuildRevive -Inputs $tools.Mod -Outputs $tools.Revive.Path {
    Set-Location $tools.Path
    exec { & $GO build -o $tools.Revive.Path $tools.Revive.Src }
}

task RunRevive BuildRevive, {
    $config = Join-Path $tools.Path revive.toml
    exec { & $tools.Revive.Path -formatter friendly -config $config $packages }
}

task BuildUnconvert -Inputs $tools.Mod -Outputs $tools.Unconvert.Path {
    Set-Location $tools.Path
    exec { & $GO build -o $tools.Unconvert.Path $tools.Unconvert.Src }
}

task RunUnconvert BuildUnconvert, {
    exec { & $tools.Unconvert.Path ./... }
}

task BuildStaticCheck -Inputs go.mod -Outputs $tools.StaticCheck.Path {
    exec { & $GO get $tools.StaticCheck.Src }
}

task RunStaticCheck BuildStaticCheck, {
    exec { & $tools.StaticCheck.Path ./... }
}

task DownloadLinter -If (-not (Test-Path $tools.Linter.Path)) {
    $goEnv = exec { & $GO env -json } | ConvertFrom-Json
    $version = '1.30.0'
    $os = $goEnv.GOHOSTOS
    $arch = $goEnv.GOHOSTARCH
    $ext = ($os -eq 'windows') ? 'zip' : 'tar.gz'

    $dir = Join-Path ([System.IO.Path]::GetTempPath()) ([System.Guid]::NewGuid())
    New-Item -ItemType Directory -Path $dir | Out-Null

    $url = "https://github.com/golangci/golangci-lint/releases/download/v$version/golangci-lint-$version-$os-$arch.$ext"
    $archive = Join-Path $dir "download.$ext"
    Write-Output "downloading $url"
    Invoke-WebRequest $url -OutFile $archive

    $IsWindows ? (Expand-Archive $archive $dir) : (exec { tar -C $dir -xzf $archive })
    $bin = $IsWindows ? 'golangci-lint.exe' : 'golangci-lint'
    Copy-Item (Join-Path $dir "golangci-lint-$version-$os-$arch\$bin") $tools.Linter.Path
    Remove-Item -Force -Recurse $dir
}

task RunLinter DownloadLinter, {
    exec { & $tools.Linter.Path run -v --disable-all --deadline=3m --enable=misspell --enable=ineffassign $directories }
}

task GoModTidy {
    exec { & $GO mod tidy }
}

task CheckTestSuite {
    Get-ChildItem . -Directory | Where-Object { $_ -notmatch 'vendor' } | 
    ForEach-Object { Get-ChildItem $_ -Recurse -Filter '*_test.go' } |
    ForEach-Object { Select-String $_ -Pattern 'type (test.*Suite.*) struct' -CaseSensitive } |
    ForEach-Object {
        $dir = Split-Path $_.Path
        $suite = $_.Matches.Groups[1].Value
        $sources = Get-ChildItem $dir -Recurse -Filter '*_test.go'
        $enabled = $sources | ForEach-Object {
            Select-String $_ -Pattern "_ = (check\.)?(Suite|SerialSuites)\((&?$suite{|new\($suite\))" -CaseSensitive
        }
        if (-not $enabled) {
            $hasCase = $sources | ForEach-Object { Select-String $_ -Pattern "func \((.* )?\*?$suite\) Test" -CaseSensitive }
            if ($hasCase) {
                throw "$suite in $dir is not enabled"
            }
        }
    }
}

task RunGoVet {
    exec { go vet -all $packages }
}

task FormatCode {
    if ((Get-Content tidb-server\main.go -Raw) -match "\r\n$") {
        Write-Build Red "Gofmt is skiped due to it will reformat CRLF. Please check your git core.autocrlf setting."
    }
    else {
        exec { gofmt -s -l -w $directories }
    }
    Set-Location (Resolve-Path cmd\importcheck)
    exec { & $GO run . ../.. }
}

# Synopsis: Check code quality with some analyzers.
task Check FormatCode, RunErrCheck, RunUnconvert, RunRevive, GoModTidy, CheckTestSuite, RunLinter, RunGoVet, RunStaticCheck

# Synopsis: Build TiDB server.
task Build -Inputs $sources -Outputs $Target {
    $build = @('build', '-tags', 'codes', $BuildFlags)
    if ($Race) {
        $build += '-race'
    }

    $version = (git describe --tags --dirty --always)
    $gitHash = (git rev-parse HEAD)
    $gitBranch = (git rev-parse --abbrev-ref HEAD)
    $buildTime = (Get-Date -UFormat '+%Y-%m-%d %I:%M:%S')

    $flags = @(
        '-X', "'github.com/pingcap/parser/mysql.TiDBReleaseVersion=$version'",
        '-X', "'github.com/pingcap/tidb/util/versioninfo.TiDBGitHash=$gitHash'",
        '-X', "'github.com/pingcap/tidb/util/versioninfo.TiDBGitBranch=$gitBranch'",
        '-X', "'github.com/pingcap/tidb/util/versioninfo.TiDBBuildTS=$buildTime'"
        '-X', "'github.com/pingcap/tidb/util/versioninfo.TiDBEdition=$Edition'"
    )
    if ($Check) {
        $flags += $testFlags
    }
    
    $build += @(
        '-ldflags', "`"$flags`"",
        '-o', $Target, 'tidb-server/main.go'
    )

    $Task.Data = $env:GOOS
    $env:GOOS = $TargetOS
    exec { & $GO $build }
} -Done {
    $env:GOOS = $Task.Data
}

task BuildExplainTest -Inputs (Get-ChildItem cmd\explaintest\* -Include '*.go') -Outputs (Get-ToolPath 'explain_test' 'cmd\explaintest') {
    Set-Location cmd\explaintest
    $output = $IsWindows ? 'explain_test.exe' : 'explain_test'
    exec { & $GO build -o $output }
}

# Synopsis: Run explain tests.
task ExplainTest -If (-not ((Get-Content cmd\explaintest\r\explain.result -Raw) -match "\r\n$")) Build, BuildExplainTest, {
    function Find-Prot {
        while ($true) {
            $port = Get-Random -Minimum 4000 -Maximum 65535
            $listener = [System.Net.Sockets.TcpListener]$port;
            try {
                $listener.Start()
                return $port
            }
            catch {
                continue
            }
            finally {
                $listener.Stop()
            }
        }
    }

    $tidbPath = Resolve-Path $Target
    Set-Location cmd\explaintest

    $explaintest = if ($IsWindows) { '.\explain_test.exe' } else { Resolve-Path '.\explain_test' }
    exec { & $GO build -o $explaintest }

    $port, $status = (Find-Prot), (Find-Prot)
   
    $logPath = 'explain-test.out'
    $tidbArgs = @('-P', "$port", '-status', "$status", '-config', 'config.toml', '-store', 'mocktikv')
    $tidb = Start-Process -FilePath $tidbPath -ArgumentList $tidbArgs -RedirectStandardOutput $logPath -NoNewWindow -PassThru
    Write-Output "TiDB server(Handle: $($tidb.Handle)) started"
    $Task.Data = $tidb
    Start-Sleep 5

    if ($ExplainTests -eq '') {
        Write-Output 'run all explain test cases'
    }
    else {
        Write-Output "run explain test cases: $ExplainTests"
    }
    exec { & $explaintest -port "$port" -status "$status" --log-level=error $ExplainTests }
} -Done {
    if ($Task.Data) {
        $Task.Data.Kill()
    }
}

# Synopsis: Check dependency.
task CheckDep {
    $list = go list -json github.com/pingcap/tidb/store/tikv | ConvertFrom-Json
    if ($list.Imports | Where-Object { Select-String -Pattern '^github.com/pingcap/parser$' -InputObject $_ }) {
        throw 'incorrect import of github.com/pingcap/parser'
    }
}

# Synopsis: Run unit tests.
task GoTest BuildFailPoint, {
    Enable-FailPoint
    $Task.Data = @{
        logLevel = $env:log_level
        tz       = $env:TZ
    }
    $env:log_level = 'fatal'
    $env:TZ = 'Asia/Shanghai'
    exec { & $GO test -p $P -ldflags "`"$testFlags`"" -cover $packages '-check.p' true '-check.timeout' 4s }
} -Done {
    Disable-FailPoint
    $env:log_level = $Task.Data.logLevel
    $env:TZ = $Task.Data.tz
}

# Synopsis: Run tests with race detecter enabled.
task GoRaceTest BuildFailPoint, {
    Enable-FailPoint
    $Task.Data = @{
        logLevel = $env:log_level
        tz       = $env:TZ
    }
    $env:log_level = 'debug'
    $env:TZ = 'Asia/Shanghai'
    exec { & $GO test -p $P -timeout 20m -race $packages }
} -Done {
    Disable-FailPoint
    $env:log_level = $Task.Data.logLevel
    $env:TZ = $Task.Data.tz
}

# Synopsis: Run tests with leak checker enabled.
task GoLeakTest BuildFailPoint, {
    Enable-FailPoint
    $Task.Data = @{
        logLevel = $env:log_level
        tz       = $env:TZ
    }
    $env:log_level = 'debug'
    $env:TZ = 'Asia/Shanghai'
    exec { & $GO test -p $P -tags leak $packages }
} -Done {
    Disable-FailPoint
    $env:log_level = $Task.Data.logLevel
    $env:TZ = $Task.Data.tz
}

# Synopsis: Run some tests with real TiKV.
task TiKVIntegrationTest BuildFailPoint, {
    Enable-FailPoint
    { & $GO test -p $P github.com/pingcap/tidb/store/tikv -with-tikv=true }
} -Done {
    Disable-FailPoint
}

# Synopsis: Ensure generated code is up to date.
task GoGenerate {
    exec { & $GO generate ./... }
    if (exec { git status -s } | ForEach-Object { (-split $_)[1] } |
        ForEach-Object { Select-String $_ -Pattern '^# Code generated .* DO NOT EDIT\.$' -CaseSensitive }) {
        throw 'Your commit is changed after running go generate ./..., it should not happen.'
    }
}

# Synopsis: Run common tests.
task Test ExplainTest, CheckDep, GoTest, GoGenerate

# Synopsis: Check and Test.
task Dev Check, Test

# Synopsis: Build TiDB server.
task . Build
