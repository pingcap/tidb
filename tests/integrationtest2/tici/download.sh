#! /usr/bin/env bash

set -eo pipefail

function normalize_oci_tag() {
    local tag="$1"
    tag="${tag#@}"
    tag="${tag//\//-}"
    echo "$tag"
}

function fetch_file_from_oci_artifact() {
    local oci_url="$1"
    local to_match_file="$2"
    local repo="$(echo ${oci_url} | cut -d ':' -f 1)"

    # get the file blob digest.
    oras manifest fetch ${oci_url} | yq --prettyPrint -oy ".layers | filter(.annotations[\"org.opencontainers.image.title\"] | test \"$to_match_file\") | .[0]" >blob.yaml

    # download file
    file="$(yq .annotations[\"org.opencontainers.image.title\"] blob.yaml)"
    blob="$repo@$(yq .digest blob.yaml)"
    echo "🔗 blob fetching url: ${blob}" >&2
    oras blob fetch --output $file $blob
    rm blob.yaml
    echo "$file"
}

function download() {
    local url=$1
    local to_match_file=$2
    local file_path=$3
    if [[ -f "${file_path}" ]]; then
        echo "file $(basename "${file_path}") already exists, skip download"
        return
    fi
    echo "🚀 Downloading file with name matched regex: '${to_match_file}' from ${url}"
    echo "📦 == artifact information ======="
    oras manifest fetch-config "$url"
    echo "================================🔚"
    local tarball_file=$(fetch_file_from_oci_artifact $url "${to_match_file}")
    mv -v "$tarball_file" "$file_path"
    echo "✅ Downloaded, saved in ${file_path}"
}

function download_and_extract_with_path() {
    local url=$1
    local to_match_file=$2
    local file_path=$3
    local path_in_archive=$4
    if [[ -e "${path_in_archive}" ]]; then
        echo "file ${path_in_archive} already exists, skip download"
        return
    fi

    download "$url" "$to_match_file" "$file_path"
    echo "📂 extract ${path_in_archive} from ${file_path} ..."
    tar -xzvf "${file_path}" "${path_in_archive}"
    rm "${file_path}"
    echo "✅ extracted ${path_in_archive} from ${file_path} ."
}

function compute_tag_platform_suffix() {
    local os="$(uname | tr '[:upper:]' '[:lower:]')"
    local arch="$(uname -m)"
    case "$arch" in
        x86_64)
            arch="amd64"
            ;;
        aarch64 | arm64)
            arch="arm64"
            ;;
        *)
            echo "Unsupported architecture: $arch"
            exit 1
            ;;
    esac
    echo "${os}_${arch}"
}

function main() {
    check_tools
    parse_cli_args "$@"

    if [[ -n "$TIDB" ]]; then
        echo "🚀 start download TiDB server"
        download_and_extract_with_path "$tidb_oci_url" '^tidb-v.+.tar.gz$' tidb.tar.gz tidb-server
        chmod +x tidb-server
        echo "🎉 download TiDB server success"
    fi
    if [[ -n "$TIKV" ]]; then
        echo "🚀 start download TiKV server"
        download_and_extract_with_path "$tikv_oci_url" '^tikv-v.+.tar.gz$' tikv.tar.gz tikv-server
        chmod +x tikv-server
        echo "🎉 download TiKV server success"
    fi
    if [[ -n "$TIKV_WORKER" ]]; then
        echo "🚀 start download TiKV worker"
        download_and_extract_with_path "$tikv_worker_oci_url" '^tikv-worker-v.+.tar.gz$' tikv.tar.gz tikv-worker
        chmod +x tikv-worker
        echo "🎉 download TiKV worker success"
    fi
    if [[ -n "$PD" ]]; then
        echo "🚀 start download PD server"
        download_and_extract_with_path "$pd_oci_url" '^pd-v.+.tar.gz$' pd.tar.gz pd-server
        chmod +x pd-server
        echo "🎉 download PD server success"
    fi
    if [[ -n "$PD_CTL" ]]; then
        echo "🚀 start download pd-ctl"
        download_and_extract_with_path "$pd_ctl_oci_url" '^pd-ctl-v.+.tar.gz$' pd.tar.gz pd-ctl
        chmod +x pd-ctl
        echo "🎉 download pd-ctl success"
    fi
    if [[ -n "$TIFLASH" ]]; then
        echo "🚀 start download TiFlash"
        download_and_extract_with_path "$tiflash_oci_url" '^tiflash-v.+.tar.gz$' tiflash.tar.gz tiflash
        chmod +x tiflash/tiflash
        ls -alh tiflash
        echo "🎉 download TiFlash success"
    fi
    if [[ -n "$TICDC" ]]; then
        echo "🚀 start download TiCDC"
        download_and_extract_with_path "$ticdc_oci_url" '^cdc-v.+.tar.gz$' cdc.tar.gz cdc
        chmod +x cdc
        echo "🎉 download TiCDC success"
    fi
    if [[ -n "$TICDC_NEW" ]]; then
        echo "🚀 start download TiCDC(new)"
        download_and_extract_with_path "$ticdc_new_oci_url" '^cdc-v.+.tar.gz$' cdc.tar.gz cdc
        chmod +x cdc
        echo "🎉 download TiCDC(new) success"
    fi
    if [[ -n "$TICI" ]]; then
        echo "🚀 start download TiCI"
        download_and_extract_with_path "$tici_oci_url" '^tici-v.+.tar.gz$' tici.tar.gz tici-server
        chmod +x tici-server
        echo "🎉 download TiCI success"
    fi
    if [[ -n "$MINIO" ]]; then
        echo "🚀 start download MinIO server and client"
        fetch_file_from_oci_artifact "$minio_oci_url" minio
        fetch_file_from_oci_artifact "$minio_oci_url" mc
        chmod +x minio mc
        echo "🎉 download MinIO server and client success"
    fi
    if [[ -n "$ETCDCTL" ]]; then
        echo "🚀 start download etcdctl"
        download "$etcd_oci_url" '^etcd-v.+.tar.gz$' etcd.tar.gz
        tar -zxf etcd.tar.gz
        mv etcd-*/etcdctl ./
        chmod +x etcdctl
        rm -rf etcd.tar.gz etcd-*
        echo "🎉 download etcdctl success"
    fi
    if [[ -n "$YCSB" ]]; then
        echo "🚀 start download go-ycsb"
        download_and_extract_with_path "$ycsb_oci_url" '^go-ycsb-.+.tar.gz$' go-ycsb.tar.gz go-ycsb
        chmod +x go-ycsb
        echo "🎉 download go-ycsb success"
    fi
    if [[ -n "$SCHEMA_REGISTRY" ]]; then
        echo "🚀 start download schema-registry"
        download "$schema_registry_oci_url" '^schema-registry.*.tar.gz$' schema-registry.tar.gz
        tar -zxf schema-registry.tar.gz --strip-components=1
        rm schema-registry.tar.gz
        chmod +x bin/*
        echo "🎉 download schema-registry success"
    fi
    if [[ -n "$SYNC_DIFF_INSPECTOR" ]]; then
        echo "🚀 start download sync-diff-inspector"
        download_and_extract_with_path "$sync_diff_inspector_oci_url" '^sync-diff-inspector-v.+.tar.gz$' sync-diff-inspector.tar.gz sync_diff_inspector
        chmod +x sync_diff_inspector
        echo "🎉 download sync-diff-inspector success"
    fi
}

function parse_cli_args() {
    for i in "$@"; do
    case $i in
        -tidb=*|--tidb=*)
        TIDB="${i#*=}"
        shift # past argument=value
        ;;
        -pd=*|--pd=*)
        PD="${i#*=}"
        shift # past argument=value
        ;;
        -pd-ctl=*|--pd-ctl=*)
        PD_CTL="${i#*=}"
        shift # past argument=value
        ;;
        -tikv=*|--tikv=*)
        TIKV="${i#*=}"
        shift # past argument=value
        ;;
        -tikv-worker=*|--tikv-worker=*)
        TIKV_WORKER="${i#*=}"
        shift # past argument=value
        ;;
        -tiflash=*|--tiflash=*)
        TIFLASH="${i#*=}"
        shift # past argument=value
        ;;
        -ticdc=*|--ticdc=*)
        TICDC="${i#*=}"
        shift # past argument=value
        ;;
        -ticdc-new=*|--ticdc-new=*)
        TICDC_NEW="${i#*=}"
        shift # past argument=value
        ;;
        -tici=*|--tici=*)
        TICI="${i#*=}"
        shift # past argument=value
        ;;
        -minio=*|--minio=*)
        MINIO="${i#*=}"
        shift # past argument=value
        ;;
        -etcdctl=*|--etcdctl=*)
        ETCDCTL="${i#*=}"
        shift # past argument=value
        ;;
        -ycsb=*|--ycsb=*)
        YCSB="${i#*=}"
        shift # past argument=value
        ;;
        -schema-registry=*|--schema-registry=*)
        SCHEMA_REGISTRY="${i#*=}"
        shift # past argument=value
        ;;
        -sync-diff-inspector=*|--sync-diff-inspector=*)
        SYNC_DIFF_INSPECTOR="${i#*=}"
        shift # past argument=value
        ;;
        --default)
        DEFAULT=YES
        shift # past argument with no value
        ;;
        -*|--*)
        echo "Unknown option $i"
        exit 1
        ;;
        *)
        ;;
    esac
    done

    [[ -n "${TIDB}" ]]          && echo "TIDB        = ${TIDB}"
    [[ -n "${TIKV}" ]]          && echo "TIKV        = ${TIKV}"
    [[ -n "${TIKV_WORKER}" ]]   && echo "TIKV_WORKER = ${TIKV_WORKER}"
    [[ -n "${PD}" ]]            && echo "PD          = ${PD}"
    [[ -n "${PD_CTL}" ]]        && echo "PD_CTL      = ${PD_CTL}"
    [[ -n "${TIFLASH}" ]]       && echo "TIFLASH     = ${TIFLASH}"
    [[ -n "${TICDC}" ]]         && echo "TICDC       = ${TICDC}"
    [[ -n "${TICDC_NEW}" ]]     && echo "TICDC_NEW   = ${TICDC_NEW}"
    [[ -n "${TICI}" ]]          && echo "TICI        = ${TICI}"
    [[ -n "${MINIO}" ]]         && echo "MINIO       = ${MINIO}"
    [[ -n "${ETCDCTL}" ]]       && echo "ETCDCTL     = ${ETCDCTL}"
    [[ -n "${YCSB}" ]]          && echo "YCSB        = ${YCSB}"
    [[ -n "${SCHEMA_REGISTRY}" ]] && echo "SCHEMA_REGISTRY = ${SCHEMA_REGISTRY}"
    [[ -n "${SYNC_DIFF_INSPECTOR}" ]] && echo "SYNC_DIFF_INSPECTOR = ${SYNC_DIFF_INSPECTOR}"

    if [[ -n $1 ]]; then
        echo "Last line of file specified as non-opt/last argument:"
        tail -1 $1
    fi

    # tiflash use feature branch, need a normalization
    TIFLASH="$(normalize_oci_tag "${TIFLASH:-}")"

    # get the tag suffix by current runtime os and arch, it will be "[linux|darwin]_[amd64|arm64]" format.
    local tag_suffix=$(compute_tag_platform_suffix)
    registry_host="${OCI_ARTIFACT_HOST:-hub.pingcap.net}"
    registry_host_community="${OCI_ARTIFACT_HOST_COMMUNITY:-us-docker.pkg.dev/pingcap-testing-account/hub}"
    tidb_oci_url="${registry_host}/pingcap/tidb/package:${TIDB}_${tag_suffix}"
    tiflash_oci_url="${registry_host}/pingcap/tiflash/package:${TIFLASH}_${tag_suffix}"
    tikv_oci_url="${registry_host}/tikv/tikv/package:${TIKV}_${tag_suffix}"
    tikv_worker_oci_url="${registry_host}/tikv/tikv/package:${TIKV_WORKER}_${tag_suffix}"
    pd_oci_url="${registry_host}/tikv/pd/package:${PD}_${tag_suffix}"
    pd_ctl_oci_url="${registry_host}/tikv/pd/package:${PD_CTL}_${tag_suffix}"
    ticdc_oci_url="${registry_host}/pingcap/tiflow/package:${TICDC}_${tag_suffix}"
    ticdc_new_oci_url="${registry_host}/pingcap/ticdc/package:${TICDC_NEW}_${tag_suffix}"
    tici_oci_url="${registry_host}/pingcap/tici/package:${TICI}_${tag_suffix}"

    # third party or public test tools.
    minio_oci_url="${registry_host_community}/pingcap/third-party/minio:${MINIO}_${tag_suffix}"
    etcd_oci_url="${registry_host_community}/pingcap/third-party/etcd:${ETCDCTL}_${tag_suffix}"
    ycsb_oci_url="${registry_host_community}/pingcap/go-ycsb/package:${YCSB}_${tag_suffix}"
    schema_registry_oci_url="${registry_host_community}/pingcap/third-party/schema-registry:${SCHEMA_REGISTRY}_${tag_suffix}"
    sync_diff_inspector_oci_url="${registry_host_community}/pingcap/tiflow/package:${SYNC_DIFF_INSPECTOR}_${tag_suffix}"
}

function check_tools() {
    command -v oras >/dev/null || { echo "Error: 'oras' not found"; exit 1; }
    command -v yq >/dev/null || { echo "Error: 'yq' not found"; exit 1; }
    command -v tar >/dev/null || { echo "Error: 'tar' not found"; exit 1; }
}

main "$@"
