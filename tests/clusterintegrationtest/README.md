# Cluster Integration Test

Before running the tests, please install tiup:
```shell
curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
source .bash_profile
tiup --version
```

## Guide: Run tests

```shell
# cd clusterintegrationtest
./run_mysql_tester.sh  # mysql-tester test

# vector python testers
python3 -m pip install uv
uv venv --python python3.9
source .venv/bin/activate
uv pip install -r requirements.txt
# prepare datasets
cd datasets
wget https://ann-benchmarks.com/fashion-mnist-784-euclidean.hdf5
wget https://ann-benchmarks.com/mnist-784-euclidean.hdf5
cd ..
./run_python_testers.sh

./run_upgrade_test.sh  # upgrade cluster test
```

## Guide: Update `r`

After changing `t` or changing optimizer plans, `r` need to be updated.

1. Start an empty cluster and expose TiDB as :4000

   ```shell
   # cd clusterintegrationtest
   ./cluster.sh
   ```

   Note: You may need to wait about 30s for TiFlash to be ready.

2. Run following commands

   ```shell
   # cd clusterintegrationtest
   GOBIN=$(realpath .)/gobin go install github.com/pingcap/mysql-tester/src@314107b26aa8fce86beb0dd48e75827fb269b365
   ./gobin/src -retry-connection-count 5 -record
   ```

## Guide: Develop python_testers

1. Prepare local environment

   ```shell
   # cd clusterintegrationtest
   python3 -m pip install uv
   uv venv --python python3.9
   source .venv/bin/activate
   uv pip install -r requirements.txt
   ```

2. Download datasets

   ```shell
   # cd clusterintegrationtest
   cd datasets
   wget https://ann-benchmarks.com/fashion-mnist-784-euclidean.hdf5
   wget https://ann-benchmarks.com/mnist-784-euclidean.hdf5
   cd ..
   ```

3. Start a CSE cluster and expose TiDB as :4000

   ```shell
   # cd clusterintegrationtest
   ./cluster.sh
   ```

   Note: You may need to wait about 30s for TiFlash to be ready.

4. Run, edit and debug tests

   ```shell
   # cd clusterintegrationtest
   source .venv/bin/activate
   python3 python_testers/vector_recall.py
   ```