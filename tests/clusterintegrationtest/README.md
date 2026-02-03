# Cluster Integration Test

Before running the tests, please install tiup and build tidb binary:
```shell
curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
source .bash_profile
tiup --version

# cd tidb
make
```

## Guide: Run tests

```shell
# cd clusterintegrationtest
./run_mysql_tester.sh  # mysql-tester test

# vector python testers
python3 -m pip install uv
uv init --python python3.9
uv venv
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
   GOBIN=$(realpath .)/gobin go install github.com/pingcap/mysql-tester/src@f2d90ea9522d30c9a8e8d70cc31c7f016ca2801f
   ./gobin/src -retry-connection-count 5 -record
   ```

## Guide: Develop python_testers

1. Prepare local environment

   ```shell
   # cd clusterintegrationtest
   python3 -m pip install uv
   uv init --python python3.9
   uv venv
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
   uv run python_testers/vector_recall.py
   ```

## Note:
If your contribution involves tidb and tiflash and will affect the test cases of this test, please submit your contribution to tiflash first and wait 2 hours after the merge before executing this test.


1. In tidb, we will download the binary of the tiflash master branch as a component for cluster testing. Please make sure that your tiflash code is submitted to the master branch.
2. In tiflash, we will also download the binary of the tidb master branch as a component for cluster testing. Please make sure that your tidb code is submitted to the master branch.

If you have other questions about this test, please contact @EricZequan.