#!groovy

node {
    def TIDB_TEST_BRANCH = "rc3"
    def TIKV_BRANCH = "master"
    def PD_BRANCH = "master"

    fileLoader.withGit('git@github.com:pingcap/SRE.git', 'master', 'github-iamxy-ssh', '') {
        fileLoader.load('jenkins/ci/pingcap_tidb_branch.groovy').call(TIDB_TEST_BRANCH, TIKV_BRANCH, PD_BRANCH)
    }
}
