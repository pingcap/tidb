#!groovy

node {
    def TIDB_TEST_BRANCH = "rc3"
    def TIKV_BRANCH = "rc3"
    def PD_BRANCH = "rc3"

    fileLoader.withGit('git@github.com:pingcap/SRE.git', 'master', 'github-iamxy-ssh', '') {
        fileLoader.load('jenkins/ci/pingcap_tidb_branch.groovy').call(TIDB_TEST_BRANCH, TIKV_BRANCH, PD_BRANCH)
    }
}
