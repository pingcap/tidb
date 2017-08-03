#!groovy

node {
    def TIDB_TEST_BRANCH = "master"
    def TIDB_BRANCH = "master"
    def TIKV_BRANCH = "master"

    fileLoader.withGit('git@github.com:pingcap/SRE.git', 'master', 'github-iamxy-ssh', '') {
        fileLoader.load('jenkins/ci/pingcap_pd_branch.groovy').call(TIDB_TEST_BRANCH, TIDB_BRANCH, TIKV_BRANCH)
    }
}
