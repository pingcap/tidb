#!groovy

node {
    def TIDB_TEST_BRANCH = "master"
    def TIKV_BRANCH = "rc2.2"
    def PD_BRANCH = "rc2.2"
    def UCLOUD_OSS_URL = "http://pingcap-dev.hk.ufileos.com"

    env.GOROOT = "/usr/local/go"
    env.GOPATH = "/go"
    env.PATH = "${env.GOROOT}/bin:/home/jenkins/bin:/bin:${env.PATH}"

    catchError {
        stage('Prepare') {
            // tidb
            node('centos7_build') {
                def ws = pwd()
                dir("go/src/github.com/pingcap/tidb") {
                    // checkout
                    checkout scm
                    // build
                    sh "GOPATH=${ws}/go:$GOPATH make"
                }
                stash includes: "go/src/github.com/pingcap/tidb/**", name: "tidb"
            }

            // tidb-test
            dir("go/src/github.com/pingcap/tidb-test") {
                // checkout
                git changelog: false, credentialsId: 'github-iamxy-ssh', poll: false, url: 'git@github.com:pingcap/tidb-test.git', branch: "${TIDB_TEST_BRANCH}"
            }
            stash includes: "go/src/github.com/pingcap/tidb-test/**", name: "tidb-test"

            // mybatis
            dir("mybatis3") {
                git changelog: false, credentialsId: 'github-iamxy-ssh', poll: false, branch: 'travis-tidb', url: 'git@github.com:pingcap/mybatis-3.git'
            }
            stash includes: "mybatis3/**", name: "mybatis"

            // tikv
            def tikv_sha1 = sh(returnStdout: true, script: "curl ${UCLOUD_OSS_URL}/refs/pingcap/tikv/${TIKV_BRANCH}/centos7/sha1").trim()
            sh "curl ${UCLOUD_OSS_URL}/builds/pingcap/tikv/${tikv_sha1}/centos7/tikv-server.tar.gz | tar xz"

            // pd
            def pd_sha1 = sh(returnStdout: true, script: "curl ${UCLOUD_OSS_URL}/refs/pingcap/pd/${PD_BRANCH}/centos7/sha1").trim()
            sh "curl ${UCLOUD_OSS_URL}/builds/pingcap/pd/${pd_sha1}/centos7/pd-server.tar.gz | tar xz"

            stash includes: "bin/**", name: "binaries"
        }

        stage('Test') {
            def tests = [:]

            tests["Unit Test"] = {
                node("test") {
                    def ws = pwd()
                    deleteDir()
                    unstash 'tidb'

                    dir("go/src/github.com/pingcap/tidb") {
                        sh "GOPATH=${ws}/go:$GOPATH make test"
                    }
                }
            }

            tests["TiDB Test"] = {
                node("test") {
                    def ws = pwd()
                    deleteDir()
                    unstash 'tidb'
                    unstash 'tidb-test'

                    dir("go/src/github.com/pingcap/tidb-test") {
                        sh """
                        ln -s tidb/_vendor/src ../vendor
                        GOPATH=${ws}/go:$GOPATH make tidbtest
                        """
                    }
                }
            }

            tests["MySQL Test"] = {
                node("test") {
                    def ws = pwd()
                    deleteDir()
                    unstash 'tidb'
                    unstash 'tidb-test'

                    dir("go/src/github.com/pingcap/tidb-test") {
                        sh """
                        ln -s tidb/_vendor/src ../vendor
                        GOPATH=${ws}/go:$GOPATH make mysqltest
                        """
                    }
                }
            }

            tests["GORM Test"] = {
                node("test") {
                    def ws = pwd()
                    deleteDir()
                    unstash 'tidb'
                    unstash 'tidb-test'

                    dir("go/src/github.com/pingcap/tidb-test") {
                        sh """
                        ln -s tidb/_vendor/src ../vendor
                        GOPATH=${ws}/go:$GOPATH make gormtest
                        """
                    }
                }
            }

            tests["Go SQL Test"] = {
                node("test") {
                    def ws = pwd()
                    deleteDir()
                    unstash 'tidb'
                    unstash 'tidb-test'

                    dir("go/src/github.com/pingcap/tidb-test") {
                        sh """
                        ln -s tidb/_vendor/src ../vendor
                        GOPATH=${ws}/go:$GOPATH make gosqltest
                        """
                    }
                }
            }

            tests["Mybaits Test"] = {
                node("test") {
                    def ws = pwd()
                    deleteDir()
                    unstash 'tidb'
                    unstash 'mybatis'

                    dir("go/src/github.com/pingcap/tidb") {
                        sh """
                        killall -9 tidb-server || true
                        bin/tidb-server --store memory -join-concurrency=1 > ${ws}/tidb_mybatis3_test.log 2>&1 &
                        """
                    }

                    try {
                        sh "mvn -B -f mybatis3/pom.xml clean test"
                    } catch (err) {
                        throw err
                    } finally {
                        sh "killall -9 tidb-server || true"
                    }
                }
            }

            def run_sqllogic_test = { ws, sqllogictest, parallelism ->
                    deleteDir()
                    unstash 'tidb'
                    unstash 'tidb-test'

                    dir("go/src/github.com/pingcap/tidb-test") {
                        sh """
                        ln -s tidb/_vendor/src ../vendor
                        SQLLOGIC_TEST_PATH=${sqllogictest} \
                        TIDB_PARALLELISM=${parallelism} \
                        TIDB_SERVER_PATH=${ws}/go/src/github.com/pingcap/tidb/bin/tidb-server \
                        GOPATH=${ws}/go:$GOPATH \
                        make sqllogictest
                        """
                    }
            }

            tests["SQLLogic Random Aggregates Test"] = {
                node("test") {
                    def ws = pwd()
                    def sqllogictest = '/home/pingcap/sqllogictest/test/random/aggregates'
                    def parallelism = 12
                    run_sqllogic_test(ws, sqllogictest, parallelism)
                }
            }

            tests["SQLLogic Random Expr Test"] = {
                node("test") {
                    def ws = pwd()
                    def sqllogictest = '/home/pingcap/sqllogictest/test/random/expr'
                    def parallelism = 4
                    run_sqllogic_test(ws, sqllogictest, parallelism)
                }
            }

            tests["SQLLogic Random Groupby Test"] = {
                node("test") {
                    def ws = pwd()
                    def sqllogictest = '/home/pingcap/sqllogictest/test/random/groupby'
                    def parallelism = 4
                    run_sqllogic_test(ws, sqllogictest, parallelism)
                }
            }

            tests["SQLLogic Random Select Test"] = {
                node("test") {
                    def ws = pwd()
                    def sqllogictest = '/home/pingcap/sqllogictest/test/random/select'
                    def parallelism = 10
                    run_sqllogic_test(ws, sqllogictest, parallelism)
                }
            }

            tests["SQLLogic Select Test"] = {
                node("test") {
                    def ws = pwd()
                    def sqllogictest = '/home/pingcap/sqllogictest/test/select'
                    def parallelism = 4
                    run_sqllogic_test(ws, sqllogictest, parallelism)
                }
            }

            tests["SQLLogic Index Between Test"] = {
                node("test") {
                    def ws = pwd()
                    def sqllogictest = '/home/pingcap/sqllogictest/test/index/between'
                    def parallelism = 4
                    run_sqllogic_test(ws, sqllogictest, parallelism)
                }
            }

            tests["SQLLogic Index commute 10 Test"] = {
                node("test") {
                    def ws = pwd()
                    def sqllogictest = '/home/pingcap/sqllogictest/test/index/commute/10'
                    def parallelism = 4
                    run_sqllogic_test(ws, sqllogictest, parallelism)
                }
            }

            tests["SQLLogic Index commute 100 Test"] = {
                node("test") {
                    def ws = pwd()
                    def sqllogictest = '/home/pingcap/sqllogictest/test/index/commute/100'
                    def parallelism = 4
                    run_sqllogic_test(ws, sqllogictest, parallelism)
                }
            }

            tests["SQLLogic Index commute 1000 Test"] = {
                node("test") {
                    def ws = pwd()
                    def sqllogictest = '/home/pingcap/sqllogictest/test/index/commute/1000'
                    def parallelism = 4
                    run_sqllogic_test(ws, sqllogictest, parallelism)
                }
            }

            tests["SQLLogic Index delete 1 Test"] = {
                node("test") {
                    def ws = pwd()
                    def sqllogictest = '/home/pingcap/sqllogictest/test/index/delete/1'
                    def parallelism = 4
                    run_sqllogic_test(ws, sqllogictest, parallelism)
                }
            }

            tests["SQLLogic Index delete 10 Test"] = {
                node("test") {
                    def ws = pwd()
                    def sqllogictest = '/home/pingcap/sqllogictest/test/index/delete/10'
                    def parallelism = 4
                    run_sqllogic_test(ws, sqllogictest, parallelism)
                }
            }

            tests["SQLLogic Index delete 100 Test"] = {
                node("test") {
                    def ws = pwd()
                    def sqllogictest = '/home/pingcap/sqllogictest/test/index/delete/100'
                    def parallelism = 4
                    run_sqllogic_test(ws, sqllogictest, parallelism)
                }
            }

            tests["SQLLogic Index delete 1000 Test"] = {
                node("test") {
                    def ws = pwd()
                    def sqllogictest = '/home/pingcap/sqllogictest/test/index/delete/1000'
                    def parallelism = 4
                    run_sqllogic_test(ws, sqllogictest, parallelism)
                }
            }

            tests["SQLLogic Index delete 10000 Test"] = {
                node("test") {
                    def ws = pwd()
                    def sqllogictest = '/home/pingcap/sqllogictest/test/index/delete/10000'
                    def parallelism = 4
                    run_sqllogic_test(ws, sqllogictest, parallelism)
                }
            }

            tests["SQLLogic Index in 10 Test"] = {
                node("test") {
                    def ws = pwd()
                    def sqllogictest = '/home/pingcap/sqllogictest/test/index/in/10'
                    def parallelism = 4
                    run_sqllogic_test(ws, sqllogictest, parallelism)
                }
            }

            tests["SQLLogic Index in 100 Test"] = {
                node("test") {
                    def ws = pwd()
                    def sqllogictest = '/home/pingcap/sqllogictest/test/index/in/100'
                    def parallelism = 4
                    run_sqllogic_test(ws, sqllogictest, parallelism)
                }
            }

            tests["SQLLogic Index in 1000 Test"] = {
                node("test") {
                    def ws = pwd()
                    def sqllogictest = '/home/pingcap/sqllogictest/test/index/in/1000'
                    def parallelism = 8
                    run_sqllogic_test(ws, sqllogictest, parallelism)
                }
            }

            tests["SQLLogic Index orderby 10 Test"] = {
                node("test") {
                    def ws = pwd()
                    def sqllogictest = '/home/pingcap/sqllogictest/test/index/orderby/10'
                    def parallelism = 4
                    run_sqllogic_test(ws, sqllogictest, parallelism)
                }
            }

            tests["SQLLogic Index orderby 100 Test"] = {
                node("test") {
                    def ws = pwd()
                    def sqllogictest = '/home/pingcap/sqllogictest/test/index/orderby/100'
                    def parallelism = 4
                    run_sqllogic_test(ws, sqllogictest, parallelism)
                }
            }

            tests["SQLLogic Index orderby 1000 Test"] = {
                node("test") {
                    def ws = pwd()
                    def sqllogictest = '/home/pingcap/sqllogictest/test/index/orderby/1000'
                    def parallelism = 4
                    run_sqllogic_test(ws, sqllogictest, parallelism)
                }
            }

            tests["SQLLogic Index orderby_nosort 10 Test"] = {
                node("test") {
                    def ws = pwd()
                    def sqllogictest = '/home/pingcap/sqllogictest/test/index/orderby_nosort/10'
                    def parallelism = 8
                    run_sqllogic_test(ws, sqllogictest, parallelism)
                }
            }

            tests["SQLLogic Index orderby_nosort 100 Test"] = {
                node("test") {
                    def ws = pwd()
                    def sqllogictest = '/home/pingcap/sqllogictest/test/index/orderby_nosort/100'
                    def parallelism = 4
                    run_sqllogic_test(ws, sqllogictest, parallelism)
                }
            }

            tests["SQLLogic Index orderby_nosort 1000 Test"] = {
                node("test") {
                    def ws = pwd()
                    def sqllogictest = '/home/pingcap/sqllogictest/test/index/orderby_nosort/1000'
                    def parallelism = 4
                    run_sqllogic_test(ws, sqllogictest, parallelism)
                }
            }

            tests["SQLLogic Index random 10 Test"] = {
                node("test") {
                    def ws = pwd()
                    def sqllogictest = '/home/pingcap/sqllogictest/test/index/random/10'
                    def parallelism = 4
                    run_sqllogic_test(ws, sqllogictest, parallelism)
                }
            }

            tests["SQLLogic Index random 100 Test"] = {
                node("test") {
                    def ws = pwd()
                    def sqllogictest = '/home/pingcap/sqllogictest/test/index/random/100'
                    def parallelism = 4
                    run_sqllogic_test(ws, sqllogictest, parallelism)
                }
            }

            tests["SQLLogic Index random 1000 Test"] = {
                node("test") {
                    def ws = pwd()
                    def sqllogictest = '/home/pingcap/sqllogictest/test/index/random/1000'
                    def parallelism = 8
                    run_sqllogic_test(ws, sqllogictest, parallelism)
                }
            }

            def run_integration_ddl_test = { ddltest ->
                def ws = pwd()
                deleteDir()
                unstash 'tidb'
                unstash 'tidb-test'
                unstash 'binaries'

                try {
                    sh """
                    killall -9 ddltest_tidb-server || true
                    killall -9 tikv-server || true
                    killall -9 pd-server || true
                    bin/pd-server --name=pd --data-dir=pd &>pd_ddl_test.log &
                    sleep 10
                    bin/tikv-server --pd-endpoints=127.0.0.1:2379 --data-dir=tikv --addr=0.0.0.0:20160 --advertise-addr=127.0.0.1:20160 &>tikv_ddl_test.log &
                    sleep 10
                    """

                    timeout(10) {
                        dir("go/src/github.com/pingcap/tidb-test") {
                            sh """
                            ln -s tidb/_vendor/src ../vendor
                            cp ${ws}/go/src/github.com/pingcap/tidb/bin/tidb-server ddl_test/ddltest_tidb-server
                            cd ddl_test && GOPATH=${ws}/go:$GOPATH ./run-tests.sh -check.f='${ddltest}'
                            """
                        }
                    }
                } catch (err) {
                    throw err
                } finally {
                    sh "killall -9 ddltest_tidb-server || true"
                    sh "killall -9 tikv-server || true"
                    sh "killall -9 pd-server || true"
                }
            }

            tests["Integration DDL Insert Test"] = {
                node("test") {
                    run_integration_ddl_test('TestDDLSuite.TestSimple.*Insert')
                }
            }

            tests["Integration DDL Update Test"] = {
                node("test") {
                    run_integration_ddl_test('TestDDLSuite.TestSimple.*Update')
                }
            }

            tests["Integration DDL Delete Test"] = {
                node("test") {
                    run_integration_ddl_test('TestDDLSuite.TestSimple.*Delete')
                }
            }

            tests["Integration DDL Other Test"] = {
                node("test") {
                    run_integration_ddl_test('TestDDLSuite.TestSimp(le\$|leMixed|leInc)')
                }
            }

            tests["Integration DDL Column and Index Test"] = {
                node("test") {
                    run_integration_ddl_test('TestDDLSuite.Test(Column|Index)')
                }
            }

            tests["Integration Connection Test"] = {
                node("test") {
                    def ws = pwd()
                    deleteDir()
                    unstash 'tidb'
                    unstash 'tidb-test'
                    unstash 'binaries'

                    try {
                        sh """
                        killall -9 tikv-server || true
                        killall -9 pd-server || true
                        bin/pd-server --name=pd --data-dir=pd &>pd_conntest.log &
                        sleep 10
                        bin/tikv-server --pd-endpoints=127.0.0.1:2379 --data-dir=tikv --addr=0.0.0.0:20160 --advertise-addr=127.0.0.1:20160 &>tikv_conntest.log &
                        sleep 10
                        """

                        dir("go/src/github.com/pingcap/tidb") {
                            sh """
                            GOPATH=`pwd`/_vendor:${ws}/go:$GOPATH CGO_ENABLED=1 go test --args with-tikv store/tikv/*.go
                            """
                        }
                    } catch (err) {
                        throw err
                    } finally {
                        sh "killall -9 tikv-server || true"
                        sh "killall -9 pd-server || true"
                    }
                }
            }

            def run_integration_other_test = { mytest ->
                def ws = pwd()
                deleteDir()
                unstash 'tidb'
                unstash 'tidb-test'
                unstash 'binaries'

                try {
                    sh """
                    killall -9 tikv-server || true
                    killall -9 pd-server || true
                    bin/pd-server --name=pd --data-dir=pd &>pd_${mytest}.log &
                    sleep 10
                    bin/tikv-server --pd-endpoints=127.0.0.1:2379 --data-dir=tikv --addr=0.0.0.0:20160 --advertise-addr=127.0.0.1:20160 &>tikv_${mytest}.log &
                    sleep 10
                    """

                    dir("go/src/github.com/pingcap/tidb-test") {
                        sh """
                        ln -s tidb/_vendor/src ../vendor
                        GOPATH=${ws}/go:$GOPATH TIKV_PATH='127.0.0.1:2379' TIDB_TEST_STORE_NAME=tikv make ${mytest}
                        """
                    }
                } catch (err) {
                    throw err
                } finally {
                    sh "killall -9 tikv-server || true"
                    sh "killall -9 pd-server || true"
                }
            }

            tests["Integration TiDB Test"] = {
                node('test') {
                    run_integration_other_test('tidbtest')
                }
            }

            tests["Integration MySQL Test"] = {
                node("test") {
                    run_integration_other_test('mysqltest')
                }
            }

            tests["Integration GORM Test"] = {
                node("test") {
                    run_integration_other_test('gormtest')
                }
            }

            tests["Integration Go SQL Test"] = {
                node("test") {
                    run_integration_other_test('gosqltest')
                }
            }

            parallel tests
        }

        currentBuild.result = "SUCCESS"
    }

    stage('Summary') {
        def getChangeLogText = {
            def changeLogText = ""
            for (int i = 0; i < currentBuild.changeSets.size(); i++) {
                for (int j = 0; j < currentBuild.changeSets[i].items.length; j++) {
                    def commitId = "${currentBuild.changeSets[i].items[j].commitId}"
                    def commitMsg = "${currentBuild.changeSets[i].items[j].msg}"
                    changeLogText += "\n" + commitId.take(7) + " - " + commitMsg
                }
            }
            return changeLogText
        }
        def changelog = getChangeLogText()
        def duration = (System.currentTimeMillis() - currentBuild.startTimeInMillis) / 1000

        def slackmsg = "${env.JOB_NAME}-${env.BUILD_NUMBER}: ${currentBuild.result}, Duration: ${duration}, Changelogs: ${changelog}"

        if (currentBuild.result != "SUCCESS") {
            slackSend channel: '#tidb', color: 'danger', teamDomain: 'pingcap', tokenCredentialId: 'slack-pingcap-token', message: "${slackmsg}"
        }
    }
}
