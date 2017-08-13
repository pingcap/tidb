#!groovy

node ('master') {
    def TIDB_TEST_BRANCH = "master"
    def TIKV_BRANCH = "master"
    def PD_BRANCH = "master"


    def UCLOUD_OSS_URL = "http://pingcap-dev.hk.ufileos.com"
    def MYBATIS3_URL = "https://github.com/pingcap/mybatis-3/archive/travis-tidb.zip"
    env.GOROOT = "/usr/local/go"
    env.GOPATH = "/go"
    env.PATH = "${env.GOROOT}/bin:/bin:${env.PATH}"

    catchError {
        stage('Prepare') {
            node('k8s-ci-build') {
                def ws = pwd()

                // tidb
                dir("go/src/github.com/pingcap/tidb") {
                    container('centos7') {
                        // checkout
                        checkout scm

                        // build
                        sh "GOPATH=${ws}/go:$GOPATH WITH_RACE=1 make && mv bin/tidb-server bin/tidb-server-race"
                        sh "GOPATH=${ws}/go:$GOPATH make"
                    }
                }
                stash includes: "go/src/github.com/pingcap/tidb/**", name: "tidb"

                // tidb-test
                dir("go/src/github.com/pingcap/tidb-test") {
                    container('centos7') {
                        // checkout
                        git changelog: false, credentialsId: 'github-iamxy-ssh', poll: false, url: 'git@github.com:pingcap/tidb-test.git', branch: "${TIDB_TEST_BRANCH}"
                    }
                }
                stash includes: "go/src/github.com/pingcap/tidb-test/**", name: "tidb-test"

                // mybatis
                dir("mybatis3") {
                    container('centos7') {
                        //git changelog: false, credentialsId: 'github-iamxy-ssh', poll: false, branch: 'travis-tidb', url: 'git@github.com:pingcap/mybatis-3.git'
                        sh "curl -L ${MYBATIS3_URL} -o travis-tidb.zip && unzip travis-tidb.zip && rm -rf travis-tidb.zip"
                        sh "cp -R mybatis-3-travis-tidb/* . && rm -rf mybatis-3-travis-tidb"
                    }
                }
                stash includes: "mybatis3/**", name: "mybatis"

                // tikv
                def tikv_sha1 = sh(returnStdout: true, script: "curl ${UCLOUD_OSS_URL}/refs/pingcap/tikv/${TIKV_BRANCH}/unportable_centos7/sha1").trim()
                sh "curl ${UCLOUD_OSS_URL}/builds/pingcap/tikv/${tikv_sha1}/unportable_centos7/tikv-server.tar.gz | tar xz"
                // pd
                def pd_sha1 = sh(returnStdout: true, script: "curl ${UCLOUD_OSS_URL}/refs/pingcap/pd/${PD_BRANCH}/centos7/sha1").trim()
                sh "curl ${UCLOUD_OSS_URL}/builds/pingcap/pd/${pd_sha1}/centos7/pd-server.tar.gz | tar xz"
                stash includes: "bin/**", name: "binaries"
            }
        }

        stage('Test') {
            def tests = [:]

            tests["Unit Test"] = {
                node("k8s-ci-test") {
                    def ws = pwd()
                    deleteDir()
                    unstash 'tidb'

                    container('centos7') {
                        dir("go/src/github.com/pingcap/tidb") {
                            sh "GOPATH=${ws}/go:$GOPATH make test"
                        }
                    }
                }
            }

            tests["Race Test"] = {
                node("k8s-ci-test") {
                    def ws = pwd()
                    deleteDir()
                    unstash 'tidb'

                    container('centos7') {
                        dir("go/src/github.com/pingcap/tidb") {
                            sh "GOPATH=${ws}/go:$GOPATH make race"
                        }
                    }
                }
            }

            tests["TiDB Test"] = {
                node("k8s-ci-test") {
                    def ws = pwd()
                    deleteDir()
                    unstash 'tidb'
                    unstash 'tidb-test'

                    container('centos7') {
                        dir("go/src/github.com/pingcap/tidb-test") {
                            sh """
                                ln -s tidb/_vendor/src ../vendor
                                GOPATH=${ws}/go:$GOPATH make tidbtest
                                """
                        }
                    }
                }
            }

            tests["DDL Etcd Test"] = {
                node("k8s-ci-test") {
                    def ws = pwd()
                    deleteDir()
                    unstash 'tidb'
                    unstash 'tidb-test'

                    container('centos7') {
                        dir("go/src/github.com/pingcap/tidb-test") {
                            sh """
                                cd ddl_etcd_test && GOPATH=${ws}/go:$GOPATH ./run-tests.sh
                                """
                        }
                    }
                }
            }

            tests["MySQL Test"] = {
                node("k8s-ci-test") {
                    def ws = pwd()
                    deleteDir()
                    unstash 'tidb'
                    unstash 'tidb-test'

                    container('centos7') {
                        dir("go/src/github.com/pingcap/tidb-test") {
                            sh """
                                ln -s tidb/_vendor/src ../vendor
                                GOPATH=${ws}/go:$GOPATH make mysqltest
                                """
                        }
                    }
                }
            }

            tests["GORM Test"] = {
                node("k8s-ci-test") {
                    def ws = pwd()
                    deleteDir()
                    unstash 'tidb'
                    unstash 'tidb-test'

                    container('centos7') {
                        dir("go/src/github.com/pingcap/tidb-test") {
                            sh """
                                ln -s tidb/_vendor/src ../vendor
                                GOPATH=${ws}/go:$GOPATH make gormtest
                                """
                        }
                    }
                }
            }

            tests["Go SQL Test"] = {
                node("k8s-ci-test") {
                    def ws = pwd()
                    deleteDir()
                    unstash 'tidb'
                    unstash 'tidb-test'

                    container('centos7') {
                        dir("go/src/github.com/pingcap/tidb-test") {
                            sh """
                                ln -s tidb/_vendor/src ../vendor
                                GOPATH=${ws}/go:$GOPATH make gosqltest
                                """
                        }
                    }
                }
            }

            tests["Mybaits Test"] = {
                node("k8s-ci-test") {
                    def ws = pwd()
                    deleteDir()
                    unstash 'tidb'
                    unstash 'mybatis'

                    container('centos7') {
                        dir("go/src/github.com/pingcap/tidb") {
                            sh """
                                killall -9 tidb-server || true
                                bin/tidb-server --store memory -join-concurrency=1 > ${ws}/tidb_mybatis3_test.log 2>&1 &
                                """
                        }

                        try {
                            sh "mvn -B -f mybatis3/pom.xml -s /pingcap/.m2/settings.xml clean test"
                        } catch (err) {
                            sh "cat ${ws}/tidb_mybatis3_test.log"
                            throw err
                        } finally {
                            sh "killall -9 tidb-server || true"
                        }
                    }
                }
            }

            parallel tests
        }

        stage('Integration Test') {
            def tests = [:]



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
                    changeLogText += "\n" + "`${commitId.take(7)}` ${commitMsg}"
                }
            }
            return changeLogText
        }
        def changelog = getChangeLogText()
        def duration = ((System.currentTimeMillis() - currentBuild.startTimeInMillis) / 1000 / 60).setScale(2, BigDecimal.ROUND_HALF_UP)
        def slackmsg = "[${env.JOB_NAME.replaceAll('%2F','/')}-${env.BUILD_NUMBER}] `${currentBuild.result}`" + "\n" +
        "Elapsed Time: `${duration}` Mins" +
        "${changelog}" + "\n" +
        "${env.RUN_DISPLAY_URL}"

        if (currentBuild.result != "SUCCESS") {
            //slackSend channel: '#tidb', color: 'danger', teamDomain: 'pingcap', tokenCredentialId: 'slack-pingcap-token', message: "${slackmsg}"
        }
    }
}
