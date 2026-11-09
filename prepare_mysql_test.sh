#! /bin/bash

tidb_ee_test_dir="tidb-ee-test"
tidb_ee_test_branch="eev6.5.1"

# get tidb_ee_test value from version.txt.
function get_code_branches(){
   echo "begin get branches."
   tidb_ee_test_branch=`grep -Po '(?<=tidb_ee_test_branch=).*' version.txt`
   if [ $? -ne 0 ];then 
      echo "get tidb_ee_test_branch fail."
      return 1
   fi
   return 0
}

# get tidb code with barnch name from gitee.
function get_tidb_code(){
    tmp_pr=`echo $PR_TITLE|grep -Eo "tidb_ee_test_branch_pr=[0-9]+||"`
    TMP_PR=$tmp_pr
    test_pr="${TMP_PR#*=}"
    echo $test_pr
    if [[ -n $test_pr ]]; then
	    git config --global user.email "you@example.com"
        cd ..
	    cd $tidb_ee_test_dir
	    git fetch git@gitee.com:pingcap_enterprise/tidb-ee-test.git pull/$test_pr/head:pr_$test_pr
	    git checkout pr_$test_pr
	    git pull --rebase git@gitee.com:pingcap_enterprise/tidb-ee-test.git $tidb_ee_test_branch
   fi
   echo "end get tidb_security_advanced code."
   return 0
}


# main 
get_code_branches
if [ $? -ne 0 ];then 
   echo "get branch fail."
   exit 1
fi
echo $tidb_ee_test_branch
get_tidb_code
if [ $? -ne 0 ];then 
   echo "get tidb code fail."
   exit 1
fi
