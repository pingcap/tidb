#! /bin/bash

cur_path=`pwd`
tidb_security_advanced_dir="tidb-security-advanced"
tidb_enterprise_utilities_dir="tidb-enterprise-utilities"
tidb_security_advanced_member="pingcap_enterprise"
tidb_enterprise_utilities_member="pingcap_enterprise"
tidb_security_advanced_branch="master"
tidb_enterprise_utilities_branch="master"

# get tidb_security_advanced_branch and tidb_enterprise_utilities_branch value from version.txt.
function get_code_branches(){
   echo "begin get branches."
   # tidb-security-advanced
   tidb_security_advanced_member=`grep -Po '(?<=tidb_security_advanced_member=).*' ${cur_path}/version.txt`
   if [ $? -ne 0 ];then
      echo "get tidb_security_advanced_member fail."
      return 1
   fi
   tidb_security_advanced_branch=`grep -Po '(?<=tidb_security_advanced_branch=).*' ${cur_path}/version.txt`
   if [ $? -ne 0 ];then
      echo "get tidb_security_advanced_branch fail."
      return 1
   fi
   # tidb-enterprise-utilities
   tidb_enterprise_utilities_member=`grep -Po '(?<=tidb_enterprise_utilities_member=).*' ${cur_path}/version.txt`
   if [ $? -ne 0 ];then 
      echo "get tidb_enterprise_utilities_member fail."
      return 1
   fi
   tidb_enterprise_utilities_branch=`grep -Po '(?<=tidb_enterprise_utilities_branch=).*' ${cur_path}/version.txt`
   if [ $? -ne 0 ];then
      echo "get tidb_enterprise_utilities_branch fail."
      return 1
   fi
   echo "end get branches."
   return 0
}

# get tidb_security_advanced code with barnch name from gitee.
function get_tidb_security_advanced_code(){
   echo "begin get tidb_security_advanced code."
   if [ ! -d "$tidb_security_advanced_dir" ]; then
      git clone -b $tidb_security_advanced_branch git@gitee.com:$tidb_security_advanced_member/tidb-security-advanced.git
      if [ $? -ne 0 ];then
         echo "checkout ${tidb_security_advanced_member}/tidb_security_advanced_branch version ${tidb_security_advanced_branch} failure."
         return 1
      fi
      tmp_pr=`echo $PR_TITLE|grep -Eo "tidb_security_advanced_branch_pr=[0-9]+||"`
      TMP_PR=$tmp_pr
      security_pr="${TMP_PR#*=}"
      echo $security_pr
      if [[ -n $security_pr ]]; then
        git config --global user.email "you@example.com"
        cd $tidb_security_advanced_dir
        git fetch git@gitee.com:$tidb_security_advanced_member/tidb-security-advanced.git pull/$security_pr/head:pr_$security_pr
        git checkout pr_$security_pr
        git pull --rebase git@gitee.com:$tidb_security_advanced_member/tidb-security-advanced.git $tidb_security_advanced_branch
        cd ..
      fi
   fi
   echo "end get tidb_security_advanced code."
   return 0
}

# get tidb_enterprise_utilities code with barnch name from gitee.
function get_tidb_enterprise_utilities_code(){
   echo "begin get tidb_enterprise_utilities code."
   if [ ! -d "$tidb_enterprise_utilities_dir" ]; then
      git clone  -b $tidb_enterprise_utilities_branch git@gitee.com:$tidb_enterprise_utilities_member/tidb-enterprise-utilities.git
      if [ $? -ne 0 ];then
         echo "checkout tidb_enterprise_utilities ${tidb_enterprise_utilities_branch} failure."
         return 1
      fi
      tmp_pr=` echo $PR_TITLE|grep -Eo "tidb_enterprise_utilities_branch_pr=[0-9]+||"`
      TMP_PR=$tmp_pr
      utilities_pr="${TMP_PR#*=}"
      echo $utilities_pr
      if [[ -n $utilities_pr ]]; then
	git config --global user.email "you@example.com"
	cd $tidb_enterprise_utilities_dir 
	git fetch git@gitee.com:$tidb_enterprise_utilities_member/tidb-enterprise-utilities.git pull/$utilities_pr/head:pr_$utilities_pr
	git checkout pr_$utilities_pr  
	git pull --rebase git@gitee.com:$tidb_enterprise_utilities_member/tidb-enterprise-utilities.git $tidb_enterprise_utilities_branch
	cd ..
      fi 
   fi
   echo "end get tidb_enterprise_utilities code."
   return 0
}

# copy all files in the specified directory and subdirectories to the target directory,
# and keep the directory structure the same.
function cp_dir(){
   local src_dir=$1
   local dst_dir=$2

   # determine whether the source and destination are both files or directories.
   # if the source and destination are files, the file is copied directly.
   if [ ! -d $src_dir ];then
      if [ ! -d $dst_dir ];then 
         cp $src_dir $dst_dir 
         if [ $? -ne 0 ];then 
            echo "cp ${src_dir} ${dst_dir} fail."
            return 1 
         else 
            echo "cp ${src_dir} ${dst_dir} success."
            return 0
         fi 
      else 
          echo "${src_dir} and ${dst_dir} are not both file type."
          return 1
      fi 
   fi

   # if the desired directory does not exist, create it.
   if [ ! -d $dst_dir ];then 
      mkdir $dst_dir
      if [ $? -ne 0 ];then
         echo "make targer dir ${dst_dir} fail."
         return 1
      fi     
   fi

   # copy all files in the specified directory and subdirectories to the target directory.
   local files=`ls $src_dir`
   for file_name in $files
   do
      local src_path="${src_dir}/${file_name}"
      local dst_path="${dst_dir}/${file_name}"
   
      cp_dir $src_path  $dst_path
      if [ $? -ne 0 ];then 
         echo "cp_dir ${src_path} to ${dst_path} fail."
         return 1
      fi    
   done
   return 0
}

# copy tidb_security_advanced files to code.
function copy_security_advanced_files(){
   echo "begin copy tidb_security_advanced files."
   for dir_name in `ls $cur_path/$tidb_security_advanced_dir`
   do
      if [ ${dir_name} = "tidb" ];then
         continue
      fi
      cur_dir="${cur_path}/${tidb_security_advanced_dir}/${dir_name}"
      if [ -d $cur_dir ];then
         if [ -d "${cur_dir}/tidb" ];then 
            cp_dir "${cur_dir}/tidb" "${cur_path}/"
            if [ $? -ne 0 ];then 
               echo "copy ${cur_dir}/tidb to ${cur_path}/tidb fail."
               return 1 
            fi
         fi
      fi
   done
   echo "end copy tidb_security_advanced files."
   return 0
}

# copy tidb_enterprise_utilities files to code.
function copy_utilities_files(){
   echo "begin copy tidb_enterprise_utilities files."
   for dir_name in `ls $cur_path/tidb-enterprise-utilities/`
   do
      cur_dir="${cur_path}/tidb-enterprise-utilities/${dir_name}"
      if [ -d $cur_dir ];then
         if [ $dir_name = "common" ];then 
           continue
         fi
         if [ -d "${cur_dir}/tidb" ];then 
            cp_dir "${cur_dir}/tidb" "${cur_path}/"
            if [ $? -ne 0 ];then 
               echo "copy ${cur_dir}/tidb to ${cur_path}/tidb fail."
               return 1 
            fi
         fi
      fi
   done
   echo "end copy tidb_enterprise_utilities files."
   return 0
}


# main 
get_code_branches
if [ $? -ne 0 ];then 
   echo "get branch fail."
   exit 1
fi
echo "security: "$tidb_security_advanced_member/$tidb_security_advanced_branch
echo "enterprise: "$tidb_enterprise_utilities_member/$tidb_enterprise_utilities_branch
echo "NEED_CLONE: "$NEED_CLONE

get_tidb_security_advanced_code
if [ $? -ne 0 ];then 
   echo "get tidb code fail."
   exit 1
fi
if  [ -z ${NEED_CLONE} ] || [ ${NEED_CLONE} != "NO" ];then
    copy_security_advanced_files
    if [ $? -ne 0 ];then 
        echo "copy security advanced files fail."
        exit 1
    fi
fi

get_tidb_enterprise_utilities_code
if [ $? -ne 0 ];then 
   echo "get tidb_enterprise_utilities code fail."
   exit 1
fi

if [ -z ${NEED_CLONE} ] || [ $NEED_CLONE !=  "NO" ];then 
    copy_utilities_files
    if [ $? -ne 0 ];then 
       echo "copy utilities files fail."
       exit 1
    fi
fi
