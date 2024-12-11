#! /bin/bash

cur_path=`pwd`
tidb_security_advanced_dir="tidb-security-advanced"
tidb_enterprise_utilities_dir="tidb-enterprise-utilities"

# copy all files in the specified directory and subdirectories to the target directory,
# and keep the directory structure the same.
function cp_dir(){
   local src_dir=$1
   local dst_dir=$2

   # determine whether the source and destination are both files or directories.
   # if the source and destination are files, the file is copied directly.
   if [ ! -d $src_dir ];then
      if [ ! -d $dst_dir ];then 
         cp -v $src_dir $dst_dir 
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
echo "NEED_COPY: "$NEED_COPY

if  [ -z ${NEED_COPY} ] || [ ${NEED_COPY} != "NO" ];then
    copy_security_advanced_files
    if [ $? -ne 0 ];then 
        echo "copy security advanced files fail."
        exit 1
    fi
fi

if [ -z ${NEED_COPY} ] || [ $NEED_COPY !=  "NO" ];then 
    copy_utilities_files
    if [ $? -ne 0 ];then 
       echo "copy utilities files fail."
       exit 1
    fi
fi
