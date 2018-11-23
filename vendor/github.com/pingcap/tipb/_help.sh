GOGO_ROOT=${GOPATH}/src/github.com/gogo/protobuf

function check_gogo_exist_and_version(){
	if [ ! -d $GOGO_ROOT ]; then
		echo "please use the following command to get specific version of gogo.\n\n"
		echo "go get -u github.com/gogo/protobuf/protoc-gen-gofast"
		echo "cd ${GOPATH}/src/github.com/gogo/protobuf"
		echo "git checkout v0.5"
		echo "rm ${GOPATH}/bin/protoc-gen-gofast"
		echo "go get github.com/gogo/protobuf/protoc-gen-gofast"
		exit 1
	else  
		cur_dir=$PWD
		cd ${GOPATH}/src/github.com/gogo/protobuf
		if [ $(git describe --tags) != 'v0.5' ]; then
			echo "please use v0.5 tag of protobuf"
			echo "cd ${GOPATH}/src/github.com/gogo/protobuf"
			echo "git checkout v0.5"
			cd $cur_dir
			exit 1
		fi

		cd $cur_dir
	fi
}
