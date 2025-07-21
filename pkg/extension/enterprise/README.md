This repo maintains the tidb enterprise extensions code.

# How to develop and debug?

TiDB enterprise extensions code is based on *git submodule*,
1. Clone this repo to your local file system like `<user-home>/enterprise-extensions`.
```shell
cd <user-home>
git clone git@github.com:lcwangchao/enterprise-extensions.git
```

2. Set the submodule url in TiDB repo
```shell
cd <tidb-repo>
git submodule set-url extension/enterprise <user-home>/enterprise-extensions
cd extension/enterprise
git remote add local <user-home>/enterprise-extensions/.git
make enterprise-server
```

3. Modify extension's code and pull it into TiDB repo
```shell
cd <user-home>/enterprise-extensions
echo "Done!" > newfile.txt
git add newfile.txt
git commit -m "newfile"

cd <tidb-repo>/extension/enterprise
git pull local main
make enterprise-server
```