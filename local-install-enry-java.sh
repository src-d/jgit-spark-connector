#!/bin/bash

# Workaround to get enry-java + libenry
# Should be removed when tech.sourced:enry-java:1.0 is published

alreadyInstalled="$HOME/.ivy2/local/tech.sourced/enry-java"
repoUrl="https://github.com/src-d/enry.git"

if [[ -d "${alreadyInstalled}" ]]; then
    echo "Found local version in '${alreadyInstalled}' skipping installation"
    exit 0
fi

git clone "${repoUrl}" enry
if [[ "$?" -ne 0 ]]; then
  echo "Failed to clone ${repoUrl}"
  exit 1
fi

mkdir -p "$GOPATH/src/gopkg.in/src-d"
ln -s "${PWD}/enry" "$GOPATH/src/gopkg.in/src-d/enry.v1"
cd enry
go get ./...
cd -

cd enry/java
make
./sbt publish-local
./sbt publishM2
cd -
