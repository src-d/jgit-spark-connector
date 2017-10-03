#!/bin/bash

# A helper script to get Apache Spark spark-shell on CI mahine
# Use TravisCI cache to minimize load on ASF mirrors



if [[ "$#" -ne 2 ]]; then
    echo "usage) $0 [spark version] [hadoop version]"
    echo "   eg) $0 2.2.0 2.7"
    exit 1
fi


SPARK_VERSION=$1
HADOOP_VERSION=$2

SPARK_CACHE=".spark-dist"
SPARK_ARCHIVE="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"


log() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
}

download_with_retry() {
    local url="$1"
    wget -O "${SPARK_ARCHIVE}.tgz" --retry-connrefused --waitretry=1 --read-timeout=20 --timeout=15 -t 3 "${url}"
    if [[ "$?" -ne 0 ]]; then
        log "3 download attempts for ${url} failed"
    fi
}

mkdir -p "${SPARK_CACHE}"
cd "${SPARK_CACHE}"
if [[ ! -f "${SPARK_ARCHIVE}.tgz" ]]; then
    pwd
    ls -la .
    log "${SPARK_CACHE} does not have ${SPARK_ARCHIVE}.tgz downloading ..."

    # download spark from archive if not cached
    log "$Apache Spark {SPARK_VERSION} being downloaded from archives"
    start_time=`date +%s`
    download_with_retry "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=spark/spark-${SPARK_VERSION}/${SPARK_ARCHIVE}.tgz"
    end_time=`date +%s`
    download_time="$((end_time-start_time))"
    log "Took ${download_time} sec"
else
    log "Using Apache Spark distr from ${SPARK_CACHE}"
fi
 
# extract archive in un-cached root, clean-up on failure
cp "${SPARK_ARCHIVE}.tgz" ..
cd ..
if ! tar zxf "${SPARK_ARCHIVE}.tgz" ; then
    log "Unable to extract ${SPARK_ARCHIVE}.tgz" >&2
    rm -rf "${SPARK_ARCHIVE}"
    rm -f "${SPARK_ARCHIVE}.tgz"
fi

mv "${SPARK_ARCHIVE}" spark
