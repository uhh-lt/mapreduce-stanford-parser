#!/usr/bin/env bash

if [ -z "$1" ] || [ -z "$3" ] ; then
    echo "Parse the corpus and save the dependency parses."
    echo "parameters: <corpus-directory> <output-directory> <cdh.config.sh>"
    exit
fi

source $3
input=$1
output=$2
outputFormat="conll"

echo "Corpus: $input"
if  $hadoop fs -test -e $input  ; then
    echo "Corpus exists: true"
else
    echo "Corpus exists: false"
fi
echo ""

echo "To start press any key, to stop press Ctrl+C"
read -n 2

jars=`echo $bin_hadoop/*.jar | tr " " ","`
path=`echo $bin_hadoop/*.jar | tr " " ":"`

HADOOP_CLASSPATH=$path $hadoop \
    de.uhh.lt.jst.CoNLL.HadoopMain \
    -libjars $jars \
    -Dmapreduce.reduce.failures.maxpercent=10 \
    -Dmapreduce.map.failures.maxpercent=10 \
    -Dmapreduce.job.queuename=$queue\
    -Dmapreduce.map.java.opts=-Xmx${hadoop_xmx_mb}m \
    -Dmapreduce.map.memory.mb=$hadoop_mb \
    -DoutputFormat=${outputFormat} \
    $input \
    $output \
    true