#!/usr/bin/env bash

if [ $# -lt 1 ];
then
        echo "USAGE: $0 <out_file>"
        exit 1
fi

CURRENT=`pwd` && cd `dirname $0` && SOURCE=`pwd` && cd ${CURRENT} && PARENT=`dirname ${SOURCE}`

CLASSPATH=${CLASSPATH}:${PARENT}/config
for file in ${PARENT}/lib/*.jar;
do
    CLASSPATH=${CLASSPATH}:${file}
done

java ${JVM_OPTIONS} -cp ${CLASSPATH} io.wizzie.reputation.otx.OtxService generate $1