#!/bin/bash

BASE_DIR=$(dirname $0)
MAVEN_DIR=~/.m2/repository
CLASSPATH=$CLASSPATH:${MAVEN_DIR}/log4j/log4j/1.2.17/log4j-1.2.17.jar:${MAVEN_DIR}/com/google/guava/guava/12.0/guava-12.0.jar:${MAVEN_DIR}/net/sf/jopt-simple/jopt-simple/4.3/jopt-simple-4.3.jar:$BASE_DIR/target/valencia-1.0-SNAPSHOT.jar:$BASE_DIR/target/test-classes

if [ -z "$JVM_OPTS" ]; then
  JVM_OPTS=" -Xms512M -Xmx512M "
fi

if [ -n "$GC_LOG" ]; then
  GC_LOG_OPT="-Xloggc:$GC_LOG"
fi

if [ -z "$LOG4J_CONFIG" ]; then
  LOG4J_CONFIG="src/main/java/log4j.properties"
fi

if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi



$JAVA -server $JVM_OPTS -XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps $GC_LOG_OPT -Dlog4j.configuration=file:$LOG4J_CONFIG -classpath $CLASSPATH $@