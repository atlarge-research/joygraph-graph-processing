#!/bin/bash
YARN=/opt/hadoop-2.7.2
PROGRAMS=/home/sietse/Programming/IdeaProjects/joygraph-programs
CONF=/home/sietse/Programming/IdeaProjects/joygraph/local_conf.conf
NAME=TEST
sbt publish-local publish
cd ${PROGRAMS}
sbt assembly
JAR=${PROGRAMS}/target/scala-2.12.0-M4/programs-assembly-0.1-SNAPSHOT.jar
${YARN}/bin/yarn jar ${JAR} io.joygraph.impl.hadoop.submission.YARNSubmissionClient ${JAR} ${CONF} ${NAME}