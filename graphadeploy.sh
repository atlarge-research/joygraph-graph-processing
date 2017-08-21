#!/bin/bash
GRAPHALYTICS_JOYGRAPH=/home/sietse/Programming/IdeaProjects/graphalytics-platforms-joygraph
GRAPHALYTICS_RUNNER=/home/sietse/graphalyticsrunner/app/graphalytics-prebuild/joygraph/graphalytics-0.3-SNAPSHOT-joygraph-0.1-SNAPSHOT-bin.tar.gz
GRAPHALYTICS_RUNNER_PROJECT=/home/sietse/Programming/IdeaProjects/graphalytics-runner-fork
TARGET_SITE=das5tudelft
#TARGET_SITE=das5vu
USERNAME=stau

JOYGRAPH_PREBUILT=/var/scratch/${USERNAME}/graphalyticsrunner/app/graphalytics-prebuild/joygraph

# create directories
#ssh ${TARGET_SITE} "mkdir -p /var/scratch/${USERNAME}/graphalyticsrunner/app"
#ssh ${TARGET_SITE} "mkdir -p /var/scratch/${USERNAME}/graphalyticsrunner/report"
#ssh ${TARGET_SITE} "mkdir -p /var/scratch/${USERNAME}/graphalyticsrunner/result"
#ssh ${TARGET_SITE} "mkdir -p ${JOYGRAPH_PREBUILT}"
#
## create yarn directory
#ssh ${TARGET_SITE} "mkdir -p /var/scratch/${USERNAME}/graphalyticsrunner/app/yarn"

# get yarn distribution
#ssh ${TARGET_SITE} "cd /var/scratch/${USERNAME}/graphalyticsrunner/app/yarn; wget http://apache.cs.uu.nl/hadoop/common/hadoop-2.7.2/hadoop-2.7.2.tar.gz"

sbt publish
cd ${GRAPHALYTICS_JOYGRAPH}
mvn clean package -DskipTests -Pgranula
#scp -C ${GRAPHALYTICS_JOYGRAPH}/graphalytics-0.3-SNAPSHOT-joygraph-0.1-SNAPSHOT-bin.tar.gz ${TARGET_SITE}:${JOYGRAPH_PREBUILT}/.

#cd ${GRAPHALYTICS_RUNNER_PROJECT}
#mvn clean package
#scp -C ${GRAPHALYTICS_RUNNER_PROJECT}/target/graphalytics-runner-1.0.jar ${TARGET_SITE}:/home/${USERNAME}/.
