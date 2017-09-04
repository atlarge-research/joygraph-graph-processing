#!/bin/bash
GRAPHALYTICS_VERSION=0.3
PLATFORM_VERSION=0.1-SNAPSHOT
JOYGRAPH=/home/sietse/Programming/IdeaProjects/joygraph
PROGRAMS=/home/sietse/Programming/IdeaProjects/joygraph-programs
GRAPHAYLTICS_DIR=/home/sietse/Programming/IdeaProjects/graphalytics0.3
GRAPHALYTICS_PLATFORM_JOYGRAPH=/home/sietse/Programming/IdeaProjects/graphalytics-platforms-joygraph/graphalytics-platforms-joygraph-platform
GRAPHALYTICS_JOYGRAPH=/home/sietse/Programming/IdeaProjects/graphalytics-platforms-joygraph
GRAPHALYTICS_RUNNER_PROJECT=/home/sietse/Programming/IdeaProjects/graphalytics-runner-fork
#TARGET_SITE=das5tudelft
TARGET_SITE=das5vu
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

cd ${JOYGRAPH}
sbt clean publish publish-local
cd ${PROGRAMS}
sbt clean publish publish-local

cd ${GRAPHAYLTICS_DIR}
git checkout v0.3
mvn install

cd ${GRAPHALYTICS_PLATFORM_JOYGRAPH}
git checkout alpha
mvn clean install -DskipTests

cd ${GRAPHALYTICS_JOYGRAPH}
mvn clean package -DskipTests
scp -C ${GRAPHALYTICS_JOYGRAPH}/graphalytics-${GRAPHALYTICS_VERSION}-joygraph-${PLATFORM_VERSION}-bin.tar.gz ${TARGET_SITE}:${JOYGRAPH_PREBUILT}/.

cd ${GRAPHALYTICS_RUNNER_PROJECT}
mvn clean package
scp -C ${GRAPHALYTICS_RUNNER_PROJECT}/target/graphalytics-runner-1.0.jar ${TARGET_SITE}:/home/${USERNAME}/.
