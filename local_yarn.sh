#!/bin/bash
YARN=/opt/hadoop-2.7.2
rm -r /tmp/hadoop-*
${YARN}/bin/hdfs namenode -format -force
${YARN}/sbin/start-dfs.sh
${YARN}/sbin/start-yarn.sh
