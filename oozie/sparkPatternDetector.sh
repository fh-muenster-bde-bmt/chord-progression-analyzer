#!/bin/bash

/home/hbase/spark-1.6.0-bin-hadoop2.6/bin/spark-submit --class "bde.SparkPatternDetector.App" --deploy-mode client --driver-memory 512m --num-executors 3 --executor-memory 512m --executor-cores 4 --jars /usr/hdp/current/hbase-client/lib/hbase-client.jar,/usr/hdp/current/hbase-client/lib/hbase-protocol.jar,/usr/hdp/current/hbase-client/lib/hbase-common.jar,/usr/hdp/current/hbase-client/lib/htrace-core-3.1.0-incubating.jar,/usr/hdp/current/hbase-client/lib/guava-12.0.1.jar --master yarn /home/hbase/SparkPatternDetector.jar

