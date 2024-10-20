#!/bin/bash

# Format du NameNode si nécessaire
if [ ! -d "/home/$HDFS_NAMENODE_USER/hadoop/dfs/name" ]; then
    hdfs namenode -format
fi

# start hadoop and yarn
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh

# start spark
$SPARK_HOME/sbin/start-all.sh

# start history server
$SPARK_HOME/sbin/start-history-server.sh

# start jupyter
jupyter notebook --ip=0.0.0.0 --port=8888 --allow-root --IdentityProvider.token=