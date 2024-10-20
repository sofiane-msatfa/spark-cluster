ARG JAVA_VERSION=17

FROM eclipse-temurin:${JAVA_VERSION}-jammy

WORKDIR /root

ENV DEBIAN_FRONTEND=noninteractive

# install packages
RUN apt update -q && apt install -y --no-install-recommends \
    openssh-server \
    nano \
    unzip \
    python3 \
    python3-pip \
    sudo \
    && apt clean

# alias for python3 and pip3
RUN ln -s /usr/local/bin/python3.11 /usr/bin/python

# remove apt cache
RUN rm -rf /var/lib/apt/lists/*

# download hadoop
ENV HADOOP_VERSION=3.4.0
RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} /usr/local/hadoop && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

# download spark
ENV SPARK_VERSION=3.4.3
RUN wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \ 
    tar -xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \ 
    mv spark-${SPARK_VERSION}-bin-hadoop3 /usr/local/spark && \ 
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# download postgresql driver
RUN wget https://jdbc.postgresql.org/download/postgresql-42.7.4.jar -P /usr/local/spark/jars

# set environment vars
ENV HADOOP_HOME=/usr/local/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV LD_LIBRARY_PATH=$HADOOP_HOME/lib/native

ENV SPARK_HOME=/usr/local/spark
ENV SPARK_MASTER_PORT=7077
ENV JAVA_HOME=/opt/java/openjdk

ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$JAVA_HOME/bin

# download python packages
RUN pip3 install --upgrade setuptools && \
    pip3 install jupyter && \
    pip3 install py4j && \
    pip3 install pyspark==${SPARK_VERSION} && \
    pip3 install psycopg2-binary && \
    pip3 install pandas 

# ssh without key
RUN ssh-keygen -t rsa -f ~/.ssh/id_rsa -P '' && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

# copy configs
COPY /config/hadoop/* /tmp/hadoop/
COPY /config/spark/* /tmp/spark/
COPY /config/ssh_config /tmp/
COPY /config/start-cluster.sh /tmp/

RUN mv /tmp/ssh_config ~/.ssh/config && \
    mv /tmp/hadoop/hadoop-env.sh $HADOOP_CONF_DIR/hadoop-env.sh && \
    mv /tmp/hadoop/hdfs-site.xml $HADOOP_CONF_DIR/hdfs-site.xml && \
    mv /tmp/hadoop/core-site.xml $HADOOP_CONF_DIR/core-site.xml && \
    mv /tmp/hadoop/mapred-site.xml $HADOOP_CONF_DIR/mapred-site.xml && \
    mv /tmp/hadoop/yarn-site.xml $HADOOP_CONF_DIR/yarn-site.xml && \
    mv /tmp/hadoop/slaves $HADOOP_CONF_DIR/slaves && \
    mv /tmp/spark/workers $SPARK_HOME/conf/workers && \
    mv /tmp/spark/spark-env.sh $SPARK_HOME/conf/spark-env.sh && \
    mv /tmp/spark/spark-default.conf $SPARK_HOME/conf/spark-default.conf && \
    mv /tmp/start-cluster.sh ~/start-cluster.sh

# create spark-events directory
RUN mkdir /tmp/spark-events && \
    chmod 777 /tmp/spark-events

RUN chmod u+r+x ~/start-cluster.sh

# remove CRLF
RUN sed -i 's/\r$//g' $HADOOP_CONF_DIR/slaves
RUN sed -i 's/\r$//g' $HADOOP_CONF_DIR/hadoop-env.sh
RUN sed -i 's/\r$//g' $SPARK_HOME/conf/workers
RUN sed -i 's/\r$//g' $SPARK_HOME/conf/spark-default.conf
RUN sed -i 's/\r$//g' $SPARK_HOME/conf/spark-env.sh
RUN sed -i 's/\r$//g' ~/start-cluster.sh
