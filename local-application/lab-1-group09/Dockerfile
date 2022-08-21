FROM openjdk:11 as base

FROM base as spark-base
ARG HADOOP_VERSION=3.2.2
ENV HADOOP_HOME=/hadoop
ENV HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:/hadoop/share/hadoop/tools/lib/*
WORKDIR /hadoop
RUN curl -L http://ftp.tudelft.nl/apache/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz | tar xz --strip-components=1
ARG SPARK_VERSION=3.1.2
ARG SPARK_LOG_DIRECTORY=/spark-events
ENV SPARK_LOG_DIRECTORY=${SPARK_LOG_DIRECTORY}
WORKDIR /spark
RUN curl -L http://ftp.tudelft.nl/apache/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-without-hadoop.tgz | tar xz --strip-components=1 && \
    echo "export SPARK_DIST_CLASSPATH=$(/hadoop/bin/hadoop classpath)" >> conf/spark-env.sh

FROM spark-base as spark-history-server
ENV SPARK_NO_DAEMONIZE=1
RUN echo "spark.history.fs.logDirectory file:${SPARK_LOG_DIRECTORY}" >> conf/spark-defaults.conf
EXPOSE 18080
ENTRYPOINT ["./sbin/start-history-server.sh"]

FROM spark-base as spark-submit
RUN echo "spark.eventLog.enabled true" >> conf/spark-defaults.conf && \
    echo "spark.eventLog.dir file:${SPARK_LOG_DIRECTORY}" >> conf/spark-defaults.conf
WORKDIR /io
ENTRYPOINT ["/spark/bin/spark-submit"]

FROM spark-base as spark-shell
WORKDIR /io
ENTRYPOINT ["/spark/bin/spark-shell"]

FROM base as osm2orc
ARG OSM2ORC_VERSION=0.5.5
WORKDIR /osm2orc
RUN curl -L https://github.com/mojodna/osm2orc/releases/download/v${OSM2ORC_VERSION}/osm2orc-${OSM2ORC_VERSION}.tar.gz | tar xz --strip-components=1
ENTRYPOINT ["/osm2orc/bin/osm2orc"]
