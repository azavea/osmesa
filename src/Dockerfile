FROM quay.io/azavea/osmesa-hbase:latest

MAINTAINER Pomadchin Grigory, daunnc@gmail.com

ENV SPARK_VERSION 2.2.0
ENV SPARK_HOME /opt/spark
ENV SPARK_CONF_DIR $SPARK_HOME/conf
ENV PATH $PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

#Install spark
RUN set -x \
    && mkdir -p $SPARK_HOME $SPARK_CONF_DIR \
    && curl -sS -# http://d3kbcqa49mib13.cloudfront.net/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz \
    | tar -xz -C ${SPARK_HOME} --strip-components=1

COPY ./fs /opt

RUN mkdir -p /opt/src

WORKDIR /opt/src

CMD ["./sbt"]
