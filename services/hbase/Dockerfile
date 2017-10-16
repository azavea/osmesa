FROM quay.io/azavea/osmesa-hdfs

ENV HBASE_HOME /opt/hbase
ENV HBASE_CONF_DIR $HBASE_HOME/conf
ENV HBASE_DATA_DIR /opt/hbbase-data
ENV HBASE_VERSION 1.3.1
ENV PATH $PATH:$HBASE_HOME/bin

ARG GEOMESA_DIST_PATH

# Install native libs for hadoop
RUN yum install -y snappy snappy-devel lzo lzo-devel hadooplzo hadooplzo-native openssl-devel autoremove hostname ssh

RUN set -x && \
    mkdir -p $HBASE_HOME $HBASE_CONF_DIR $HBASE_DATA_DIR && \
    curl -# http://apache.mirrors.tds.net/hbase/$HBASE_VERSION/hbase-$HBASE_VERSION-bin.tar.gz | tar -xz -C ${HBASE_HOME} --strip-components=1

COPY ./fs /
COPY geomesa-dist.tar.gz /root/geomesa-dist.tar.gz
RUN mkdir -p /tmp/jar && \
    tar -xf /root/geomesa-dist.tar.gz -C /tmp/jar && \
    JAR=$(find /tmp/jar -name "*distributed-runtime*.jar") && \
    mv $JAR /opt/hbase/lib/geomesa-dist.jar && \
    rm -r /tmp/jar && \
    rm /root/geomesa-dist.tar.gz

# Stargate  8080  / 8085
# Thrift    9090  / 9095
# HMaster   16000 / 16010
# RS        16201 / 16301
EXPOSE 2181 8080 8085 9090 9095 16000 16010 16201 16301

WORKDIR "${HBASE_HOME}"
CMD [ "/sbin/entrypoint.sh" ]
