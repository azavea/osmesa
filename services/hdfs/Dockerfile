FROM quay.io/azavea/osmesa-base

MAINTAINER Pomadchin Grigory, daunnc@gmail.com

ENV HADOOP_VERSION 2.7.1
ENV HADOOP_HOME /usr/lib/hadoop
ENV HADOOP_CONF_DIR /etc/hadoop/conf

RUN set -x \
&& wget -nv -O /etc/yum.repos.d/bigtop.repo  http://www.apache.org/dist/bigtop/bigtop-1.1.0/repos/centos7/bigtop.repo \
&& yum install -y hadoop-hdfs-${HADOOP_VERSION} hadoop-mapreduce-${HADOOP_VERSION} openssl-devel nmap

COPY ./fs /
VOLUME ["/data/hdfs"]
ENTRYPOINT [ "/sbin/entrypoint.sh" ]
