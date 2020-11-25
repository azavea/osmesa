FROM bde2020/spark-master:2.3.1-hadoop2.7

COPY sources.list /etc/apt
RUN \
  rm /etc/apt/sources.list.d/* \
  && apt-get update \
  && apt-get upgrade -y \
  && apt-get clean

RUN apt-get dist-upgrade -y

RUN \
  apt-get install -y --no-install-recommends postgresql-client \
  && rm -rf /var/lib/apt/lists/*

COPY osmesa-apps.jar /opt/osmesa-apps.jar
COPY streaming/log4j.properties /spark/conf/
COPY refresh-views.sh /usr/local/bin/refresh-views.sh

WORKDIR /opt
