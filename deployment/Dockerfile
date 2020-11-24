FROM bde2020/spark-master:2.3.1-hadoop2.7

RUN \
  apt update \
  && apt install -y --no-install-recommends postgresql-client \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

COPY osmesa-apps.jar /opt/osmesa-apps.jar
COPY streaming/log4j.properties /spark/conf/
COPY refresh-views.sh /usr/local/bin/refresh-views.sh

WORKDIR /opt
