FROM quay.io/azavea/osmesa-hbase

ARG HADOOP_VERSION
ARG ZOOKEEPER_VERSION
ARG HBASE_VERSION

ENV GEOSERVER_VERSION 2.9.4

ENV ACCUMULO_VERSION ${ACCUMULO_VERSION}
ENV THRIFT_VERSION ${THRIFT_VERSION}
ENV GEOMESA_VERSION ${GEOMESA_VERSION}
ENV TOMCAT_VERSION 8.0.35
ENV CATALINA_OPTS "-Xmx8g -XX:MaxPermSize=512M -Duser.timezone=UTC -server -Djava.awt.headless=true"

# Install tomcat
RUN set -x \
  && groupadd tomcat \
  && useradd -M -s /bin/nologin -g tomcat -d /opt/tomcat tomcat \
  && mkdir -p /opt/tomcat/webapps/geoserver \
  && curl -sS  https://archive.apache.org/dist/tomcat/tomcat-8/v${TOMCAT_VERSION}/bin/apache-tomcat-${TOMCAT_VERSION}.tar.gz \
  | tar -zx -C /opt/tomcat --strip-components=1

RUN set -x \
  && curl -sS -L -o /tmp/geoserver-war.zip \
    http://sourceforge.net/projects/geoserver/files/GeoServer/${GEOSERVER_VERSION}/geoserver-${GEOSERVER_VERSION}-war.zip \
  && unzip /tmp/geoserver-war.zip geoserver.war -d /tmp \
  && unzip /tmp/geoserver.war -d /opt/tomcat/webapps/geoserver \
  && rm -rf /tmp/geoserver-war.zip /tmp/geoserver.war /opt/tomcat/webapps/geoserver/META-INF

# Install geoserver WPS plugin
RUN set -x \
  && curl -sS -L -o /tmp/geoserver-wps.zip \
    http://sourceforge.net/projects/geoserver/files/GeoServer/${GEOSERVER_VERSION}/extensions/geoserver-${GEOSERVER_VERSION}-wps-plugin.zip \
  && unzip -j /tmp/geoserver-wps.zip -d /opt/tomcat/webapps/geoserver/WEB-INF/lib/ \
  && rm -rf /tmp/geoserver-wps.zip

ENV TOMCAT_LIBS /opt/tomcat/webapps/geoserver/WEB-INF/lib/

# Install geomesa HBase distribution
COPY geomesa-hbase-dist.tar.gz /root/geomesa-hbase-dist.tar.gz
RUN mkdir -p /tmp/dist && \
    tar -xf /root/geomesa-hbase-dist.tar.gz -C /tmp/dist && \
    PLUGIN_TAR=$(find /tmp/dist -name "*gs-plugin*.tar.gz") && \
    tar -xf $PLUGIN_TAR -C /tmp/dist && \
    PLUGIN_JAR=$(find /tmp/dist -name "*gs-plugin*.jar") && \
    mv $PLUGIN_JAR $TOMCAT_LIBS && \
    WPS_JAR=$(find /tmp/dist -name "*process-wps*.jar") && \
    mv $WPS_JAR $TOMCAT_LIBS && \
    INSTALL_HADOOP=$(find /tmp/dist -name "*install-hadoop.sh") && \
    $INSTALL_HADOOP $TOMCAT_LIBS -h $HADOOP_VERSION -z $ZOOKEEPER_VERSION && \
    INSTALL_HBASE=$(find /tmp/dist -name "*install-hbase.sh") && \
    $INSTALL_HBASE $TOMCAT_LIBS -h $HBASE_VERSION && \
    rm -r /tmp/dist && \
    rm /root/geomesa-hbase-dist.tar.gz

RUN set -x \
  && chown root /opt/tomcat/webapps/geoserver/WEB-INF/lib/* \
  && chgrp root /opt/tomcat/webapps/geoserver/WEB-INF/lib/*

COPY server.xml /opt/tomcat/conf/server.xml
RUN ln -s /opt/hbase/conf/hbase-site.xml /opt/tomcat/webapps/geoserver/WEB-INF/classes/hbase-site.xml
VOLUME ["/opt/tomcat/webapps/geoserver/data"]
EXPOSE 9090
CMD ["/opt/tomcat/bin/catalina.sh", "run"]
