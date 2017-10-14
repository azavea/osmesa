FROM quay.io/azavea/osmesa-geoserver

ENV ACCUMULO_VERSION ${ACCUMULO_VERSION}
ENV THRIFT_VERSION ${THRIFT_VERSION}
ENV HADOOP_VERSION 2.7.3
ENV GEOSERVER_VERSION 2.9.4
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

# Install geomesa distribution
COPY .cache/geomesa-accumulo-dist_2.11-${GEOMESA_VERSION}-bin.tar.gz /tmp

RUN set -x \
  && cd /opt \
  && tar zxvf /tmp/geomesa-accumulo-dist_2.11-${GEOMESA_VERSION}-bin.tar.gz \
  && ln -s geomesa-accumulo_2.11-${GEOMESA_VERSION} geomesa


# Install GeoMesa Accumulo DataStore
RUN set -x \
    && cd /opt/tomcat/webapps/geoserver/WEB-INF/lib/ \
    && tar zxvf /opt/geomesa/dist/gs-plugins/geomesa-accumulo-gs-plugin_2.11-${GEOMESA_VERSION}-install.tar.gz

# Install GeoMesa Accumulo BlobStore
RUN set -x \
    && cd /opt/tomcat/webapps/geoserver/WEB-INF/lib/ \
    && tar zxvf /opt/geomesa/dist/gs-plugins/geomesa-blobstore-gs-plugin_2.11-${GEOMESA_VERSION}-install.tar.gz

# Install GeoMesa Stream Store
RUN set -x \
    && cd /opt/tomcat/webapps/geoserver/WEB-INF/lib/ \
    && tar zxvf /opt/geomesa/dist/gs-plugins/geomesa-stream-gs-plugin_2.11-${GEOMESA_VERSION}-install.tar.gz

# Install GeoMesa GeoJson Support
RUN set -x \
    && cd /opt/tomcat/webapps/geoserver/WEB-INF/lib/ \
    && tar zxvf /opt/geomesa/dist/gs-plugins/geomesa-geojson-gs-plugin_2.11-${GEOMESA_VERSION}-install.tar.gz

# Install GeoMesa Process Jar
RUN set -x \
    && cd /opt/tomcat/webapps/geoserver/WEB-INF/lib/ \
    && cp /opt/geomesa/dist/gs-plugins/geomesa-process-wps_2.11-${GEOMESA_VERSION}.jar .

# Install Hadoop and Accumulo specific jars
RUN set -x \
    && /opt/geomesa/bin/install-hadoop-accumulo.sh /opt/tomcat/webapps/geoserver/WEB-INF/lib -a ${ACCUMULO_VERSION} -h ${HADOOP_VERSION} -t ${THRIFT_VERSION}
RUN set -x \
    && rm /opt/tomcat/webapps/geoserver/WEB-INF/lib/guava-11.*.jar

RUN set -x \
  && chown root /opt/tomcat/webapps/geoserver/WEB-INF/lib/* \
  && chgrp root /opt/tomcat/webapps/geoserver/WEB-INF/lib/*

COPY server.xml /opt/tomcat/conf/server.xml
VOLUME ["/opt/tomcat/webapps/geoserver/data"]
EXPOSE 9090
CMD ["/opt/tomcat/bin/catalina.sh", "run"]
