FROM quay.io/azavea/osmesa-hbase:latest

MAINTAINER Pomadchin Grigory, daunnc@gmail.com

COPY ./osmesa-query.jar /opt/osmesa-query.jar

RUN useradd -m query

RUN chown -R query:query /etc/hadoop/conf/

RUN chmod -R +r /etc/hadoop/conf/

USER query

CMD java -jar /opt/osmesa-query.jar
