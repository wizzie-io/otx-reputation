FROM java:openjdk-8-jre

MAINTAINER agomez@wizzie.io

RUN apt-get update
RUN apt-get install -y gettext-base

RUN mkdir -p /var/log/reputation-otx

ADD reputation-otx.tar.gz /opt/
RUN rm -f /opt/reputation-otx/config/*

ADD log4j.xml /opt/reputation-otx/config/
ADD config_env.json /opt/reputation-otx/config/
ADD otx-service-start.sh /bin

ENV METRIC_ENABLE true
ENV METRIC_INTERVAL 60000
ENV INTERVAL_MS 1800000
ENV REPUTATION_TOPIC otx-reputation

ENV JVM_OPTIONS -Xmx512m -Xms512m

CMD /bin/otx-service-start.sh


