FROM openjdk:8-jre-slim

MAINTAINER agomez@wizzie.io

RUN apt-get update && apt-get install -y gettext-base && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /var/log/reputation-otx

ADD reputation-otx.tar.gz /opt/
RUN rm -f /opt/reputation-otx/config/*

ADD log4j2.xml /opt/reputation-otx/config/
ADD config_env.json /opt/reputation-otx/config/
ADD otx-service-start.sh /bin

ENV METRIC_ENABLE=true \
    METRIC_INTERVAL=60000 \
    INTERVAL_MS=1800000 \
    REPUTATION_TOPIC=otx-reputation \
    BOOTSTRAP_KAFKA_TOPICS=[\"__reputation_otx_bootstrap\"] \
    METRIC_KAFKA_TOPIC=__metrics \
    JVM_OPTIONS="-Xmx512m -Xms512m"

CMD /bin/otx-service-start.sh


