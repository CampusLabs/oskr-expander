FROM quay.io/orgsync/clojure:2.5.3
MAINTAINER Lars Levie <llevie@campuslabs.com>

WORKDIR /code
COPY . /code/

RUN lein uberjar \
    && mkdir /opt/oskr-expander \
    && mv /code/target/oskr-expander.jar /opt/oskr-expander/oskr-expander.jar \
    && rm -Rf /code \
    && rm -Rf /root/.m2

WORKDIR /opt/oskr-expander

ENV HEAP_SIZE 200m
ENV KAFKA_BOOTSTRAP kafka:9092
ENV KAFKA_GROUP_ID oskr-expander
ENV KAFKA_SPEC_TOPIC Communications.Specifications
ENV KAFKA_PART_TOPIC Communications.MessageParts

CMD exec java \
    -server \
    -XX:+UseG1GC \
    -Xmx${HEAP_SIZE} \
    -Xms${HEAP_SIZE} \
    -XX:MaxGCPauseMillis=1000 \
    -XX:+AggressiveOpts \
    -jar oskr-expander.jar
