FROM flink:1.20.3-java17

ARG FLINK_VERSION=1.20.3
ARG KAFKA_CONNECTOR_VERSION=3.4.0-1.20

RUN wget -P /opt/flink/lib \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
    && wget -P /opt/flink/lib \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-avro/${FLINK_VERSION}/flink-avro-${FLINK_VERSION}.jar" \
    && wget -P /opt/flink/lib \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-avro-confluent-registry/${FLINK_VERSION}/flink-avro-confluent-registry-${FLINK_VERSION}.jar"
