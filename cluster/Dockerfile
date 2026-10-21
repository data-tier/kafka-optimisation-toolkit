# The image every node and the client container run: the official Apache Kafka
# image plus the Prometheus JMX exporter agent and the two small bundled Java
# tools from bench/java. Build context is the repository root.

ARG KAFKA_IMAGE=apache/kafka:4.3.0
ARG JDK_IMAGE=eclipse-temurin:21.0.11_10-jdk-alpine

FROM ${KAFKA_IMAGE} AS kafka

# The Kafka image ships a JRE only, so the bundled tools are compiled in a JDK
# image of the same Java release, against the kafka-clients jar from the image.
FROM ${JDK_IMAGE} AS javatools
COPY --from=kafka /opt/kafka/libs /opt/kafka/libs
COPY bench/java/src /src
RUN mkdir /classes \
 && javac --release 21 -proc:none -Xlint:all -Werror -cp '/opt/kafka/libs/*' -d /classes $(find /src -name '*.java')

FROM kafka
ARG JMX_EXPORTER_VERSION=1.7.0
ARG JMX_EXPORTER_SHA256=0ad843261c567d1924a017d542e14c40f2afff523d940c25ee000742fde4b3a9
ADD --checksum=sha256:${JMX_EXPORTER_SHA256} --chmod=644 \
    https://github.com/prometheus/jmx_exporter/releases/download/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar \
    /opt/kot/jmx_prometheus_javaagent.jar
COPY --from=javatools /classes /opt/kot/classes
COPY --chmod=644 cluster/jmx-exporter/broker.yml cluster/jmx-exporter/controller.yml /opt/kot/jmx/
COPY --chmod=755 cluster/start-node.sh /opt/kot/start-node.sh
ENTRYPOINT ["/opt/kot/start-node.sh"]
