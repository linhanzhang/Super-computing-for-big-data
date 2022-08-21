FROM openjdk:11 as base
ARG KAFKA_VERSION=3.0.0
ARG KAFKA_SCALA_VERSION=2.13
WORKDIR /kafka
RUN curl -L http://ftp.tudelft.nl/apache/kafka/${KAFKA_VERSION}/kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}.tgz | tar xz --strip-components=1

FROM base as zookeeper-server
EXPOSE 2181
ENTRYPOINT ["bin/zookeeper-server-start.sh", "config/zookeeper.properties"]

FROM base as kafka-server
EXPOSE 9092
ENTRYPOINT ["bin/kafka-server-start.sh", "config/server.properties"]

FROM base as kafka-console-consumer
ENTRYPOINT ["bin/kafka-console-consumer.sh"]

FROM rust:1.55 as rust-kafka
RUN apt-get update && apt-get install -y cmake
ARG target
ENV target=${target}
ADD ./$target /src
WORKDIR /src
RUN cargo build --release
CMD /src/target/release/${target}

FROM rust-kafka as consumer
RUN curl -sL https://deb.nodesource.com/setup_16.x | bash - && \
  apt-get install -y nodejs
WORKDIR /src/visualizer
RUN npm install && \
  npm run bundle && \
  mv dist/ /dist
EXPOSE 1234
CMD /src/target/release/${target}
