# Dockerfile for Kafka

FROM debian:jessie
MAINTAINER Daniel Collins <daniel.collins@viasat.com>

ENV DEBIAN_FRONTEND noninteractive

# System Packages
RUN apt-get update && apt-get install -y openjdk-7-jre-headless wget python-pip
# Python Packages
RUN pip install --upgrade pip virtualenv

# Get Python ZooKeeper (Kazoo)
RUN pip install kazoo

# Get Kafka
RUN wget -q -O - http://mirror.cogentco.com/pub/apache/kafka/0.8.2.0/kafka_2.11-0.8.2.0.tgz | tar -xzf - -C /opt

ENV JAVA_HOME /usr/lib/jvm/java-7-openjdk-amd64
ENV KAFKA_HOME /opt/kafka_2.11-0.8.2.0

# Get latest available release of Kafka (no stable release yet).
RUN mkdir -p /opt/kafka_2.11-0.8.2.0

# Add the run wrapper script.
# Script is necessary to generate the config based on environment variables
ADD configure.py /opt/kafka_2.11-0.8.2.0/bin/

WORKDIR /opt/kafka_2.11-0.8.2.0

EXPOSE 9092

ENTRYPOINT python bin/configure.py && bin/kafka-server-start.sh config/server.properties
