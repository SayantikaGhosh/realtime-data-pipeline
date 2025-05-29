FROM bitnami/spark:3.3.0

USER root

RUN apt-get update && \
    apt-get install -y python3-pip openjdk-11-jdk && \
    pip3 install delta-spark==2.1.0 kafka-python

ENV PYSPARK_PYTHON=python3
