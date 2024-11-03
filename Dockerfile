FROM apache/airflow:2.10.0
USER root
RUN apt-get update \
 && apt-get install -y procps \
 && apt-get install -y openjdk-17-jdk
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
USER airflow
RUN pip install apache-airflow-providers-apache-spark