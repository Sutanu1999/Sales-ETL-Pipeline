FROM apache/airflow:2.9.3

USER root

# Install Java (required for PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

# Install PySpark, Delta Lake, and other pipeline dependencies
RUN pip install --no-cache-dir \
    pyspark==3.5.8 \
    delta-spark==3.2.1 \
    python-dotenv \
    pandas