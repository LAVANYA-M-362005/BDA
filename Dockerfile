FROM bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8

USER root
RUN sed -i \
    -e 's|deb.debian.org/debian|archive.debian.org/debian|g' \
    -e 's|security.debian.org/debian-security|archive.debian.org/debian-security|g' \
    -e '/stretch-updates/d' \
    /etc/apt/sources.list \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install -y --no-install-recommends python3 python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN python3 -m venv /opt/pipeline-venv
ENV PATH=/opt/pipeline-venv/bin:$PATH

COPY pipeline-requirements.txt ./pipeline-requirements.txt
RUN pip install --no-cache-dir -r pipeline-requirements.txt

COPY producer ./producer
COPY consumer ./consumer
COPY mapreduce ./mapreduce
COPY scheduler ./scheduler
RUN sed -i 's/\r$//' ./scheduler/*.sh
COPY data ./data

ENV HADOOP_HOME=/opt/hadoop-3.2.1 \
    KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    KAFKA_TOPIC=sales-transactions \
    HDFS_URL=http://namenode:9870 \
    HDFS_INPUT_DIR=/sales/input \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
