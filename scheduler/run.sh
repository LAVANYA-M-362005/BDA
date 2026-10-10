#!/bin/bash
set -eu

INPUT_PATH=/sales/input
OUTPUT_PATH=/sales/output/top-products
RESULT_PATH=/app/results/top10_products.csv
TEMP_RESULT=/app/results/top10_products.csv.tmp
STREAMING_JAR=$(find "${HADOOP_HOME:-/opt/hadoop-3.2.1}" -path '*/share/hadoop/tools/lib/hadoop-streaming-*.jar' -print -quit)

if [ -z "$STREAMING_JAR" ]; then
    echo "Could not find the Hadoop Streaming JAR under ${HADOOP_HOME:-/opt/hadoop-3.2.1}." >&2
    exit 1
fi

mkdir -p /app/results

while true; do
    if hdfs dfs -ls "$INPUT_PATH" 2>/dev/null | grep '\.json$' >/dev/null; then
        hdfs dfs -rm -r -f "$OUTPUT_PATH"
        hadoop jar "$STREAMING_JAR" \
            -D mapreduce.framework.name=local \
            -D mapreduce.job.reduces=1 \
            -input "$INPUT_PATH" \
            -output "$OUTPUT_PATH" \
            -mapper "python3 mapper.py" \
            -reducer "python3 reducer.py" \
            -file /app/mapreduce/mapper.py \
            -file /app/mapreduce/reducer.py

        hdfs dfs -cat "$OUTPUT_PATH"/part-* > "$TEMP_RESULT"
        mv "$TEMP_RESULT" "$RESULT_PATH"
        echo "Updated product rankings at $RESULT_PATH."
    else
        echo "Waiting for sales transactions in HDFS at $INPUT_PATH."
    fi
    sleep "${SCHEDULER_INTERVAL_SECONDS:-60}"
done
