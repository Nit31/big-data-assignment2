#!/bin/bash
source .venv/bin/activate

# Default input path if not provided
INPUT_PATH=${1:-/index/data}

# Create necessary directories in HDFS
hdfs dfs -mkdir -p /tmp/index

# Determine if input is local or in HDFS
if [[ $INPUT_PATH == /* ]] || [[ $INPUT_PATH == hdfs://* ]]; then
    # Input is already in HDFS
    HDFS_INPUT=$INPUT_PATH
else
    # Input is local, copy to HDFS
    echo "Copying local file/folder to HDFS..."
    hdfs dfs -mkdir -p /tmp/input
    hdfs dfs -put -f $INPUT_PATH /tmp/input/
    HDFS_INPUT="/tmp/input/$(basename $INPUT_PATH)"
fi

# Set up Cassandra schema if it doesn't exist
echo "Setting up Cassandra schema..."
python3 app.py

############################
# Run Term Frequency MapReduce Job
echo "Running Term Frequency MapReduce..."

mapred streaming \
    -D mapreduce.job.reduces=10 \
    -D stream.num.map.output.key.fields=2 \
    -D mapreduce.partition.keypartitioner.options="-k1,2" \
    -D mapreduce.partition.keycomparator.options="-k1,1 -k2,2" \
    -files "$(pwd)/mapreduce/mapper1.py,$(pwd)/mapreduce/reducer1.py,$(pwd)/libs.zip" \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -input $HDFS_INPUT \
    -output /tmp/index/output_tf
############################
# Run MapReduce2 Job
echo "Running Document Frequency MapReduce..."

mapred streaming \
    -D mapred.reduce.tasks=4 \
    -files "$(pwd)/mapreduce/mapper2.py,$(pwd)/mapreduce/reducer2.py,$(pwd)/libs.zip" \
    -mapper "python3 mapper2.py" \
    -reducer "python3 reducer2.py" \
    -input $HDFS_INPUT \
    -output /tmp/index/output_df
# ############################
# # Run Stats Aggregation MapReduce Job
echo "Running Stats Aggregation MapReduce..."

mapred streaming \
    -D mapred.reduce.tasks=10 \
    -files "$(pwd)/mapreduce/mapper3.py,$(pwd)/mapreduce/reducer3.py,$(pwd)/libs.zip" \
    -mapper "python3 mapper3.py" \
    -reducer "python3 reducer3.py" \
    -input $HDFS_INPUT \
    -output /tmp/index/output_stats

# ############################

echo "Indexing done. Testing the indexer..."

python3 test_cassandra.py
