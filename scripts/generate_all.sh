#!/usr/bin/env bash
set -e
CURRENT_DIR=$(pwd)
FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Configuration
NUM_WORKERS=16
NUM_BUCKETS=256
BASE_RECORDS_PER_BUCKET=3906250
ZIPF_SHAPES="0.00 0.20 0.40 0.60 0.80 1.00"
DATA_SKEW=0.002  # Partitioning skew is there anyway
MAX_KEY="100e6"

# Vars
GCS_BUCKET='gs://andrea_smb_test'
DATA_BUCKET="${GCS_BUCKET}/generated_data"

OUTPUT_DIR=$(realpath "${FILE_DIR}/../data")
SCHEMA="${GCS_BUCKET}/schemas/Record.avsc"

#gsutil -m rm -rf ${TMP_LOCATION}
TMP_LOCATION="${GCS_BUCKET}/tmp"
STAGING_LOCATION="${TMP_LOCATION}/staging"


DATAFLOW_ARGS="--numWorkers=${NUM_WORKERS} --tempLocation=${TMP_LOCATION} --stagingLocation=${STAGING_LOCATION} --diskSizeGb=50 --autoscalingAlgorithm=NONE --project=***REMOVED*** --runner=DataflowRunner --region=europe-west1 --workerMachineType=n1-standard-4"

echo "Copying schema..."
gsutil cp $(realpath "${FILE_DIR}/../schemas/Record.avsc") "${SCHEMA}/"


echo "Compiling..." && cd ${FILE_DIR}/.. && sbt ";compile ;pack"
echo "Cleaning up old data..." && gsutil -m rm -r ${DATA_BUCKET} || true
echo "Generating data..." &&
for s in ${ZIPF_SHAPES}; do
  OUTPUT_DIR_LEFT="${DATA_BUCKET}/s$s/lhs"
  OUTPUT_DIR_RIGHT="${DATA_BUCKET}/s$s/rhs"

  sStr=${s/./}
  time=$(date +%s)

  target/pack/bin/data-generator --jobName="datagenerator-s$sStr-lhs-$time-$( printf "%04x%04x" $RANDOM $RANDOM )" --count=$((NUM_BUCKETS * BASE_RECORDS_PER_BUCKET)) --output=${OUTPUT_DIR_LEFT} --zipfShape=$s --dataSkew=${DATA_SKEW} --numBuckets=${NUM_BUCKETS} --schemaFile=${SCHEMA} ${DATAFLOW_ARGS}
  target/pack/bin/data-generator --jobName="datagenerator-s$sStr-rhs-$time-$( printf "%04x%04x" $RANDOM $RANDOM )" --count=$((NUM_BUCKETS * BASE_RECORDS_PER_BUCKET * 12/10)) --output=${OUTPUT_DIR_RIGHT} --zipfShape=$s --dataSkew=${DATA_SKEW} --numBuckets=${NUM_BUCKETS} --schemaFile=${SCHEMA} ${DATAFLOW_ARGS}
done;

cd ${CURRENT_DIR}
