#!/usr/bin/env bash
set -e
CURRENT_DIR=$(pwd)
FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Configuration
NUM_WORKERS=32

ZIPF_SHAPES="1.40"

# Vars
GCS_BUCKET='gs://andrea_smb_test'
DATA_BUCKET="${GCS_BUCKET}/generated_data"
SCHEMA_DIR="${GCS_BUCKET}/schemas"

TMP_LOCATION="${GCS_BUCKET}/tmp"
STAGING_LOCATION="${TMP_LOCATION}/staging"

EVENT_SCHEMA="${SCHEMA_DIR}/Event.avsc"
KEY_SCHEMA="${SCHEMA_DIR}/Key.avsc"

DATAFLOW_ARGS="--numWorkers=${NUM_WORKERS}  --maxNumWorkers=${NUM_WORKERS} --tempLocation=${TMP_LOCATION} --stagingLocation=${STAGING_LOCATION} --project=***REMOVED*** --runner=DataflowRunner --region=europe-west1 --workerMachineType=n1-standard-4"

echo "Compiling..." && cd ${FILE_DIR}/.. && sbt ";compile ;pack"
echo "Joining data..."
time=$(date +%s)

for i in ${ZIPF_SHAPES}; do
  INPUT_KEYS_BUCKETED="${DATA_BUCKET}/bucketed_keys_skewadj/*.avro"
  INPUT_EVENTS_BUCKETED="${DATA_BUCKET}/bucketed_events_skewadj/s$i/*.avro"

  sStr=${i/./}
  time=$(date +%s)

  target/pack/bin/smb-join-job --jobName="smb-join-job-skewadj-s$sStr-$time-$( printf "%04x%04x" $RANDOM $RANDOM )" --events=${INPUT_EVENTS_BUCKETED} --keys=${INPUT_KEYS_BUCKETED} ${DATAFLOW_ARGS}
done;


cd ${CURRENT_DIR}
