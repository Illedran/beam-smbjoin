#!/usr/bin/env bash
set -e
CURRENT_DIR=$(pwd)
FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Configuration
NUM_WORKERS=30

EVENTS=6e9
EVENT_KEYS=50e6
KEY_SPACE=1e9
ZIPF_SHAPES="1.10 1.20 1.30 1.40"

FANOUT_FACTOR=4

# Vars
GCS_BUCKET='gs://andrea_smb_test'
DATA_BUCKET="${GCS_BUCKET}/generated_data"

OUTPUT_DIR=$(realpath "${FILE_DIR}/../data")
SCHEMA_DIR="${GCS_BUCKET}/schemas"

#gsutil -m rm -rf ${TMP_LOCATION}
TMP_LOCATION="${GCS_BUCKET}/tmp"
STAGING_LOCATION="${TMP_LOCATION}/staging"


DATAFLOW_ARGS="--maxNumWorkers=${NUM_WORKERS} --tempLocation=${TMP_LOCATION} --stagingLocation=${STAGING_LOCATION} --project=***REMOVED*** --runner=DataflowRunner --region=europe-west1 --workerMachineType=n1-standard-4"

echo "Copying schema..."
gsutil cp $(realpath "${FILE_DIR}/../schemas/Event.avsc") "${SCHEMA_DIR}/"
gsutil cp $(realpath "${FILE_DIR}/../schemas/Key.avsc") "${SCHEMA_DIR}/"

OUTPUT_DIR_KEYS="${DATA_BUCKET}/keys"

echo "Compiling..." && cd ${FILE_DIR}/.. && sbt ";compile ;pack"
#echo "Cleaning up old data..." && gsutil -m rm -r ${DATA_BUCKET} || true
echo "Generating data..."
time=$(date +%s)

#gsutil -m rm -r ${OUTPUT_DIR_KEYS} || true
#target/pack/bin/data-generator-key --jobName="datagenerator-key-$time-$( printf "%04x%04x" $RANDOM $RANDOM )" --keySpace=${KEY_SPACE} --output=${OUTPUT_DIR_KEYS} --fanoutFactor=${FANOUT_FACTOR} ${DATAFLOW_ARGS}
for s in ${ZIPF_SHAPES}; do

  sStr=${s/./}
  time=$(date +%s)
  OUTPUT_DIR_EVENTS="${DATA_BUCKET}/events/s$s/"

  gsutil -m rm -r ${OUTPUT_DIR_EVENTS} || true
  target/pack/bin/data-generator-event --jobName="datagenerator-event-s$sStr-$time-$( printf "%04x%04x" $RANDOM $RANDOM )" --count=${EVENTS} --output=${OUTPUT_DIR_EVENTS} --zipfShape=$s --fanoutFactor=${FANOUT_FACTOR} --numKeys=${EVENT_KEYS} --keySpace=${KEY_SPACE} ${DATAFLOW_ARGS}
done;

cd ${CURRENT_DIR}
