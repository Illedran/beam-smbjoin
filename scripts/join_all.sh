#!/usr/bin/env bash
set -e
EXECDIR=$( pwd )
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"


GCS_BUCKET='gs://andrea_smb_test'
DATA_BUCKET="${GCS_BUCKET}/generated_data"
SCHEMA="${GCS_BUCKET}/schemas/Record.avsc"
NUM_BUCKETS=256
NUM_WORKERS=32
jobs="s0.00 s0.20 s0.40 s0.60 s0.80 s1.00"

TMP_LOCATION="${GCS_BUCKET}/tmp"
STAGING_LOCATION="${TMP_LOCATION}/staging"

#gsutil -m rm -rf ${TMP_LOCATION}

DATAFLOW_ARGS="--numWorkers=${NUM_WORKERS} --tempLocation=${TMP_LOCATION} --stagingLocation=${STAGING_LOCATION} --diskSizeGb=100 --autoscalingAlgorithm=NONE --project=***REMOVED*** --runner=DataflowRunner --region=europe-west1 --workerMachineType=n2-standard-2"

echo "Compiling..." && cd ${DIR}/.. && sbt ";compile ;pack"
echo "Starting jobs..." &&
for i in $jobs; do
  INPUT_LEFT="${DATA_BUCKET}/$i/bucketed_lhs/*.avro"
  INPUT_RIGHT="${DATA_BUCKET}/$i/bucketed_rhs/*.avro"
  INPUT_LEFT_SKEWADJ="${DATA_BUCKET}/$i/bucketed_lhs_skewadj/*.avro"
  INPUT_RIGHT_SKEWADJ="${DATA_BUCKET}/$i/bucketed_rhs_skewadj/*.avro"

  sStr=${i/./}
  time=$(date +%s)

  target/pack/bin/join-job --jobName="joinjob-$sStr-$time-$( printf "%04x%04x" $RANDOM $RANDOM )" --inputLeft=${INPUT_LEFT} --inputRight=${INPUT_RIGHT} --schemaFile=${SCHEMA} ${DATAFLOW_ARGS}
  target/pack/bin/smb-join-job --jobName="smbjoin-$sStr-$time-$( printf "%04x%04x" $RANDOM $RANDOM )" --inputLeft=${INPUT_LEFT_SKEWADJ} --inputRight=${INPUT_RIGHT_SKEWADJ} --schemaFile=${SCHEMA} ${DATAFLOW_ARGS}
done;


cd ${EXECDIR}
