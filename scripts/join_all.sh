#!/usr/bin/env bash

EXECDIR=$( pwd )
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"


GCS_BUCKET='gs://andrea_smb_test'
DATA_BUCKET="${GCS_BUCKET}/generated_data"
SCHEMA="${GCS_BUCKET}/schemas/Record.avsc"
NUM_BUCKETS=32
NUM_WORKERS=8
BUCKET_SIZE_MB=338

TMP_LOCATION="${GCS_BUCKET}/tmp"
STAGING_LOCATION="${TMP_LOCATION}/staging"

gsutil -m rm -rf ${TMP_LOCATION}

DATAFLOW_ARGS="--numWorkers=${NUM_WORKERS} --tempLocation=${TMP_LOCATION} --stagingLocation=${STAGING_LOCATION} --diskSizeGb=50 --autoscalingAlgorithm=NONE --project=***REMOVED*** --runner=DataflowRunner --region=europe-west1 --workerMachineType=n1-standard-1"

echo "Compiling..."
cd ${DIR}/.. && sbt ";clean ;reload ;compile ;pack" &&
echo "Starting jobs..." &&
for i in $(ls ${DIR}/../data); do
  INPUT_LEFT="${DATA_BUCKET}/$i/lhs/*.avro"
  INPUT_RIGHT="${DATA_BUCKET}/$i/rhs/*.avro"
  OUTPUT_DIR_LEFT="${DATA_BUCKET}/$i/bucketed_lhs"
  OUTPUT_DIR_RIGHT="${DATA_BUCKET}/$i/bucketed_rhs"
  OUTPUT_DIR_LEFT_SKEWADJ="${OUTPUT_DIR_LEFT}_skewadj"
  OUTPUT_DIR_RIGHT_SKEWADJ="${OUTPUT_DIR_RIGHT}_skewadj"

  sStr=${i/./}
  time=$(date +%s)

  gsutil -m rm -rf ${OUTPUT_DIR_LEFT}
  gsutil -m rm -rf ${OUTPUT_DIR_RIGHT}
  target/pack/bin/smb-make-buckets-job --jobName="smbmakebuckets-$sStr-lhs-$time-$( printf "%04x%04x" $RANDOM $RANDOM )" --input=${INPUT_LEFT} --output=${OUTPUT_DIR_LEFT} --numBuckets=${NUM_BUCKETS} --schemaFile=${SCHEMA} ${DATAFLOW_ARGS}
  target/pack/bin/smb-make-buckets-job --jobName="smbmakebuckets-$sStr-rhs-$time-$( printf "%04x%04x" $RANDOM $RANDOM )" --input=${INPUT_RIGHT} --output=${OUTPUT_DIR_RIGHT} --numBuckets=${NUM_BUCKETS} --schemaFile=${SCHEMA} ${DATAFLOW_ARGS}

  gsutil -m rm -rf ${OUTPUT_DIR_LEFT_SKEWADJ}
  gsutil -m rm -rf ${OUTPUT_DIR_RIGHT_SKEWADJ}
  target/pack/bin/smb-make-buckets-skew-adj-job --jobName="smbmakebuckets-skewadj-$sStr-lhs-$time-$( printf "%04x%04x" $RANDOM $RANDOM )" --input=${INPUT_LEFT} --output=${OUTPUT_DIR_LEFT_SKEWADJ} --bucketSizeMB=${BUCKET_SIZE_MB} --schemaFile=${SCHEMA} ${DATAFLOW_ARGS}
  target/pack/bin/smb-make-buckets-skew-adj-job --jobName="smbmakebuckets-skewadj-$sStr-rhs-$time-$( printf "%04x%04x" $RANDOM $RANDOM )" --input=${INPUT_RIGHT} --output=${OUTPUT_DIR_RIGHT_SKEWADJ} --bucketSizeMB=${BUCKET_SIZE_MB} --schemaFile=${SCHEMA} ${DATAFLOW_ARGS}
  break
done;


cd ${EXECDIR}
