package smbjoin

import java.nio.channels.Channels

import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.options.PipelineOptionsFactory
import smbjoin.MySCollectionFunctions._

/* Example:
sbt "runMain smbjoin.SMBMakeBucketsJobBeam
  --input=data/events-1000000-0.avro
  --avroSchema=schema/empty_event.avro
  --output=bucketed/events-100000-0
  --numBuckets=20
 */

object SMBMakeBucketsJobBeam {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val input = args("input")
    val output = args("output")
    val numBuckets = args("numBuckets").toInt
    val schemaPath = args("avroSchema")

    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create)
    val schemaResource = FileSystems.matchNewResource(schemaPath, false)

    val dataFileReader = new DataFileStream(
      Channels.newInputStream(FileSystems.open(schemaResource)),
      new GenericDatumReader[GenericRecord]
    )
    val schema = dataFileReader.getSchema

    implicit val ordering: Ordering[GenericRecord] = SMBUtils.ordering
    implicit val coderGenericRecord: Coder[GenericRecord] =
      Coder.avroGenericRecordCoder(schema)

    sc.avroFile[GenericRecord](input, schema = schema)
      .saveAsAvroBucketedFileBeam(
        output,
        numBuckets,
        schema,
        bucketer = SMBUtils.bucketer
      )

    val result = sc.close().waitUntilFinish()
  }
}
