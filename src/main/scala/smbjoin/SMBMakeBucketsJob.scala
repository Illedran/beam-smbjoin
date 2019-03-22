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
import org.apache.avro.Schema

/* Example:
sbt "runMain smbjoin.SMBMakeBucketsJobBeam
  --input=data/events-1000000-0.avro
  --schemaFile=schema/Event.avsc
  --output=bucketed/events-100000-0
  --numBuckets=20
 */

object SMBMakeBucketsJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val input = args("input")
    val output = args("output")
    val numBuckets = args("numBuckets").toInt
    val avroSchemaPath = args.optional("avroSchema")
    val schemaFilePath = args.optional("schemaFile")

    if (avroSchemaPath.isEmpty && schemaFilePath.isEmpty) {
      sys.error("One of --avroSchema or --schemaFile is required.")
    }

    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create)
    val schema: Schema = if (schemaFilePath.isDefined) {
      import org.apache.avro.Schema
      val schemaResource =
        FileSystems.matchNewResource(schemaFilePath.get, false)
      new Schema.Parser().parse(Channels.newInputStream(FileSystems.open(schemaResource)))
    } else {
      val schemaResource =
        FileSystems.matchNewResource(avroSchemaPath.get, false)

      val dataFileReader = new DataFileStream(
        Channels.newInputStream(FileSystems.open(schemaResource)),
        new GenericDatumReader[GenericRecord]
      )
      dataFileReader.getSchema
    }

    implicit val ordering: Ordering[GenericRecord] = SMBUtils.ordering
    implicit val coderGenericRecord: Coder[GenericRecord] =
      Coder.avroGenericRecordCoder(schema)

    sc.avroFile[GenericRecord](input, schema = schema)
      .saveAsAvroBucketedFile(
        output,
        numBuckets,
        schema,
        bucketer = SMBUtils.bucketer
      )

    val result = sc.close().waitUntilFinish()
  }
}
