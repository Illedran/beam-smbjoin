package smbjoin

import java.nio.channels.Channels

import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.Schema
import org.apache.beam.sdk
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.options.PipelineOptionsFactory
import smbjoin.beam.{SMBPartitioning, SMBAutoShuffle, SMBAvroSink, SMBShuffle}
import smbjoin.SerializableSchema._

/* Example:
sbt "runMain smbjoin.SMBMakeBucketsJobBeam
  --input=data/events-1000000-0.avro
  --schemaFile=schema/Event.avsc
  --output=bucketed/events-100000-0
  --numBuckets=20
 */

object SMBMakeSkewedBucketsJobBeam {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val input = args("input")
    val output = args("output")
    val avroSchemaPath = args.optional("avroSchema")
    val schemaFilePath = args.optional("schemaFile")
    val skewnessEps = args("skewnessEps").toDouble

    if (avroSchemaPath.isEmpty && schemaFilePath.isEmpty) {
      sys.error("One of --avroSchema or --schemaFile is required.")
    }

    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create)
    val schema: Schema = if (schemaFilePath.isDefined) {
      val schemaResource =
        FileSystems.matchNewResource(schemaFilePath.get, false)
      new Schema.Parser()
        .parse(Channels.newInputStream(FileSystems.open(schemaResource)))
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

    val partitioning = new SMBPartitioning[String, GenericRecord] {
      override def getJoinKeyCoder: sdk.coders.Coder[String] =
        StringUtf8Coder.of()

      override def getJoinKey(value: GenericRecord): String =
        value
          .get("identData")
          .asInstanceOf[GenericRecord]
          .get("user_id")
//          .get("id")
          .toString
    }

    sc.avroFile[GenericRecord](input, schema = schema)
      .internal
      .apply(SMBAutoShuffle.create(partitioning, skewnessEps))
      .apply(
        SMBAvroSink.create(FileSystems.matchNewResource(output, true), schema)
      )

    val result = sc.close().waitUntilFinish()
  }
}
