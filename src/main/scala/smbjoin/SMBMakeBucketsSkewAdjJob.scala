package smbjoin

import java.nio.channels.Channels

import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.options.PipelineOptionsFactory
import smbjoin.BucketedSCollectionFunctions._

/* Example:
sbt "runMain smbjoin.SMBMakeBucketsJobBeam
  --input=data/events-1000000-0.avro
  --schemaFile=schema/Event.avsc
  --output=bucketed/events-100000-0
  --numBuckets=20
 */

object SMBMakeBucketsSkewAdjJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    import smbjoin.beam.{SMBAvroSink, SMBPartitioning, SMBSizeShuffle}
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val input = args("input")
    val output = args("output")
    val avroSchemaPath = args.optional("avroSchema")
    val schemaFilePath = args.optional("schemaFile")
    val bucketSizeMB=args.getOrElse("bucketSizeMB","256").toInt
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

    implicit val coderGenericRecord: Coder[GenericRecord] =
      Coder.avroGenericRecordCoder(schema)

    def joinKey(input: GenericRecord): Int = {
      Integer.parseInt(input.get("id").toString, 16)
    }

    val partitioning: SMBPartitioning[Int, GenericRecord] =
      AvroSMBUtils.getAvroSMBSimplePartitioning(schema, joinKey)

    sc.avroFile[GenericRecord](input, schema = schema)
      .internal
      .apply(
        SMBSizeShuffle
          .builder()
          .bucketSizeMB(bucketSizeMB)
          .smbPartitioning(partitioning)
          .build()
      )
      .apply(
        SMBAvroSink.create(FileSystems.matchNewResource(output, true), schema)
      )
    val result = sc.close()
  }
}
