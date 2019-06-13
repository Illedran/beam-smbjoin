package smbjoin

import java.nio.channels.Channels

import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.beam.sdk.coders.{BigEndianIntegerCoder, Coder => BCoder}
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.options.PipelineOptionsFactory

object SMBMakeBucketsJob {

  import smbjoin.beam.{SMBAvroSink, SMBPartitioning, SMBShuffle}

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
    implicit val coderInt: Coder[Int] =
      Coder.beam(BigEndianIntegerCoder.of.asInstanceOf[BCoder[Int]])
    // This coder maintains ordering of ints

    def joinKey(input: GenericRecord): Int = {
      input.get("id").asInstanceOf[Int]
    }

    val partitioning: SMBPartitioning[Int, GenericRecord] =
      SMBUtils.getSMBPartitioning(joinKey)(coderInt, coderGenericRecord)

    sc.avroFile[GenericRecord](input, schema = schema)
      .internal
      .apply(SMBShuffle.create(partitioning, numBuckets))
      .apply(
        SMBAvroSink.create(FileSystems.matchNewResource(output, true), schema)
      )

    val result = sc.close()
  }
}
