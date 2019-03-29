package smbjoin

import java.nio.channels.Channels

import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.options.PipelineOptionsFactory
import smbjoin.MySCollectionFunctions._

/* Example:
sbt "runMain example.SMBMakeBucketsExample
  --input=data/
  --output=gs://[BUCKET]/[PATH]/wordcount"
  --numBuckets=40
 */

object SMBCount {
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

    implicit val ordering: Ordering[GenericRecord] = SMBUtils.ordering
    implicit val coderGenericRecord: Coder[GenericRecord] =
      Coder.avroGenericRecordCoder(schema)

//    val inputFiles: Iterable[String] =
//      FileSystems.`match`(input).metadata.asScala.map(_.resourceId.toString)
//    sc.parallelize(inputFiles)
//      .applyTransform(gio.AvroIO.readAllGenericRecords(schema))
    sc.avroFile[GenericRecord](input, schema = schema)
      .map(SMBUtils.bucketer)
      .map({ k =>
        Math.floorMod(k, numBuckets)
      })
      .countByValue
      .saveAsTextFile(output, numShards = 1)

    sc.close().waitUntilFinish()
  }
}
