package smbjoin

import java.nio.channels.Channels

import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.Schema
import org.apache.beam.sdk.coders.{Coder => BCoder}
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.GroupByKey
import org.apache.beam.sdk.values.KV
import scala.collection.JavaConverters._

/* Example:
sbt "runMain smbjoin.GroupByTestBeam
  --input=data/events-1000000-0.avro
  --schemaFile=schema/Event.avsc
  --output=bucketed/events-100000-0
 */

object GroupByTestBeam {
  def main(cmdlineArgs: Array[String]): Unit = {
    import com.spotify.scio.coders.CoderMaterializer
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val input = args("input")
    val output = args("output")

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

    val kvCoder: BCoder[KV[Char, Double]] =
      CoderMaterializer.kvCoder[Char, Double](sc)
    sc.wrap {
        sc.avroFile[GenericRecord](input, schema = schema)
          .map { rec =>
            KV.of(rec.get("id").toString.head, rec.get("value").asInstanceOf[Double])
          }
          .internal
          .setCoder(kvCoder)
          .apply(GroupByKey.create[Char, Double])
      }
      .map { kv =>
        (kv.getKey, kv.getValue.asScala)
      }
//      .saveAsAvroFile(output, schema = schema)

    val result = sc.close().waitUntilFinish()
  }
}
