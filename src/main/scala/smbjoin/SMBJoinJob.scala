package smbjoin

import java.nio.channels.Channels

import com.spotify.scio._
import com.spotify.scio.coders.Coder
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.options.PipelineOptionsFactory

/*
sbt "runMain example.SMBMakeBucketsExample
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/wordcount"
 */

object SMBJoinJob {

  def main(cmdlineArgs: Array[String]): Unit = {
    import smbjoin.beam.{SMBAvroInput, SMBJoinType}
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val left = args("inputLeft")
    val right = args("inputRight")
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

    def joinKey(input: GenericRecord): Int = {
      Integer.parseInt(input.get("id").toString, 16)
    }

    val smbData = sc.customInput(
      "SMBRead",
      SMBAvroInput.create(
        left,
        right,
        schema,
        schema,
        AvroSMBUtils.getAvroSMBPartitioning(schema, joinKey),
        AvroSMBUtils.getAvroSMBPartitioning(schema, joinKey)
      )
    )

    smbData
      .internal
      .apply(SMBJoinType.innerJoin[Int, GenericRecord, GenericRecord])


    val result = sc.close()
  }
}
