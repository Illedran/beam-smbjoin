package smbjoin

import java.nio.channels.Channels
import java.util.concurrent.ThreadLocalRandom

import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{
  GenericDatumReader,
  GenericRecord,
  GenericRecordBuilder
}
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.options.PipelineOptionsFactory

/* Example:
sbt "runMain smbjoin.SMBMakeBucketsJobBeam
  --input=data/events-1000000-0.avro
  --schemaFile=schema/Event.avsc
  --output=bucketed/events-100000-0
  --numBuckets=20
 */

object DataGenerator {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val output = args("output")
    val count = args("count").toLong
    val numBuckets = args.getOrElse("numBuckets", "1").toInt
    val avroSchemaPath = args.optional("avroSchema")
    val schemaFilePath = args.optional("schemaFile")
    val zipfShape = args.getOrElse("zipfShape", "0.00").toDouble
    val dataSkew = args.getOrElse("dataSkew", "0.00").toDouble
    val numKeys = args.getOrElse("keySize", "1e6").toDouble.toLong

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

    // Compute harmonic series for Zipf distribution
    val H = for (b <- 1 to numBuckets) yield {
      1.0 / Math.pow(b, zipfShape)
    }
    val Hsum = H.sum

    val bucketCounts: Array[Int] = (for (b <- 0 until numBuckets) yield {
      (count * H(b) / Hsum).toInt
    }).toArray
    for (i <- 0 until (count - bucketCounts.sum).toInt) {
      bucketCounts(i) += 1
    }

    // Fix the random seed for the data-skewed records in each bucket
    val skewedSeeds = Stream
      .continually(ThreadLocalRandom.current.nextLong(numKeys))
      .take(numBuckets)
      .toVector

    def getKey(bucketIdx: Int, seed: Long): Int =
      Math.floorMod(bucketIdx + seed * numBuckets, numKeys).toInt

    val serializableSchema = new SerializableSchema(schema)

    implicit val coderGenericRecord: Coder[GenericRecord] =
      Coder.avroGenericRecordCoder(schema)

    sc.parallelize(0 until numBuckets)
      .flatMap { i =>
        Stream.continually(i).take(bucketCounts(i))
      }
      .map { i =>
        if (ThreadLocalRandom.current.nextDouble() < dataSkew) {
          getKey(i, skewedSeeds(i))
        } else {
          getKey(i, ThreadLocalRandom.current.nextLong(numKeys))
        }
      }
      .map { k =>
        new GenericRecordBuilder(serializableSchema.schema)
          .set("id", f"$k%08x")
          .build
          .asInstanceOf[GenericRecord]
      }
      .saveAsAvroFile(output, schema = schema)

    val result = sc.close()
  }
}
