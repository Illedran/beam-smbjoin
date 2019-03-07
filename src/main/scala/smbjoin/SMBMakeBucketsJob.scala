package smbjoin

import java.io.File

import com.spotify.scio._
import com.spotify.scio.avro._
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import smbjoin.MySCollectionFunctions._

/* Example:
sbt "runMain example.SMBMakeBucketsExample
  --input=data/
  --output=gs://[BUCKET]/[PATH]/wordcount"
  --numBuckets=40
 */

object SMBMakeBucketsJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val input = args("input")
    val output = args("output")
    val numBuckets = args("numBuckets").toInt

    val datumReader = new GenericDatumReader[GenericRecord]
    val dataFileReader = new DataFileReader(new File(input), datumReader)
    val schema = dataFileReader.getSchema

    implicit val ordering: Ordering[GenericRecord] = SMBUtils.ordering

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
