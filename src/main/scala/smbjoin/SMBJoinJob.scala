package smbjoin

import java.io.File

import com.spotify.scio._
import org.apache.avro.Schema
import smbjoin.SMBScioContext._

/*
sbt "runMain example.SMBMakeBucketsExample
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/wordcount"
 */

object SMBJoinJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val left = args("inputLeft")
    val right = args("inputRight")
    val output = args("output")

    val keysSchema =
      new Schema.Parser().parse(new File("data/keys_schema.json"))
    val eventSchema =
      new Schema.Parser().parse(new File("data/events_schema.json"))

    val input = sc
      .smbReader(left, right, keysSchema, eventSchema)
      .saveAsTextFile(output)

    val result = sc.close().waitUntilFinish()
  }
}
