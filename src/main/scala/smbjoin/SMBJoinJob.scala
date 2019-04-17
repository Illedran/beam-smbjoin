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
      new Schema.Parser().parse(new File("schema/Key.avsc"))
    val eventSchema =
      new Schema.Parser().parse(new File("schema/Event.avsc"))

    val input = sc
      .avroSmbFile[String, Key, Event](
        left,
        right,
        keysSchema,
        eventSchema,
        _.getId.toString,
        _.getId.toString
      )
      .saveAsTextFile(output)

    val result = sc.close().waitUntilFinish()
  }
}
