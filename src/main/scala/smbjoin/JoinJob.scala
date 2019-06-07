package smbjoin

import com.spotify.scio._
import com.spotify.scio.avro._

import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.options.PipelineOptionsFactory

/*
sbt "runMain example.SMBMakeBucketsExample
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/wordcount"
 */

object JoinJob {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val eventsInput = args("events")
    val keysInput = args("keys")

    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create)

    val events = sc
      .avroFile[Event](eventsInput)
      .withName("Extract key left")
      .map { r =>
        (r.getId, r)
      }

    val keys = sc
      .avroFile[Key](keysInput)
      .withName("Extract key right")
      .map { r =>
        (r.getId, r)
      }

    val joined = events.join(keys)

    val result = sc.close()
  }
}
