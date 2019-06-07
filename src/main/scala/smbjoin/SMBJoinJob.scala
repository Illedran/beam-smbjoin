package smbjoin

import com.spotify.scio._
import com.spotify.scio.coders.Coder
import org.apache.beam.sdk.coders.{BigEndianIntegerCoder, Coder => BCoder}
import smbjoin.beam.{SMBAvroInput, SMBJoinType}

/*
sbt "runMain example.SMBMakeBucketsExample
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/wordcount"
 */

object SMBJoinJob {

  def main(cmdlineArgs: Array[String]): Unit = {
    import org.apache.beam.sdk.io.FileSystems
    import org.apache.beam.sdk.options.PipelineOptionsFactory
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val eventsInput = args("events")
    val keysInput = args("keys")

    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create)

    implicit val coderInt: Coder[Int] =
      Coder.beam(BigEndianIntegerCoder.of.asInstanceOf[BCoder[Int]])
    // This coder maintains ordering of ints

    val smbData = sc.customInput(
      "SMBRead",
      SMBAvroInput.create(
        eventsInput,
        keysInput,
        Event.getClassSchema,
        Key.getClassSchema,
        SMBUtils.getSMBPartitioning[Int, Event](_.getId),
        SMBUtils.getSMBPartitioning[Int, Key](_.getId),
      )
    )

    smbData.internal
      .apply(SMBJoinType.innerJoin[Int, Event, Key])

    val result = sc.close()
  }
}
