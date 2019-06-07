package smbjoin

import java.nio.ByteBuffer
import java.util.concurrent.ThreadLocalRandom

import com.spotify.scio._
import com.spotify.scio.avro._
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.Reshuffle

/* Example:
sbt "runMain smbjoin.SMBMakeBucketsJobBeam
  --input=data/events-1000000-0.avro
  --schemaFile=schema/Event.avsc
  --output=bucketed/events-100000-0
  --numBuckets=20
 */

object DataGeneratorEvent {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val output = args("output")
    val count = args("count").toDouble.toLong
    val zipfShape = args.getOrElse("zipfShape", "0.00").toDouble
    val keySpace = args("keySpace").toDouble.toInt
    val numKeys = args("numKeys").toDouble.toInt
    val fanoutFactor = args.getOrElse("fanoutFactor", "1").toInt
    val payloadSize = args.getOrElse("payloadSize", "96").toInt

    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create)

    val keyGap = keySpace / numKeys
    sc.parallelize(List(0))
      .flatMap { _ =>
        val r = 0 until numKeys

        var Hsum: Double = 0.0
        for (i <- 0 until numKeys) {
          Hsum += 1.0 / Math.pow(i + 1, zipfShape)
        }

        for (i <- r) yield {
          val key = ThreadLocalRandom.current().nextInt(keyGap) + i * keyGap
          val freq = count.toDouble / Math.pow(i + 1, zipfShape) / Hsum
          (key, freq)
        }
      }
      .flatMap {
        case (key, freq) =>
          val adjFreq = (freq / fanoutFactor).round
          for (_ <- 0 until fanoutFactor) yield (key, adjFreq)
      }
      .applyTransform(Reshuffle.viaRandomKey())
      .flatMap {
        case (key, freq) =>
          for (_ <- 0L until freq) yield key
      }
      .map { key =>
        val payload: Array[Byte] = new Array[Byte](payloadSize)
        ThreadLocalRandom.current().nextBytes(payload)
        Event
          .newBuilder()
          .setId(key)
          .setPayload(ByteBuffer.wrap(payload))
          .build()
      }
      .saveAsAvroFile(output)

    val result = sc.close()
  }
}
