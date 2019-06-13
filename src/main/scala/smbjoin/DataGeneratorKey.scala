package smbjoin

import java.util.UUID.randomUUID

import com.spotify.scio._
import com.spotify.scio.avro._
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.options.PipelineOptionsFactory

/* Example:
sbt "runMain smbjoin.DataGeneratorKey
  --numKeys=1000
 */
object DataGeneratorKey {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val output = args("output")
    val keySpace = args.getOrElse("keySpace", "100000").toDouble.toInt.toDouble

    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create)

    val blockSize = 5000
    val blocks: Int = math.ceil(keySpace / blockSize).toInt
    sc.parallelize(0 until blocks)
      .flatMap { i =>
        for (j <- 0 until blockSize) yield {
          j + i * blockSize
        }
      }
      .map { key =>
        Key
          .newBuilder()
          .setId(key)
          .setKey(randomUUID().toString)
          .build()
      }
      .saveAsAvroFile(output)

    val result = sc.close()
  }
}
