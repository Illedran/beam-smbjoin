package smbjoin

import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.beam.sdk.coders.{Coder => BCoder}
import org.apache.beam.sdk.io.FileSystems
import smbjoin.beam.{SMBAutoShuffle, SMBAvroSink, SMBPartitioning, SMBShuffle}

object BucketedSCollectionFunctions {

  import scala.language.implicitConversions

  implicit def toBucketedSCollection[T: Coder](
                                                c: SCollection[T]
                                              ): BucketedSCollection[T] =
    new BucketedSCollection(c)

}

class BucketedSCollection[T](@transient val self: SCollection[T])(
  implicit coder: Coder[T]
) extends Serializable {

  def saveAsBucketedAvroFile[J: Coder](
                                        path: String,
                                        numBuckets: Int,
                                        schema: Schema,
                                        joinKey: T => J,
                                      ): Unit = {
    val partitioning = getSMBPartitioning(joinKey)

    self
      .internal
      .apply(SMBShuffle.create(partitioning, numBuckets))
      .apply(
        SMBAvroSink.create(FileSystems.matchNewResource(path, true), schema)
      )
  }

  def saveAsBucketedAvroFileWithSkew[J: Coder](
                                                path: String,
                                                eps: Double,
                                                schema: Schema,
                                                joinKey: T => J,
                                              ): Unit = {

    val partitioning = getSMBPartitioning(joinKey)

    self
      .internal
      .apply(SMBAutoShuffle.create(partitioning, eps))
      .apply(
        SMBAvroSink.create(FileSystems.matchNewResource(path, true), schema)
      )

  }


  private def getSMBPartitioning[J](joinKey: T => J)(implicit joder: Coder[J]): SMBPartitioning[J, T] = {
   val beamJoder: BCoder[J] = CoderMaterializer.beam(self.context, joder)
    new SMBPartitioning[J, T] {
      override def getJoinKeyCoder: BCoder[J] = beamJoder

      override def getJoinKey(value: T): J = joinKey(value)
    }
  }

}
