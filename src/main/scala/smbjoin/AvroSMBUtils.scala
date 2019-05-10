package smbjoin

import java.nio.ByteBuffer

import com.spotify.scio.coders.{Coder, CoderMaterializer}
import org.apache.avro.Schema
import org.apache.beam.sdk.coders.{Coder => BCoder}
import org.apache.beam.sdk.util.CoderUtils
import smbjoin.beam.SMBPartitioning

object AvroSMBUtils {

  def getAvroSMBPartitioning[K, T](
    schema: Schema,
    joinKeyFn: T => K
  )(implicit koder: Coder[K], toder: Coder[T]): SMBPartitioning[K, T] = {
    val beamKoder: BCoder[K] = CoderMaterializer.beamWithDefault(koder)
    val beamToder: BCoder[T] = CoderMaterializer.beamWithDefault(toder)
    new SMBPartitioning[K, T] {
      override def getJoinKeyCoder: BCoder[K] = beamKoder
      override def getRecordCoder: BCoder[T] = beamToder
      override def getJoinKey(value: T): K = joinKeyFn(value)
    }
  }

  // Partitioning used for experiments for join keys which can be interpreted as an int
  // Does not actually hash the data so that we can generate collisions ourselves, simply decodes it
  def getAvroSMBSimplePartitioning[T](schema: Schema, joinKeyFn: T => Int)(implicit
    toder: Coder[T]
  ): SMBPartitioning[Int, T] = {
    val beamKoder = CoderMaterializer.beamWithDefault(Coder[Int])
    val beamToder: BCoder[T] = CoderMaterializer.beamWithDefault(toder)

    new SMBPartitioning[Int, T] {
      override def getJoinKeyCoder: BCoder[Int] = beamKoder
      override def getRecordCoder: BCoder[T] = beamToder
      override def getJoinKey(value: T): Int = joinKeyFn(value)

      override def hashEncodedKey(encodedJoinKey: Array[Byte]): Int =
        CoderUtils.decodeFromByteArray(beamKoder, encodedJoinKey)
    }
  }

}
