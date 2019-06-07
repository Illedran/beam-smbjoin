package smbjoin

import com.spotify.scio.coders.{Coder, CoderMaterializer}
import org.apache.beam.sdk.coders.{Coder => BCoder}
import smbjoin.beam.SMBPartitioning

object SMBUtils {

  def getSMBPartitioning[K, T](
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
}
