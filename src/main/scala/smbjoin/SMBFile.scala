package smbjoin

import org.apache.beam.sdk.coders.{Coder => BCoder}
import com.spotify.scio.coders.{Coder, CoderMaterializer}

object SMBFile {
  def beamCoder: BCoder[SMBFile] = {
    CoderMaterializer.beamWithDefault(Coder[SMBFile])
  }
}

case class SMBFile(bucketId: Int,
                   values: Iterable[Array[Byte]],
                   shardId: Int = 0)
