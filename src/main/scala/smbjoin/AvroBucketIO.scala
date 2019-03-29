package smbjoin

import java.nio.channels.Channels
import java.nio.ByteBuffer

import com.google.common.base.Supplier
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.{EmptyTap, EmptyTapOf, ScioIO, Tap}
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.beam.sdk.io.{
  DefaultFilenamePolicy,
  FileBasedSink,
  FileSystems
}
import org.apache.beam.sdk.util.MimeTypes

import scala.concurrent.Future

case class AvroBucketIO(path: String,
                        serializableSchema: SerializableSchema,
                        numBuckets: Int,
) extends ScioIO[SMBFile] {
  override type ReadP = Nothing
  override type WriteP = AvroBucketIO.WriteParam
  val tapT = EmptyTapOf[SMBFile]

  val shardTemplate = "bucket-SSSS-of-NNNN"

  override def tap(params: Nothing): Tap[Nothing] = EmptyTap

  override protected def read(sc: ScioContext,
                              params: ReadP): SCollection[SMBFile] =
    throw new IllegalStateException("BucketIO is write-only")

  override protected def write(data: SCollection[SMBFile],
                               params: WriteP): Future[Tap[Nothing]] = {

    data.map { bucket =>
      val shardSuffix = f"-s${bucket.shardId}%04d${params.suffix}%s"
      val resourceId = DefaultFilenamePolicy
        .fromParams(
          new DefaultFilenamePolicy.Params()
            .withBaseFilename(FileSystems.matchNewResource(path, true))
            .withShardTemplate(shardTemplate)
            .withSuffix(shardSuffix)
        )
        .unwindowedFilename(
          bucket.bucketId,
          numBuckets,
          FileBasedSink.CompressionType.UNCOMPRESSED
        )

      println(s"Writing $resourceId")
      val channel = FileSystems.create(resourceId, MimeTypes.BINARY)
      val dataFileWriter =
        new DataFileWriter[GenericRecord](new GenericDatumWriter())
          .setCodec(CodecFactory.deflateCodec(params.compressionLevel))
          .setMeta("smbjoin.bucketId", bucket.bucketId)
          .setMeta("smbjoin.shardId", bucket.shardId)
          .create(serializableSchema.schema, Channels.newOutputStream(channel))

      // TODO: add metadata from params.metadata

      bucket.values.foreach { data =>
        dataFileWriter.appendEncoded(ByteBuffer.wrap(data))
      }

      dataFileWriter.close()
      if (channel.isOpen) {
        channel.close()
      }
    }
    Future.successful(EmptyTap)
  }
}
object AvroBucketIO {
  case class WriteParam(
    private val _suffix: String = WriteParam.DefaultSuffix,
    compressionLevel: Int = WriteParam.DefaultCompressionLevel,
    metadata: Map[String, AnyRef] = WriteParam.DefaultMetadata
  ) { val suffix: String = _suffix + ".avro" }

  object WriteParam {
    val DefaultSuffix = ""
    val DefaultCompressionLevel: Int = 6
    val DefaultMetadata: Map[String, AnyRef] = Map.empty
  }
}
