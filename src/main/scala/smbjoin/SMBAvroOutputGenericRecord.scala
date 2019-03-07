package smbjoin

import java.nio.channels.Channels

import com.google.common.base.Supplier
import com.spotify.scio.ScioContext
import com.spotify.scio.io.{EmptyTap, EmptyTapOf, ScioIO, Tap}
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.beam.sdk.io.{DefaultFilenamePolicy, FileBasedSink, FileSystems}
import org.apache.beam.sdk.util.MimeTypes

import scala.concurrent.Future

case class SMBAvroOutputGenericRecord(schema: Supplier[Schema],
                                      numBuckets: Int,
                                      outputPath: String)
    extends ScioIO[SMBucket[GenericRecord]] {
  override type ReadP = Nothing
  override type WriteP = Unit
  val tapT = EmptyTapOf[SMBucket[GenericRecord]]

  val compressionLevel: Int = 6
  val shardTemplate = "bucket-SSSSS-of-NNNNN"
  val suffix = ".avro"

  override def tap(params: Nothing): Nothing = ???

  override protected def read(
    sc: ScioContext,
    params: Nothing
  ): SCollection[SMBucket[GenericRecord]] = ???

  override protected def write(data: SCollection[SMBucket[GenericRecord]],
                               params: Unit): Future[Tap[tapT.T]] = {

    //    val sink =
    //      SMBAvroSink.of[GenericRecord](
    //        output,
    //        GenericDatumWriterSupplier,
    //        SerializableSchema.of(schema))
    data.map { bucket =>
      val resourceId = DefaultFilenamePolicy
        .fromParams(
          new DefaultFilenamePolicy.Params()
            .withBaseFilename(FileSystems.matchNewResource(outputPath, true))
            .withShardTemplate(shardTemplate)
            .withSuffix(suffix)
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
          .setCodec(CodecFactory.deflateCodec(compressionLevel))
          .create(schema.get, Channels.newOutputStream(channel))

      bucket.values.foreach(dataFileWriter.append)

      dataFileWriter.close()
      if (channel.isOpen) {
        channel.close()
      }
    }
    Future.successful(EmptyTap)
  }
}
