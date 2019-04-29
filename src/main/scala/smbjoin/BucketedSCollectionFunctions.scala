package smbjoin

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.beam.sdk.io.FileSystems
import smbjoin.beam.{SMBAutoShuffle, SMBAvroSink, SMBShuffle, SMBSizeShuffle}

object BucketedSCollectionFunctions {

  import scala.language.implicitConversions

  implicit def toBucketedSCollection[T: Coder](
    c: SCollection[T]
  ): BucketedSCollection[T] =
    new BucketedSCollection(c)

}

class BucketedSCollection[T](@transient val self: SCollection[T])(
  implicit coder: Coder[T]
) {

  def saveAsBucketedAvroFile[K: Coder](path: String,
                                       numBuckets: Int,
                                       schema: Schema,
                                       joinKeyFn: T => K,
  ): Unit = {
    val partitioning = AvroSMBUtils.getAvroSMBPartitioning(schema, joinKeyFn)

    self.internal
      .apply(SMBShuffle.create(partitioning, numBuckets))
      .apply(
        SMBAvroSink.create(FileSystems.matchNewResource(path, true), schema)
      )
  }

  def saveAsBucketedAvroFileAuto[K: Coder](path: String,
                                               eps: Double,
                                               schema: Schema,
                                               joinKeyFn: T => K,
  ): Unit = {

    val partitioning = AvroSMBUtils.getAvroSMBPartitioning(schema, joinKeyFn)

    self.internal
      .apply(SMBAutoShuffle.create(partitioning, eps))
      .apply(
        SMBAvroSink.create(FileSystems.matchNewResource(path, true), schema)
      )

  }

  def saveAsBucketedAvroFileSize[K: Coder](path: String,
                                           schema: Schema,
                                           joinKeyFn: T => K,
                                           bucketSizeMB: Int = 256,
                                          ): Unit = {

    val partitioning = AvroSMBUtils.getAvroSMBPartitioning(schema, joinKeyFn)

    self.internal
      .apply(SMBSizeShuffle.builder().bucketSizeMB(bucketSizeMB)
        .smbPartitioning(partitioning).build())
      .apply(
        SMBAvroSink.create(FileSystems.matchNewResource(path, true), schema)
      )

  }

}
