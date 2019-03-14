package smbjoin

import java.nio.channels.Channels

import com.google.common.base.Preconditions.checkNotNull
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.transforms.Reshuffle

import scala.collection.JavaConverters._

object SMBScioContext {

  import scala.language.implicitConversions

  implicit def toSMBScioContext(c: ScioContext): SMBScioContext =
    SMBScioContext(c)

}

case class SMBScioContext(@transient self: ScioContext) {

  // scalastyle:off
  def smbReader(
                 leftSpec: String,
                 rightSpec: String,
                 leftSchema: Schema,
                 rightSchema: Schema
               ): SCollection[(String, Iterable[GenericRecord], Iterable[GenericRecord])] = {

    val left = FileSystems
      .`match`(leftSpec)
      .metadata
      .asScala
      .map(_.resourceId)

    val right = FileSystems
      .`match`(rightSpec)
      .metadata
      .asScala
      .map(_.resourceId)

    val leftSchemaSupplier = SerializableSchema.of(leftSchema)
    val rightSchemaSupplier = SerializableSchema.of(rightSchema)

    val resolvedBucketSpec =
      resolveBucketSpec(left, right, leftSchema, rightSchema)
    self
      .parallelize(resolvedBucketSpec)
      .applyTransform(Reshuffle.viaRandomKey()) //TODO: is this needed?
      .flatMap {
      case (leftFile, rightFile) =>
        checkNotNull(leftFile)
        checkNotNull(rightFile)
        val leftChannel = FileSystems.open(leftFile)

        val leftInputStream = Channels.newInputStream(leftChannel)
        val leftStream = new DataFileStream(
          leftInputStream,
          new GenericDatumReader[GenericRecord](leftSchemaSupplier.get)
        )

        val rightChannel = FileSystems.open(rightFile)
        val rightInputStream = Channels.newInputStream(rightChannel)
        val rightStream = new DataFileStream(
          rightInputStream,
          new GenericDatumReader[GenericRecord](rightSchemaSupplier.get)
        )

        SMBUtils.smbJoin(
          leftStream.iterator.asScala.buffered,
          rightStream.iterator.asScala.buffered
        )
    }
  }

  def resolveBucketSpec(
                         left: Iterable[ResourceId],
                         right: Iterable[ResourceId],
                         leftSchema: Schema,
                         rightSchema: Schema
                       ): Iterable[(ResourceId, ResourceId)] = {
    println("Resolving bucket spec")
    val leftWithBucketId = left
      .map { fileName =>
        val (bucketId, shardId) = getBucketingMetadata(fileName, leftSchema)
        (bucketId, fileName)
      }

    val rightWithBucketId = right
      .map { fileName =>
        val (bucketId, shardId) = getBucketingMetadata(fileName, rightSchema)
        (bucketId, fileName)
      }

    val leftBucketIds = leftWithBucketId.map {
      _._1
    }.toSeq.sorted
    val rightBucketIds = rightWithBucketId.map {
      _._1
    }.toSeq.sorted


    if (leftBucketIds.last != rightBucketIds.last ||
      (0L to leftBucketIds.last) != leftBucketIds.distinct ||
      (0L to rightBucketIds.last) != rightBucketIds.distinct) {
      throw new RuntimeException(
        "Left/right should have same number of buckets"
      )
    } else {
      println(s"Found ${leftBucketIds.last + 1} buckets across (${leftBucketIds.size}, ${rightBucketIds.size}) shards")
    }

    for (leftIt <- leftWithBucketId;
         rightIt <- rightWithBucketId
         if leftIt._1 == rightIt._1) yield {
      (leftIt._2, rightIt._2)
    }
  }

  def getBucketingMetadata(resourceId: ResourceId,
                           schema: Schema): (Long, Long) = {
    checkNotNull(resourceId)
    val channel = FileSystems.open(resourceId)

    val inputStream = Channels.newInputStream(channel)
    val stream = new DataFileStream(
      inputStream,
      new GenericDatumReader[GenericRecord](schema)
    )
    val bucketId = stream.getMetaLong("smbjoin.bucketId")
    val shardId = stream.getMetaLong("smbjoin.shardId")
    stream.close()
    inputStream.close()
    channel.close()
    (bucketId, shardId)
  }

  //scalastyle:on
}
