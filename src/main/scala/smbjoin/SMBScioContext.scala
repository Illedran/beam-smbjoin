package smbjoin

import java.nio.channels.Channels

import com.google.common.base.Preconditions.checkNotNull
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.transforms.Reshuffle

import scala.collection.JavaConverters._

object SMBScioContext {

  import scala.language.implicitConversions

  implicit def toSMBScioContext(c: ScioContext): SMBScioContext =
    SMBScioContext(c)

}

case class SMBScioContext(@transient self: ScioContext) {

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
      .sortBy(_.getFilename)
    val right = FileSystems
      .`match`(rightSpec)
      .metadata
      .asScala
      .map(_.resourceId)
      .sortBy(_.getFilename)

    if (left.size != right.size) {
      throw new RuntimeException(
        "Left/right should have same number of buckets"
      )
    }

    val leftSchemaSupplier = SerializableSchema.of(leftSchema)
    val rightSchemaSupplier = SerializableSchema.of(rightSchema)

    self
      .parallelize(left zip right)
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
}
