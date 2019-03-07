package smbjoin

import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.concurrent.Future

object MySCollectionFunctions {

  import scala.language.implicitConversions

  implicit def toGenericAvroSCollection(
    c: SCollection[GenericRecord]
  ): GenericAvroSCollection =
    new GenericAvroSCollection(c)

}

class GenericAvroSCollection(@transient val self: SCollection[GenericRecord])
    extends Serializable {

  def saveAsAvroBucketedFile(
    path: String,
    numBuckets: Int,
    schema: Schema,
    bucketer: GenericRecord => Int
  )(implicit ord: Ordering[GenericRecord]): Future[Tap[Nothing]] = {
    self
      .map { v =>
        (Math.floorMod(bucketer(v), numBuckets), v)
      }
      .groupByKey
      .map {
        case (bucketId, values) =>
          SMBucket(bucketId, values.toSeq.sorted)
      }
      .write(
        SMBAvroOutputGenericRecord(
          SerializableSchema.of(schema),
          numBuckets,
          path
        )
      )
  }

}
