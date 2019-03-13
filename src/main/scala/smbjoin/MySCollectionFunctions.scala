package smbjoin

import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import com.twitter.algebird.CMSHasher
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

  def saveAsAvroBucketedFileSkewed(
    path: String,
    numBuckets: Int,
    schema: Schema,
    bucketer: GenericRecord => Int
  )(implicit ord: Ordering[GenericRecord], hasher: CMSHasher[Int]): Unit = {
    import com.twitter.algebird._

    // TODO: might be better to use SparseCMS
    val eps: Double = 0.001
    val seed: Int = 42
    val delta: Double = 1E-10

    val keyAggregator = CMS.aggregator[Int](eps, delta, seed)

    val cmsSide = self
      .map { v => Math.floorMod(bucketer(v), numBuckets) }
      .aggregate(keyAggregator)
      .flatMap { cms =>
        (0 to numBuckets).map { i =>
          val numShards: Int =
            (numBuckets.toFloat * cms.frequency(i).estimate / cms.totalCount).ceil.toInt
          (i, numShards)
        }
      }
      .asMapSideInput

    self
      .withSideInputs(cmsSide)
      .map {
        case (v, sideContext) =>
          val cms = sideContext(cmsSide)
          val bucketId = Math.floorMod(bucketer(v), numBuckets)
          val shardId = scala.util.Random.nextInt(cms.getOrElse(bucketId, 1))
          ((bucketId, shardId), v)
      }
      .toSCollection
      .groupByKey
      .map {
        case ((bucketId, shardId), values) =>
          SMBucket(bucketId, values.toSeq.sorted, shardId = shardId)
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
