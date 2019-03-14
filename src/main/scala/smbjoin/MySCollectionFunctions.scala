package smbjoin

import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import com.twitter.algebird.{CMSHasher, _}
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
    val eps: Double = 1.0 / numBuckets // ??
    val seed: Int = 42
    val delta: Double = 0.01

    val keyAggregator = CMS.aggregator[Int](eps, delta, seed)

    val cmsSide = self
      .map { v => Math.floorMod(bucketer(v), numBuckets) }
      .aggregate(keyAggregator)
      .flatMap { cms =>
        (0 until numBuckets).map { i =>
          val freq: Double = 1.0 * cms.frequency(i).estimate / cms.totalCount
          val numShards: Int = ((freq * (numBuckets - 1)) / (1.0 - freq)).ceil.toInt
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
