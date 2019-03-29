package smbjoin

import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.io.Tap
import com.spotify.scio.values.{SCollection, SideOutput}
import com.spotify.scio.ScioMetrics
import org.apache.avro.Schema
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.extensions.sorter.BufferedExternalSorter
import org.apache.beam.sdk.transforms.{DoFn, GroupByKey, ParDo, View}
import org.apache.beam.sdk.transforms.DoFn.{Element, ProcessElement}
import org.apache.beam.sdk.util.CoderUtils
import org.apache.beam.sdk.values.KV
import smbjoin.beam.SortValuesBytes

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.hashing.MurmurHash3
object MySCollectionFunctions {

  import scala.language.implicitConversions

  implicit def toGenericAvroSCollection[T: Coder](
    c: SCollection[T]
  ): BucketedSCollection[T] =
    new BucketedSCollection(c)

}

class BucketedSCollection[T](@transient val self: SCollection[T])(
  implicit coder: Coder[T]
) extends Serializable {

  private final val BUCKET_SIZE_MB = 256

  def saveAsBucketedAvroFile[J](
    path: String,
    numBuckets: Int,
    schema: Schema,
    joinKey: T => J,
    suffix: String = AvroBucketIO.WriteParam.DefaultSuffix,
    compressionLevel: Int = AvroBucketIO.WriteParam.DefaultCompressionLevel,
    metadata: Map[String, AnyRef] = AvroBucketIO.WriteParam.DefaultMetadata
  )(implicit joder: Coder[J]): Future[Tap[Nothing]] = {

    shuffleBuckets(numBuckets, joinKey)
      .write(AvroBucketIO(path, schema, numBuckets))(
        AvroBucketIO.WriteParam(suffix, compressionLevel, metadata)
      )
  }

  private def shuffleBuckets[J](numBuckets: Int, joinKey: T => J)(
    implicit joder: Coder[J]
  ): SCollection[SMBFile] = {

    val recordBCoder = CoderMaterializer.beam(self.context, coder)
    val joinBCoder = CoderMaterializer.beam(self.context, joder)

    val groupByCoder = CoderMaterializer
      .kvCoder[Integer, KV[Array[Byte], Array[Byte]]](self.context)
    val sorted =
      self
        .map { record =>
          val sortKey =
            CoderUtils.encodeToByteArray(joinBCoder, joinKey(record))

          val bucketKey =
            Math.floorMod(MurmurHash3.bytesHash(sortKey), numBuckets)

          val encoded: Array[Byte] =
            CoderUtils.encodeToByteArray(recordBCoder, record)

          val byteOverheadPerRecord: Double = 13.0 * 8
          KV.of(new Integer(bucketKey), KV.of(sortKey, encoded))
        }
        .internal
        .setCoder(groupByCoder)
        .apply(
          GroupByKey
            .create[Integer, KV[Array[Byte], Array[Byte]]]
        )
        .apply(
          SortValuesBytes
            .create[Integer](BufferedExternalSorter.options().withMemoryMB(256))
        )

    self.context
      .wrap {
        sorted
      }
      .map { kv =>
        val bucketKey = kv.getKey
        val encodedData = kv.getValue.asScala.map(_.getValue)
        SMBFile(bucketKey, encodedData)
      }

  }

  def saveAsBucketedSkewedAvroFile[J: Coder](
    path: String,
    numBuckets: Int,
    schema: Schema,
    joinKey: T => J,
    eps: Double = 1.0,
    suffix: String = AvroBucketIO.WriteParam.DefaultSuffix,
    compressionLevel: Int = AvroBucketIO.WriteParam.DefaultCompressionLevel,
    metadata: Map[String, AnyRef] = AvroBucketIO.WriteParam.DefaultMetadata
  ): Future[Tap[Nothing]] = {

    shuffleSkewedBucketsRR(joinKey, eps)
      .write(AvroBucketIO(path, schema, numBuckets))(
        AvroBucketIO.WriteParam(suffix, compressionLevel, metadata)
      )
  }

  //scalastyle:off method.length
  private def shuffleSkewedBucketsRR[J](joinKey: T => J, eps: Double)(
    implicit joder: Coder[J]
  ): SCollection[SMBFile] = {

    val bucketIds = SideOutput[Int]()
    val byteCount = SideOutput[Double]()

    val bucketCountMetric = ScioMetrics.counter("Number of buckets")
    val shardCountMetric = ScioMetrics.counter("Number of shards")
    val shardSizeMetric = ScioMetrics.counter("Maximum records per shard")

    val recordBCoder = CoderMaterializer.beam(self.context, coder)
    val joinBCoder = CoderMaterializer.beam(self.context, joder)
    val (withBucketId, sideOutputs) = self
      .withSideOutputs(bucketIds, byteCount)
      .map {
        case (v, sideContext) =>
          val sortKey = CoderUtils.encodeToByteArray(joinBCoder, joinKey(v))
          val bucketKey = MurmurHash3.bytesHash(sortKey)
          sideContext.output(bucketIds, bucketKey)
          val encoded: Array[Byte] =
            CoderUtils.encodeToByteArray(recordBCoder, v)
          val byteOverheadPerRecord: Double = 13.0 * 8
          sideContext.output(byteCount, byteOverheadPerRecord + encoded.length)
          (bucketKey, KV.of(sortKey, encoded))
      }

    val sideNumBuckets = sideOutputs(byteCount).sum.map { totalSize =>
      val approxNumBuckets =
        Math.ceil(totalSize / (BUCKET_SIZE_MB * 1024L * 1024L))
      val roundedBuckets = scala.math
        .round(scala.math.log(approxNumBuckets) / scala.math.log(2))
        .toInt
      val numBuckets = 1 << roundedBuckets
      bucketCountMetric.inc(numBuckets)
      numBuckets
    }.asSingletonSideInput

    val shardsPerBucket = sideOutputs(bucketIds)
      .withSideInputs(sideNumBuckets)
      .map {
        case (bucketKey, sideContext) =>
          Math.floorMod(bucketKey, sideContext(sideNumBuckets))
      }
      .toSCollection
      .countByValue
      .groupBy(_ => true)
      .withName("Resolve skewness")
      .withSideInputs(sideNumBuckets)
      .flatMap {
        case ((_, it), sideContext) =>
          val numBuckets = sideContext(sideNumBuckets)
          val bucketId = it.map(_._1)
          val bucketCount = it.map(_._2).toArray
          val totalCount = bucketCount.sum
          val f = Array.fill[Int](numBuckets)(1)
          var fsum = numBuckets
          shardCountMetric.inc(numBuckets)
          var done = false
          var thresh: Long = 0
          while (!done) {
            done = true
            thresh = Math.floor((1 + eps) * totalCount / fsum).toLong
            for (i <- bucketId) {
              if (bucketCount(i).toDouble / f(i) > thresh) {
                done = false
                f(i) += 1
                fsum += 1
                shardCountMetric.inc(1)
              }
            }
          }
          shardSizeMetric.inc(thresh)
          bucketId zip f
      }
      .toSCollection
      .map { case (i, f) => KV.of(i, f) }
      .internal
      .setCoder(CoderMaterializer.kvCoder[Int, Int](self.context))
      .apply(View.asMap[Int, Int])

    val roundRobinDoFn
      : DoFn[(Int, KV[Array[Byte], Array[Byte]]),
             KV[KV[Integer, Integer], KV[Array[Byte], Array[Byte]]]] =
      new DoFn[(Int, KV[Array[Byte], Array[Byte]]), KV[
        KV[Integer, Integer],
        KV[Array[Byte], Array[Byte]]
      ]]() {

        private val localCounts: mutable.Map[Int, Int] =
          mutable.Map.empty[Int, Int]

        @ProcessElement
        private[smbjoin] def processElement(
          @Element record: (Int, KV[Array[Byte], Array[Byte]]),
          c: ProcessContext
        ): Unit = {
          val bucketKey: Int = record._1
          val nextShard: Int =
            localCounts.getOrElseUpdate(bucketKey, 0)
          val out: KV[KV[Integer, Integer], KV[Array[Byte], Array[Byte]]] =
            KV.of(
              KV.of(new Integer(bucketKey), new Integer(nextShard)),
              record._2
            )

          c.output(out)

          val sideMap = c.sideInput(shardsPerBucket)
          localCounts(bucketKey) = Math
            .floorMod(nextShard + 1, sideMap.get(bucketKey))
        }
      }

    val sorted =
      withBucketId
        .withSideInputs(sideNumBuckets)
        .map {
          case ((bucketId, record), sideContext) =>
            (Math.floorMod(bucketId, sideContext(sideNumBuckets)), record)
        }
        .toSCollection
        .internal
        .apply(
          ParDo
            .of(roundRobinDoFn)
            .withSideInputs(shardsPerBucket)
        )
        .apply(
          GroupByKey
            .create[KV[Integer, Integer], KV[Array[Byte], Array[Byte]]]
        )
        .apply(
          SortValuesBytes
            .create[KV[Integer, Integer]](
              BufferedExternalSorter.options().withMemoryMB(BUCKET_SIZE_MB)
            )
        )

    self.context
      .wrap {
        sorted
      }
      .map { kv =>
        val bucketKey = kv.getKey.getKey
        val shardId = kv.getKey.getValue
        val encodedData = kv.getValue.asScala.map(_.getValue)
        SMBFile(bucketKey, encodedData, shardId = shardId)
      }

  }
  //scalastyle:on
}
