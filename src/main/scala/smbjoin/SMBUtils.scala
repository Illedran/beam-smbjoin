package smbjoin

import org.apache.avro.generic.GenericRecord

import scala.collection.mutable
import scala.util.hashing.MurmurHash3

object SMBUtils {
  val ordering: Ordering[GenericRecord] = Ordering.by(bucketKey)

  def bucketer(record: GenericRecord): Int =
    MurmurHash3.stringHash(bucketKey(record))

  def smbJoin(
               leftIt: BufferedIterator[GenericRecord],
               rightIt: BufferedIterator[GenericRecord]
             ): Iterable[(String, Iterable[GenericRecord], Iterable[GenericRecord])] = {

    def consumeGroup(
                      bIt: BufferedIterator[GenericRecord]
                    ): (String, Iterable[GenericRecord]) = {
      val buffer = mutable.ListBuffer.newBuilder[GenericRecord]
      val groupKey = SMBUtils.bucketKey(bIt.head)
      while (bIt.hasNext && SMBUtils.bucketKey(bIt.head) == groupKey) {
        buffer += bIt.next
      }
      (groupKey, buffer.result)
    }

    val buffer = mutable.ListBuffer
      .newBuilder[(String, Iterable[GenericRecord], Iterable[GenericRecord])]

    while (leftIt.hasNext || rightIt.hasNext) {
      (leftIt.hasNext, rightIt.hasNext) match {
        case (true, false) =>
          val (leftKey, leftGroup) = consumeGroup(leftIt)
          buffer += ((leftKey, leftGroup, Iterable.empty))
        case (false, true) =>
          val (rightKey, rightGroup) = consumeGroup(rightIt)
          buffer += ((rightKey, Iterable.empty, rightGroup))
        case (true, true) =>
          (SMBUtils.bucketKey(leftIt.head) compare SMBUtils.bucketKey(
            rightIt.head
          )).signum match {
            case -1 =>
              val (leftKey, leftGroup) = consumeGroup(leftIt)
              buffer += ((leftKey, leftGroup, Iterable.empty))
            case 0 =>
              val (leftKey, leftGroup) = consumeGroup(leftIt)
              val (_, rightGroup) = consumeGroup(rightIt)
              buffer += ((leftKey, leftGroup, rightGroup))
            case 1 =>
              val (rightKey, rightGroup) = consumeGroup(rightIt)
              buffer += ((rightKey, Iterable.empty, rightGroup))
          }
      }
    }
    buffer.result
  }

  def bucketKey(record: GenericRecord): String = record.get("id").toString

}
