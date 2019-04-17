package smbjoin

import org.apache.avro.generic.GenericRecord

import scala.collection.mutable

object SMBUtils {
  val ordering: Ordering[GenericRecord] = Ordering.by(joinKey)

  def smbJoin(
    leftIt: BufferedIterator[GenericRecord],
    rightIt: BufferedIterator[GenericRecord]
  ): Iterable[(String, Iterable[GenericRecord], Iterable[GenericRecord])] = {

    def consumeGroup(bIt: BufferedIterator[GenericRecord]) = {
      val buffer = mutable.ListBuffer.newBuilder[GenericRecord]
      val groupKey = SMBUtils.joinKey(bIt.head)
      while (bIt.hasNext && SMBUtils.joinKey(bIt.head) == groupKey) {
        buffer += bIt.next
      }
      (groupKey, buffer.result)
    }

    val buffer = mutable.ListBuffer
      .newBuilder[(String, Iterable[GenericRecord], Iterable[GenericRecord])]

    while (leftIt.hasNext || rightIt.hasNext) {
      (leftIt.hasNext, rightIt.hasNext) match {
        case (true, false) => // Left outer join
          val (leftKey, leftGroup) = consumeGroup(leftIt)
          buffer += ((leftKey, leftGroup, Iterable.empty))
        case (false, true) => // Right outer join
          val (rightKey, rightGroup) = consumeGroup(rightIt)
          buffer += ((rightKey, Iterable.empty, rightGroup))
        case (true, true) =>
          (SMBUtils.joinKey(leftIt.head) compare SMBUtils.joinKey(rightIt.head)).signum match {
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

  def joinKey(record: GenericRecord): String = record.get("id").toString

}
