package example

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

object SMBSCollectionFunctions {

  import scala.language.implicitConversions

  implicit def makeSMBSCollectionFunctions[T: Ordering : Coder](self: SCollection[T]):
  SMBSCollectionFunctions[T] = new SMBSCollectionFunctions(self)
}

class SMBSCollectionFunctions[T: Ordering : Coder](self: SCollection[T]) {


  def sortMergeBuckets(numBuckets: Int): SCollection[SMBucket[T]] = {
    //    val hashed =
    self.map(x => {
      (Math.floorMod(x.hashCode, numBuckets), x)
    })
      .groupByKey

      .map { case (key, value) => SMBucket(key, numBuckets, value.toVector.sorted) }
  }
}