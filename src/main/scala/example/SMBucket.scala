package example


import com.spotify.scio.coders.Coder


case class SMBucket[T : Coder](bucketId: Int, numBuckets: Int, values: Iterable[T])
