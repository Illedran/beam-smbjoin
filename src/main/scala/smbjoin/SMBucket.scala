package smbjoin

case class SMBucket[T](bucketId: Int, values: Iterable[T])
